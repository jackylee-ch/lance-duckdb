"""Verify that attached REST tables use namespace tag operations and credentials."""

import json
import shutil
import subprocess
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse


ROOT = Path(__file__).resolve().parents[2]
DUCKDB = ROOT / "build/release/duckdb"
EXTENSION = ROOT / "build/release/extension/lance/lance.duckdb_extension"


def sql(statement, expected_error=None):
    result = subprocess.run(
        [str(DUCKDB), "-unsigned", "-no-agent", "-noheader", "-list", "-c", f"LOAD '{EXTENSION}'; {statement}"],
        text=True,
        capture_output=True,
        timeout=60,
    )
    if expected_error:
        assert result.returncode and expected_error in result.stderr, result.stderr
    elif result.returncode:
        raise RuntimeError(result.stderr)
    return result.stdout.strip()


def main():
    with tempfile.TemporaryDirectory() as tmp:
        dataset = Path(tmp) / "accessible.lance"
        shutil.copytree(ROOT / "test/data/branch_tag_fixture.lance", dataset)
        local = f"ATTACH '{tmp}' AS ns (TYPE LANCE); "
        sql(local + "ALTER TABLE ns.main.accessible DROP TAG 'dev_tag'")
        assert sql(f"SELECT tag FROM lance_tags('{dataset}')") == "main_tag"
        assert sql(f"SELECT max(version) FROM lance_versions('{dataset}')") == "3"
        calls = []

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def route(self):
                uri = urlparse(self.path)
                path = unquote(uri.path)
                payload = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                status = 200
                if path.endswith("/table/list"):
                    response = {"tables": ["samples.v2"]}
                elif "/namespace/" in path and path.endswith("/list"):
                    response = {"namespaces": []}
                elif "/table/" in path and path.endswith("/describe"):
                    fields = [{"name": "id", "nullable": True, "type": {"type": "int64"}}]
                    response = {"schema": {"fields": fields}, "location": str(dataset)}
                elif "/tags/" in path:
                    body = json.loads(payload)
                    calls.append(
                        (
                            self.command,
                            path,
                            parse_qs(uri.query),
                            body,
                            self.headers.get("Authorization"),
                            self.headers.get("x-tag-fixture"),
                        )
                    )
                    if body["tag"] in ("denied", "main_tag"):
                        status, response = 403, {"message": "tag mutation denied"}
                    else:
                        response = {}
                else:
                    status, response = 404, {"message": "unknown route"}
                data = json.dumps(response).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            do_POST = route
            do_GET = route

        with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                attach = (
                    f"ATTACH 'Team' AS ns (TYPE LANCE, ENDPOINT 'http://127.0.0.1:{server.server_port}', "
                    "DELIMITER '|', BEARER_TOKEN 'test-only', HEADER 'x-tag-fixture=release'); "
                )
                alter = attach + 'ALTER TABLE ns.main."samples.v2" '
                sql(alter + "CREATE TAG 'Release' AT VERSION 1")
                sql(alter + "DROP TAG 'Release'")
                sql(alter + "CREATE TAG 'denied' AT VERSION 1", expected_error="denied")
                sql(alter + "DROP TAG 'main_tag'", expected_error="denied")
                assert sql(f"SELECT tag FROM lance_tags('{dataset}')") == "main_tag"
            finally:
                server.shutdown()
                thread.join()

        expected = [
            ("create", "Release", 1),
            ("delete", "Release", None),
            ("create", "denied", 1),
            ("delete", "main_tag", None),
        ]
        assert len(calls) == len(expected), calls
        for (method, path, query, body, bearer, header), (action, tag, version) in zip(calls, expected):
            assert (method, path) == ("POST", f"/v1/table/Team|samples.v2/tags/{action}"), calls
            assert query.get("delimiter") == ["|"] and body["id"] == ["Team", "samples.v2"], calls
            assert (body["tag"], body.get("version")) == (tag, version), calls
            assert (bearer, header) == ("Bearer test-only", "release"), calls


if __name__ == "__main__":
    main()
