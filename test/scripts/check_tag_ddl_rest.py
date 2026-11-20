"""Verify that attached REST tables use namespace tag operations and credentials."""

import ctypes
import json
import shutil
import subprocess
import sys
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
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


def concurrent_table_drop(attach, creating, dropped, release):
    # Two native connections must share the catalog and its schema lock.
    suffix = "dylib" if sys.platform == "darwin" else "so"
    lib = ctypes.CDLL(str(ROOT / f"build/release/src/libduckdb.{suffix}"))
    db, writer, dropper = (ctypes.c_void_p() for _ in range(3))

    def execute(connection, statement):
        assert lib.duckdb_query(connection, statement.encode(), None) == 0, statement

    assert lib.duckdb_open(None, ctypes.byref(db)) == 0
    try:
        assert lib.duckdb_connect(db, ctypes.byref(writer)) == 0
        assert lib.duckdb_connect(db, ctypes.byref(dropper)) == 0
        execute(writer, "LOAD lance; " + attach)
        table = 'ns.main."samples.v2"'
        with ThreadPoolExecutor(2) as pool:
            create = pool.submit(execute, writer, f"ALTER TABLE {table} CREATE TAG 'pending' AT VERSION 1")
            try:
                assert creating.wait(10), "CREATE never reached the namespace"
                drop = pool.submit(execute, dropper, f"DROP TABLE {table}")
                # An unlocked DROP can reach the server before CREATE has completed.
                dropped.wait(1)
            finally:
                release.set()
            create.result()
            drop.result()
    finally:
        release.set()
        lib.duckdb_disconnect(ctypes.byref(dropper))
        lib.duckdb_disconnect(ctypes.byref(writer))
        lib.duckdb_close(ctypes.byref(db))


def main():
    with tempfile.TemporaryDirectory() as tmp:
        dataset = Path(tmp) / "accessible.lance"
        shutil.copytree(ROOT / "test/data/branch_tag_fixture.lance", dataset)
        local = f"ATTACH '{tmp}' AS ns (TYPE LANCE); "
        # Lance 14.0.3 encodes this filename when another client creates the tag.
        tags = dataset / "_refs" / "tags"
        shutil.copyfile(tags / "main_tag.json", tags / "%C3%A9.json")
        sql(local + "ALTER TABLE ns.main.accessible DROP TAG 'é'")
        sql(local + "ALTER TABLE ns.main.accessible DROP TAG 'dev_tag'")
        assert sql(f"SELECT tag FROM lance_tags('{dataset}')") == "main_tag"
        assert sql(f"SELECT max(version) FROM lance_versions('{dataset}')") == "3"
        writes = []
        order = []
        creating, dropped, release = (threading.Event() for _ in range(3))
        table_pages = [["samples.v2"]]
        tag_mode = "normal"

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def route(self):
                uri = urlparse(self.path)
                path = unquote(uri.path)
                payload = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                query = parse_qs(uri.query)
                token = query.get("page_token", [None])[0]
                status = 200
                if (
                    self.headers.get("Authorization") != "Bearer test-only"
                    or self.headers.get("x-tag-fixture") != "release"
                ):
                    status, response = 403, {"message": "fixture credentials required"}
                elif path == "/v1/namespace/Team/table/list":
                    if table_pages == "denied":
                        status, response = 403, {"message": "table listing denied"}
                    elif table_pages == "repeated":
                        response = {"tables": [], "page_token": "next"}
                    else:
                        response = {"tables": table_pages[1 if token == "next" else 0]}
                        if len(table_pages) == 2 and token is None:
                            response["page_token"] = "next"
                elif "/namespace/" in path and path.endswith("/list"):
                    response = {"namespaces": []}
                elif "/table/" in path and path.endswith("/describe"):
                    fields = [{"name": "id", "nullable": True, "type": {"type": "int64"}}]
                    response = {"schema": {"fields": fields}, "location": str(dataset)}
                elif path == "/v1/table/Team|samples.v2/tags/list":
                    assert self.command == "POST" and query["delimiter"] == ["|"]
                    if tag_mode == "denied" and token == "next":
                        status, response = 403, {"message": "tag listing denied"}
                    elif tag_mode == "repeated" or token is None:
                        response = {"tags": {}, "page_token": "next"}
                    else:
                        assert token == "next"
                        response = {
                            "tags": {name: {"version": 1, "manifestSize": 64} for name in ("Release", "main_tag")}
                        }
                elif "/tags/" in path:
                    body = json.loads(payload)
                    assert self.command == "POST" and query["delimiter"] == ["|"]
                    assert path.startswith("/v1/table/Team|samples.v2/tags/") and body["id"] == ["Team", "samples.v2"]
                    writes.append((path.split("/")[-1], body["tag"], body.get("version")))
                    if body["tag"] == "pending":
                        creating.set()
                        assert release.wait(10)
                        order.append("create")
                    if body["tag"] in ("denied", "main_tag"):
                        status, response = 403, {"message": "tag mutation denied"}
                    else:
                        response = {}
                elif path == "/v1/table/Team|samples.v2/drop":
                    dropped.set()
                    order.append("drop")
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
                table_pages = [["other"], ["samples.v2"]]
                sql(alter + "CREATE TAG 'Release' AT VERSION 1")
                sql(alter + "DROP TAG 'RELEASE'", expected_error="tag RELEASE does not exist")
                sql(alter + "DROP TAG 'Release'")
                sql(alter + "CREATE TAG 'denied' AT VERSION 1", expected_error="denied")
                sql(alter + "DROP TAG 'main_tag'", expected_error="denied")
                for tag_mode, error in [("denied", "tag listing denied"), ("repeated", "repeated page token")]:
                    sql(alter + "DROP TAG 'main_tag'", expected_error=error)
                tag_mode = "normal"

                warm = attach + 'DESCRIBE ns.main."samples.v2"; ALTER TABLE ns.main."SAMPLES.V2" '
                sql(warm + "CREATE TAG 'unique' AT VERSION 1")
                for table_pages, error in [
                    ([["samples.v2"], ["SAMPLES.V2"]], "Ambiguous Lance namespace table names"),
                    ([["SAMPLES.V2"]], "does not match the bound table"),
                    ([[]], "does not match the bound table"),
                    ("repeated", "repeated page token"),
                    ("denied", "table listing denied"),
                ]:
                    for operation in ("CREATE TAG 'blocked' AT VERSION 1", "DROP TAG 'main_tag'"):
                        sql(warm + operation, expected_error=error)
                table_pages = [["samples.v2"]]
                concurrent_table_drop(attach, creating, dropped, release)
                assert order == ["create", "drop"], order
                assert sql(f"SELECT tag FROM lance_tags('{dataset}')") == "main_tag"
            finally:
                server.shutdown()
                thread.join()

        expected = [
            ("create", "Release", 1),
            ("delete", "Release", None),
            ("create", "denied", 1),
            ("delete", "main_tag", None),
            ("create", "unique", 1),
            ("create", "pending", 1),
        ]
        assert writes == expected, writes


if __name__ == "__main__":
    main()
