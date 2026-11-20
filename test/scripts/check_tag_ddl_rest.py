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

from check_dml_returning_lifecycle import Result

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


def concurrent_table_ddl(attach, tmp, creating, dropped, release, release_drop, deny_drop):
    # Two native connections must share the catalog and its schema lock.
    suffix = "dylib" if sys.platform == "darwin" else "so"
    lib = ctypes.CDLL(str(ROOT / f"build/release/src/libduckdb.{suffix}"))
    db, writer, dropper = (ctypes.c_void_p() for _ in range(3))

    lib.duckdb_result_error.restype = ctypes.c_char_p

    def execute(connection, statement, expected_error=None):
        result = Result()
        try:
            status = lib.duckdb_query(connection, statement.encode(), ctypes.byref(result))
            message = (lib.duckdb_result_error(ctypes.byref(result)) or b"").decode()
            if expected_error:
                assert status and expected_error in message, (statement, message)
            else:
                assert not status, (statement, message)
        finally:
            lib.duckdb_destroy_result(ctypes.byref(result))

    assert lib.duckdb_open(None, ctypes.byref(db)) == 0
    try:
        assert lib.duckdb_connect(db, ctypes.byref(writer)) == 0
        assert lib.duckdb_connect(db, ctypes.byref(dropper)) == 0
        execute(writer, "LOAD lance; " + attach)
        table = 'ns.main."samples.v2"'
        # This service accepts aliases but omits the optional name in describe.
        execute(writer, 'DESCRIBE ns.main."SAMPLES.V2"')
        execute(writer, f"ALTER TABLE {table} CREATE TAG 'blocked' AT VERSION 1", "retry with the listed spelling")
        execute(writer, f"ALTER TABLE {table} CREATE TAG 'rebound' AT VERSION 1")
        with ThreadPoolExecutor(2) as pool:
            for interrupt in (False, True):
                creating.clear()
                dropped.clear()
                release.clear()
                create = pool.submit(
                    execute,
                    writer,
                    f"ALTER TABLE {table} CREATE TAG 'pending' AT VERSION 1",
                    "Interrupt" if interrupt else None,
                )
                try:
                    assert creating.wait(10), "CREATE never reached the namespace"
                    if interrupt:
                        lib.duckdb_interrupt(writer)
                    drop = pool.submit(execute, dropper, f"DROP TABLE {table}")
                    assert not dropped.wait(1), "DROP reached the namespace before the tag write completed"
                finally:
                    release.set()
                create.result()
                drop.result()

            for definition in ("(id BIGINT)", "AS SELECT 1::BIGINT AS id"):
                ddl = f"CREATE OR REPLACE TABLE {table} {definition}"
                dropped.clear()
                release_drop.clear()
                replace = pool.submit(execute, dropper, ddl)
                try:
                    assert dropped.wait(10), "replacement never reached the namespace"
                    execute(writer, f"ALTER TABLE {table} CREATE TAG 'racing' AT VERSION 1", "changed concurrently")
                finally:
                    release_drop.set()
                replace.result()
                deny_drop.set()
                try:
                    execute(dropper, ddl, "replacement denied")
                finally:
                    deny_drop.clear()
                execute(writer, f"ALTER TABLE {table} CREATE TAG 'after' AT VERSION 1")

            creating.clear()
            dropped.clear()
            release_drop.clear()
            child = 'ns.Child."samples.v2"'
            execute(writer, f"DESCRIBE {child}")
            drop = pool.submit(execute, dropper, "DROP SCHEMA ns.Child CASCADE")
            try:
                assert dropped.wait(10), "CASCADE never reached the namespace"
                create = pool.submit(
                    execute, writer, f"ALTER TABLE {child} CREATE TAG 'stale' AT VERSION 1", "does not exist"
                )
                assert not creating.wait(1), "a tag request reached the namespace during CASCADE"
            finally:
                release_drop.set()
            drop.result()
            create.result()

        execute(writer, f"ATTACH '{tmp}' AS local_ns (TYPE LANCE)")
        bound = "local_ns.main.bound"
        execute(writer, f"CREATE TABLE {bound} AS SELECT 11::BIGINT AS id")
        execute(writer, "SET threads = 1")
        statement, pending = ctypes.c_void_p(), ctypes.c_void_p()
        result = Result()
        try:
            tag_sql = f"ALTER TABLE {bound} CREATE TAG 'stale' AT VERSION 1".encode()
            assert lib.duckdb_prepare(writer, tag_sql, ctypes.byref(statement)) == 0
            assert lib.duckdb_pending_prepared(statement, ctypes.byref(pending)) == 0
            execute(dropper, f"DROP TABLE {bound}; CREATE TABLE {bound} AS SELECT 99::BIGINT AS id")
            status = lib.duckdb_execute_pending(pending, ctypes.byref(result))
            message = lib.duckdb_result_error(ctypes.byref(result))
            assert status and b"changed concurrently" in (message or b""), message
        finally:
            lib.duckdb_destroy_result(ctypes.byref(result))
            lib.duckdb_destroy_pending(ctypes.byref(pending))
            lib.duckdb_destroy_prepare(ctypes.byref(statement))
        path = Path(tmp) / "bound.lance"
        assert sql(f"SELECT count(*) FROM lance_tags('{path}')") == "0"
        execute(writer, f"ALTER TABLE {bound} CREATE TAG 'fresh' AT VERSION 1")
        assert sql(f"SELECT tag FROM lance_tags('{path}')") == "fresh"
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
        release_drop = threading.Event()
        release_drop.set()
        deny_drop = threading.Event()
        location = dataset
        table_pages = [["samples.v2"]]
        tag_mode = "normal"
        describe_table_name = True

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def route(self):
                nonlocal location
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
                elif path == "/v1/namespace/Team/list":
                    response = {"namespaces": ["Child"]}
                elif path == "/v1/namespace/Team|Child/table/list":
                    response = {"tables": ["samples.v2"]}
                elif "/namespace/" in path and path.endswith("/list"):
                    response = {"namespaces": []}
                elif "/table/" in path and path.endswith("/describe"):
                    fields = [{"name": "id", "nullable": True, "type": {"type": "int64"}}]
                    response = {"schema": {"fields": fields}, "location": str(location)}
                    if describe_table_name:
                        response["table"] = "samples.v2"
                elif path == "/v1/table/Team|samples.v2/tags/list":
                    assert self.command == "POST" and query["delimiter"] == ["|"]
                    if tag_mode == "denied" and token == "next":
                        status, response = 403, {"message": "tag listing denied"}
                    elif tag_mode == "unreadable":
                        status, response = 400, {"message": "cannot read encoded tag filename"}
                    elif tag_mode == "repeated" or token is None:
                        response = {"tags": {}, "page_token": "next"}
                    else:
                        assert token == "next"
                        response = {
                            "tags": {name: {"version": 1, "manifestSize": 64} for name in ("Release", "main_tag")}
                        }
                elif path == "/v1/table/Team|Child|samples.v2/tags/create":
                    assert json.loads(payload)["id"] == ["Team", "Child", "samples.v2"]
                    creating.set()
                    order.append("child tag")
                    response = {}
                elif path == "/v1/namespace/Team|Child/drop":
                    dropped.set()
                    assert release_drop.wait(10)
                    order.append("cascade")
                    response = {}
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
                    assert release_drop.wait(10)
                    if deny_drop.is_set():
                        status, response = 403, {"message": "replacement denied"}
                    else:
                        order.append("drop")
                        response = {}
                elif path == "/v1/table/Team|samples.v2/declare":
                    location = Path(tmp) / f"replacement-{len(order)}.lance"
                    response = {"location": str(location)}
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
                for tag_mode, error in [
                    ("denied", "tag listing denied"),
                    ("repeated", "repeated page token"),
                    ("unreadable", "cannot verify exact tag name with list_table_tags"),
                ]:
                    sql(alter + "DROP TAG 'main_tag'", expected_error=error)
                tag_mode = "normal"

                warm = attach + 'DESCRIBE ns.main."samples.v2"; ALTER TABLE ns.main."SAMPLES.V2" '
                sql(warm + "CREATE TAG 'unique' AT VERSION 1")
                alias = attach + 'DESCRIBE ns.main."SAMPLES.V2"; ALTER TABLE ns.main."SAMPLES.V2" '
                sql(alias + "CREATE TAG 'alias' AT VERSION 1")
                sql(alias + "DROP TAG 'Release'")
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
                describe_table_name = False
                concurrent_table_ddl(attach, tmp, creating, dropped, release, release_drop, deny_drop)
                assert order == ["create", "drop", "create", "drop", "drop", "drop", "cascade"], order
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
            ("create", "alias", 1),
            ("delete", "Release", None),
            ("create", "rebound", 1),
            ("create", "pending", 1),
            ("create", "pending", 1),
            ("create", "after", 1),
            ("create", "after", 1),
        ]
        assert writes == expected, writes

        file_store = Path(tmp).as_uri().replace("file:", "file-object-store:", 1)
        file_alter = f"ATTACH '{file_store}' AS ns (TYPE LANCE); ALTER TABLE ns.main.accessible "
        sql(file_alter + "DROP TAG 'MAIN_TAG'", "does not exist")
        sql(file_alter + "CREATE TAG 'Exact' AT VERSION 1; ALTER TABLE ns.main.accessible DROP TAG 'Exact'")
        assert sql(f"SELECT tag FROM lance_tags('{dataset}')") == "main_tag"


if __name__ == "__main__":
    main()
