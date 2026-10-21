import sqlite3

from backends.base import Backend, load


class SQLiteBackend(Backend):
    name = "sqlite"

    scanner_calls = [
        "SELECT * FROM sqlite_query('{catalog}', 'SELECT password FROM secret')",
        "SELECT id, (SELECT password FROM sqlite_query('{catalog}', 'SELECT password FROM secret')) FROM {users}",
        "SELECT id, (SELECT password FROM query_table('{catalog}.{schema}.secret')) FROM {users}",
    ]

    upstream_reads = [
        "SELECT * FROM app.{schema}.secret_view",
    ]

    def attach(self, con, location):
        path = location / "upstream.sqlite"
        upstream = sqlite3.connect(path)
        try:
            upstream.executescript(self.seed)
        finally:
            upstream.close()
        load(con, "sqlite")
        con.execute(f"ATTACH '{path}' AS lite (TYPE sqlite)")
        return "lite", "main"
