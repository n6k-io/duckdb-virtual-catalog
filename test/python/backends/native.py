from backends.base import Backend


class NativeBackend(Backend):
    name = "native"

    scanner_calls = [
        "SELECT * FROM query_table('{catalog}.{schema}.secret')",
        "SELECT * FROM query('SELECT password FROM {catalog}.{schema}.secret')",
        "SELECT id, (SELECT password FROM query_table('{catalog}.{schema}.secret')) FROM {users}",
        "SELECT id, (SELECT password FROM query('SELECT password FROM {catalog}.{schema}.secret')) FROM {users}",
    ]

    upstream_reads = [
        "SELECT * FROM app.{schema}.secret_view",
    ]

    def attach(self, con, location):
        con.execute(self.seed)
        return "memory", "main"
