import os
import shutil
import subprocess

from backends.base import Backend, load, unique_id

DSN_VAR = "VC_POSTGRES_DSN"


class PostgresBackend(Backend):
    """One server shared by every test; each attach gets its own schema."""

    name = "postgres"

    scanner_calls = [
        "SELECT * FROM postgres_query('{catalog}', 'SELECT password FROM {schema}.secret')",
        "SELECT id, (SELECT password FROM postgres_query('{catalog}', 'SELECT password FROM {schema}.secret')) FROM {users}",
        "SELECT id, (SELECT password FROM query_table('{catalog}.{schema}.secret')) FROM {users}",
    ]

    upstream_reads = [
        "SELECT * FROM app.{schema}.secret_view",
        "SELECT * FROM app.pg_catalog.pg_proc",
        "SELECT * FROM app.information_schema.tables",
    ]

    def unavailable(self):
        if not os.environ.get(DSN_VAR):
            return f"{DSN_VAR} not set in the environment or .env"
        if not shutil.which("psql"):
            return "psql not on PATH"
        return None

    def psql(self, schema, *args, stdin=None):
        subprocess.run(
            ["psql", os.environ[DSN_VAR], "-q", "-v", "ON_ERROR_STOP=1", *args],
            input=stdin,
            text=True,
            check=True,
            capture_output=True,
            env={**os.environ, "PGOPTIONS": f"-c search_path={schema}"},
        )

    def attach(self, con, location):
        schema = unique_id("s")
        self.psql("public", "-c", f"CREATE SCHEMA {schema}")
        self.psql(schema, stdin=self.seed)
        load(con, "postgres")
        con.execute(f"ATTACH '{os.environ[DSN_VAR]}' AS pg (TYPE postgres)")
        return "pg", schema

    def close(self, source):
        try:
            source.con.close()
        finally:
            self.psql("public", "-c", f"DROP SCHEMA {source.schema} CASCADE")
