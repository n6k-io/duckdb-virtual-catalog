"""Differential parity harness: the same SQL against a native table, a bridged table and a
provider table, with a native DuckDB table as the oracle.

A probe's outcome on a backend is one of:

  parity       both succeeded, same rows and same resulting table state
  unsupported  native succeeded, the backend raised
  differs      both succeeded, but the rows or the resulting state disagree

LEDGER records every non-parity outcome, with the reason. The suite fails in BOTH directions: a
probe that regresses, and a probe listed here that starts working. The second is the point -- it is
how a gap closing gets noticed instead of quietly drifting out of the docs.
"""

from dataclasses import dataclass, field

import pyarrow as pa

from conftest import READWRITE, bridge, new_connection
from provider_stub import FakeProvider

COLUMNS = "id INTEGER PRIMARY KEY, name VARCHAR, n INTEGER"
ROWS = [(1, "a", 10), (2, "b", 20), (3, "c", 30)]


@dataclass
class Probe:
    name: str
    category: str
    sql: str
    #: Read back after the statement to compare the resulting table state. None for probes that
    #: leave no table to read (DROP, RENAME).
    verify: str | None = "SELECT * FROM {t} ORDER BY id"
    #: Index into a ';'-split `sql` after which to snapshot the backing store through a second
    #: connection (backend.observe()). Only a mid-transaction snapshot separates "the write landed
    #: at COMMIT" from "the write landed at the statement and COMMIT was a no-op" -- the end state
    #: is identical either way.
    observe_at: int | None = None


@dataclass
class Outcome:
    status: str  # "ok" | "error"
    rows: object = None
    state: object = None
    observed: object = None
    error: str = ""

    def comparable(self):
        return (self.rows, self.state, self.observed)


class Backend:
    """A connection holding a table called `t` with COLUMNS/ROWS, reachable at `table`."""

    name = ""

    def execute(self, sql):
        return self.con.execute(sql).fetchall()

    def observe(self):
        """The backing store's rows, read outside the transaction driving `self.con`."""
        raise NotImplementedError

    def close(self):
        for con in getattr(self, "_owned", []):
            con.close()


class NativeBackend(Backend):
    name = "native"

    def __init__(self):
        self.con = new_connection()
        self._owned = [self.con]
        self.con.execute(f"CREATE TABLE t({COLUMNS})")
        self.con.executemany("INSERT INTO t VALUES (?, ?, ?)", ROWS)
        self.observer = self.con.cursor()
        self.table = "memory.main.t"

    def observe(self):
        return self.observer.execute("SELECT * FROM t ORDER BY id").fetchall()


class BridgeBackend(Backend):
    name = "bridge"

    def __init__(self):
        self.source = new_connection()
        self.con = new_connection()
        self._owned = [self.source, self.con]
        self.source.execute(f"CREATE TABLE t({COLUMNS})")
        self.source.executemany("INSERT INTO t VALUES (?, ?, ?)", ROWS)
        bridge(self.source, self.con, {"t": READWRITE})
        self.observer = self.source.cursor()
        self.table = "app.main.t"

    def observe(self):
        return self.observer.execute("SELECT * FROM t ORDER BY id").fetchall()


class ProviderBackend(Backend):
    name = "provider"

    def __init__(self):
        self.con = new_connection(bridge=False, provider=True)
        self._owned = [self.con]
        self.con.execute("ATTACH ':memory:' AS app (TYPE virtual_catalog_provider)")
        self.provider = FakeProvider(self.con)
        self.provider.add_table(
            "t",
            pa.table(
                {
                    "id": pa.array([r[0] for r in ROWS], type=pa.int32()),
                    "name": pa.array([r[1] for r in ROWS]),
                    "n": pa.array([r[2] for r in ROWS], type=pa.int32()),
                }
            ),
            primary_key=["id"],
        )
        self.provider.register()
        self.table = "app.main.t"

    def observe(self):
        # The backing store is the stub's own Arrow table, read out-of-band -- there is no second
        # connection to take, and no transaction it could be enrolled in.
        return sorted(tuple(row.values()) for row in self.provider.rows("t"))


BACKENDS = {b.name: b for b in (BridgeBackend, ProviderBackend)}


def run_probe(backend, probe):
    sql = probe.sql.format(t=backend.table)
    observed = None
    try:
        if probe.observe_at is None:
            rows = backend.execute(sql)
        else:
            statements = [s.strip() for s in sql.split(";") if s.strip()]
            for i, statement in enumerate(statements):
                rows = backend.execute(statement)
                if i == probe.observe_at:
                    observed = backend.observe()
    except Exception as exc:  # noqa: BLE001 -- the failure itself is the datum
        return Outcome("error", error=f"{type(exc).__name__}: {str(exc).splitlines()[0][:160]}")
    state = None
    if probe.verify:
        try:
            state = backend.execute(probe.verify.format(t=backend.table))
        except Exception as exc:  # noqa: BLE001
            state = f"verify-error: {str(exc).splitlines()[0][:120]}"
    return Outcome("ok", rows=rows, state=state, observed=observed)


def classify(native: Outcome, other: Outcome) -> str:
    if native.status == "error":
        # Probes that a native table rejects too (a duplicate key, a NULL in a NOT NULL column).
        # Matching the rejection is parity; accepting what native refuses is the worse divergence,
        # because the caller gets no signal at all.
        return "parity" if other.status == "error" else "accepts-invalid"
    if other.status == "error":
        return "unsupported"
    return "parity" if native.comparable() == other.comparable() else "differs"


# --- probes -------------------------------------------------------------------------------------

PROBES = [
    # read -------------------------------------------------------------------------------------
    Probe("select_star", "read", "SELECT * FROM {t} ORDER BY id"),
    Probe("select_projection", "read", "SELECT n, id FROM {t} ORDER BY id"),
    Probe("where_equality", "read", "SELECT * FROM {t} WHERE id = 2"),
    Probe("where_in", "read", "SELECT * FROM {t} WHERE id IN (1, 3) ORDER BY id"),
    Probe("where_is_null", "read", "SELECT * FROM {t} WHERE name IS NULL"),
    Probe("order_limit_offset", "read", "SELECT * FROM {t} ORDER BY n DESC LIMIT 2 OFFSET 1"),
    Probe("count_star", "read", "SELECT count(*) FROM {t}"),
    Probe("aggregate", "read", "SELECT sum(n), avg(n), min(id), max(id) FROM {t}"),
    Probe(
        "group_by_having", "read", "SELECT n > 15 AS big, count(*) FROM {t} GROUP BY 1 HAVING count(*) > 0 ORDER BY 1"
    ),
    Probe("distinct", "read", "SELECT DISTINCT n > 15 AS big FROM {t} ORDER BY 1"),
    Probe("self_join", "read", "SELECT a.id, b.id FROM {t} a JOIN {t} b ON a.id = b.id - 1 ORDER BY 1"),
    Probe("cte", "read", "WITH x AS (SELECT * FROM {t} WHERE n > 10) SELECT count(*) FROM x"),
    Probe("window", "read", "SELECT id, sum(n) OVER (ORDER BY id) FROM {t} ORDER BY id"),
    Probe("qualify", "read", "SELECT id FROM {t} QUALIFY row_number() OVER (ORDER BY id) = 1"),
    Probe("union", "read", "SELECT id FROM {t} UNION SELECT id FROM {t} ORDER BY 1"),
    Probe("scalar_subquery", "read", "SELECT (SELECT max(n) FROM {t})"),
    Probe("exists_subquery", "read", "SELECT count(*) FROM {t} a WHERE EXISTS (SELECT 1 FROM {t} b WHERE b.id = a.id)"),
    Probe("filter_on_unprojected", "read", "SELECT id FROM {t} WHERE name = 'b'"),
    # write ------------------------------------------------------------------------------------
    Probe("insert_values", "write", "INSERT INTO {t} VALUES (4, 'd', 40)"),
    Probe("insert_multi_row", "write", "INSERT INTO {t} VALUES (4, 'd', 40), (5, 'e', 50)"),
    Probe("insert_select", "write", "INSERT INTO {t} SELECT id + 10, name, n FROM {t}"),
    Probe("insert_partial_columns", "write", "INSERT INTO {t} (id, name) VALUES (4, 'd')"),
    Probe("insert_returning", "write", "INSERT INTO {t} VALUES (4, 'd', 40) RETURNING id"),
    Probe("insert_on_conflict_do_nothing", "write", "INSERT INTO {t} VALUES (1, 'dup', 0) ON CONFLICT DO NOTHING"),
    Probe(
        "insert_on_conflict_do_update",
        "write",
        "INSERT INTO {t} VALUES (1, 'dup', 0) ON CONFLICT (id) DO UPDATE SET n = excluded.n",
    ),
    Probe("insert_or_replace", "write", "INSERT OR REPLACE INTO {t} VALUES (1, 'replaced', 99)"),
    Probe("insert_or_ignore", "write", "INSERT OR IGNORE INTO {t} VALUES (1, 'dup', 0)"),
    Probe("insert_duplicate_key_raises", "write", "INSERT INTO {t} VALUES (1, 'dup', 0)"),
    Probe("update_simple", "write", "UPDATE {t} SET n = n + 1 WHERE id = 2"),
    Probe("update_all_rows", "write", "UPDATE {t} SET n = 0"),
    Probe("update_key_column", "write", "UPDATE {t} SET id = id + 100 WHERE id = 3"),
    Probe("update_from", "write", "UPDATE {t} SET n = s.v FROM (SELECT 2 AS k, 99 AS v) s WHERE {t}.id = s.k"),
    Probe("update_returning", "write", "UPDATE {t} SET n = 0 WHERE id = 1 RETURNING id"),
    Probe("delete_simple", "write", "DELETE FROM {t} WHERE id = 2"),
    Probe("delete_all", "write", "DELETE FROM {t}"),
    Probe("delete_using", "write", "DELETE FROM {t} USING (SELECT 2 AS k) s WHERE {t}.id = s.k"),
    Probe("delete_returning", "write", "DELETE FROM {t} WHERE id = 1 RETURNING id"),
    Probe("truncate", "write", "TRUNCATE {t}"),
    # ddl --------------------------------------------------------------------------------------
    Probe("add_column", "ddl", "ALTER TABLE {t} ADD COLUMN extra INTEGER"),
    Probe("drop_column", "ddl", "ALTER TABLE {t} DROP COLUMN n"),
    Probe("rename_column", "ddl", "ALTER TABLE {t} RENAME COLUMN n TO m"),
    Probe("alter_column_type", "ddl", "ALTER TABLE {t} ALTER COLUMN n SET DATA TYPE BIGINT"),
    Probe("set_default", "ddl", "ALTER TABLE {t} ALTER COLUMN n SET DEFAULT 7"),
    Probe("add_not_null", "ddl", "ALTER TABLE {t} ALTER COLUMN name SET NOT NULL"),
    Probe("comment_on_table", "ddl", "COMMENT ON TABLE {t} IS 'hello'"),
    Probe("drop_table", "ddl", "DROP TABLE {t}", verify=None),
    Probe("create_view_over", "ddl", "CREATE VIEW v_over AS SELECT * FROM {t}"),
    Probe("ctas_from", "ddl", "CREATE TABLE copy_of AS SELECT * FROM {t}"),
    # metadata ---------------------------------------------------------------------------------
    Probe("describe", "metadata", "SELECT column_name, column_type FROM (DESCRIBE {t})"),
    Probe("duckdb_tables", "metadata", "SELECT count(*) FROM duckdb_tables() WHERE table_name = 't'"),
    Probe(
        "duckdb_columns",
        "metadata",
        "SELECT column_name, data_type FROM duckdb_columns() WHERE table_name = 't' ORDER BY column_index",
    ),
    Probe(
        "duckdb_constraints",
        "metadata",
        "SELECT constraint_type FROM duckdb_constraints() WHERE table_name = 't' ORDER BY 1",
    ),
    Probe(
        "information_schema_columns",
        "metadata",
        "SELECT column_name FROM information_schema.columns WHERE table_name = 't' ORDER BY ordinal_position",
    ),
    Probe(
        "information_schema_tables", "metadata", "SELECT count(*) FROM information_schema.tables WHERE table_name = 't'"
    ),
    Probe("pragma_table_info", "metadata", "SELECT name, type FROM pragma_table_info('{t}')"),
    Probe("show_tables_contains", "metadata", "SELECT count(*) FROM (SHOW ALL TABLES) WHERE name = 't'"),
    Probe("summarize", "metadata", "SELECT column_name FROM (SUMMARIZE SELECT * FROM {t}) ORDER BY 1"),
    # semantics --------------------------------------------------------------------------------
    Probe("rowid_select", "semantics", "SELECT count(rowid) FROM {t}"),
    Probe("not_null_is_enforced", "semantics", "INSERT INTO {t} VALUES (NULL, 'x', 1)"),
    Probe("type_error_surfaces", "semantics", "INSERT INTO {t} VALUES ('not an int', 'x', 1)"),
    Probe("empty_result_shape", "semantics", "SELECT * FROM {t} WHERE id = 999"),
    Probe("prepared_statement", "semantics", "PREPARE p AS SELECT count(*) FROM {t}; EXECUTE p"),
    # transactions -----------------------------------------------------------------------------
    Probe("rollback_insert", "transactions", "BEGIN; INSERT INTO {t} VALUES (4, 'd', 40); ROLLBACK", observe_at=1),
    Probe("commit_insert", "transactions", "BEGIN; INSERT INTO {t} VALUES (4, 'd', 40); COMMIT", observe_at=1),
    Probe("rollback_delete", "transactions", "BEGIN; DELETE FROM {t} WHERE id = 1; ROLLBACK", observe_at=1),
]

PROBES_BY_NAME = {p.name: p for p in PROBES}


# --- the gap ledger -----------------------------------------------------------------------------
# (probe, backend) -> (status, reason). Anything not listed must reach parity.
#
# Adding a row here is a decision, not bookkeeping: it says this divergence is known and accepted.
# Removing one is how a closed gap gets noticed.

_RETURNING = ("unsupported", "the provider refuses RETURNING at bind time")

# Two blockers. The binder takes its conflict target from GetStorageInfo().index_info, which the
# provider entry leaves empty -- so declaring a PK constraint does not help. And 1.5.4 rewrites every
# on_conflict_info (INSERT OR REPLACE/IGNORE included) into MERGE INTO, which lands on
# Catalog::PlanMergeInto; the provider implements only PlanInsert/Update/Delete.
_NO_CONFLICT_TARGET = ("unsupported", "no unique index_info to plan against, and no PlanMergeInto")

_BRIDGE_ALTER = ("unsupported", "ALTER TABLE on bridge entries is rejected by design")
_PROVIDER_ALTER = ("unsupported", "provider ALTER covers only ADD/DROP/RENAME COLUMN")

# Stateless by design: a write commits on the backing store as the statement runs, independently of
# the target transaction. ROLLBACK does not take it back, and COMMIT has nothing left to do -- an
# outside reader already sees the row.
_NO_TXN = ("differs", "writes commit on the backing store as they run, outside the target transaction")

LEDGER: dict[tuple[str, str], tuple[str, str]] = {
    ("insert_returning", "provider"): _RETURNING,
    ("update_returning", "provider"): _RETURNING,
    ("delete_returning", "provider"): _RETURNING,
    ("insert_on_conflict_do_nothing", "provider"): _NO_CONFLICT_TARGET,
    ("insert_on_conflict_do_update", "provider"): _NO_CONFLICT_TARGET,
    ("insert_or_replace", "provider"): _NO_CONFLICT_TARGET,
    ("insert_or_ignore", "provider"): _NO_CONFLICT_TARGET,
    ("add_column", "bridge"): _BRIDGE_ALTER,
    ("drop_column", "bridge"): _BRIDGE_ALTER,
    ("rename_column", "bridge"): _BRIDGE_ALTER,
    ("alter_column_type", "bridge"): _BRIDGE_ALTER,
    ("set_default", "bridge"): _BRIDGE_ALTER,
    ("add_not_null", "bridge"): _BRIDGE_ALTER,
    ("alter_column_type", "provider"): _PROVIDER_ALTER,
    ("set_default", "provider"): _PROVIDER_ALTER,
    ("add_not_null", "provider"): _PROVIDER_ALTER,
    ("drop_table", "bridge"): ("unsupported", "DROP on a bridge entry is rejected; detach instead"),
    ("drop_table", "provider"): ("unsupported", "DROP on a provider entry is rejected; invalidate instead"),
    # A bridge entry copies the source's constraints; a provider has no source table to copy from,
    # and its declared key is metadata the host does not enforce (see insert_duplicate_key_raises).
    ("duckdb_constraints", "provider"): ("differs", "nothing backs a constraint, so none is declared"),
    # The bridge's writes ride the source connection the target's transaction owns, so they commit
    # and roll back with it. A provider's host has no transaction to join.
    ("rollback_insert", "provider"): _NO_TXN,
    ("commit_insert", "provider"): _NO_TXN,
    ("rollback_delete", "provider"): _NO_TXN,
    # Constraint enforcement lives in the backing store. The bridge inherits the source's, so it
    # matches native; a provider has none unless its host implements them.
    ("insert_duplicate_key_raises", "provider"): (
        "accepts-invalid",
        "provider host enforces no constraints; the stub accepts the duplicate",
    ),
    ("not_null_is_enforced", "provider"): (
        "accepts-invalid",
        "provider host enforces no constraints; the stub accepts the NULL",
    ),
}
