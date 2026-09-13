"""Seeded DuckDB fixtures for the test server.

Every served connection gets its own writable in-memory catalog (`seed_memory`),
so tests can UPDATE/INSERT without seeing each other's writes.
"""

import duckdb

from n6k_server.bridge import bridge
from n6k_server.extension import load_n6k
from n6k_server.test_server.fixtures import fixture_statements

# Clients that ATTACH with this alias get a bridge-backed catalog instead of the
# plain native one — used by the network→bridge→table chain test.
BRIDGE_CATALOG = "bridge_db"
_BRIDGE_CONN_CONFIG: dict[str, str | bool | int | float | list[str]] = {"allow_unsigned_extensions": "true"}


def seed_memory(con: duckdb.DuckDBPyConnection, catalog: str) -> None:
    con.execute(f"ATTACH ':memory:' AS \"{catalog}\"")
    seed_tables(con, catalog)


def seed_bridge(catalog: str) -> tuple[duckdb.DuckDBPyConnection, duckdb.DuckDBPyConnection]:
    """Build a bridge-backed catalog for the chain test.

    A source connection holds the real tables; they are bridged into `catalog`
    (a `TYPE virtual_catalog_bridge` attach) on the target connection with mixed
    permissions. Returns `(target, source)` — the caller must keep `source` alive
    for the session's lifetime, since the bridge reads from it on every query.
    """
    source = duckdb.connect(config=_BRIDGE_CONN_CONFIG)
    source.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name VARCHAR)")
    source.execute("INSERT INTO items VALUES (1, 'Widget'), (2, 'Gadget')")
    source.execute("CREATE TABLE readonly_items (id INTEGER PRIMARY KEY, name VARCHAR)")
    source.execute("INSERT INTO readonly_items VALUES (1, 'Locked')")

    target = duckdb.connect(config=_BRIDGE_CONN_CONFIG)
    # `target` is handed to the serving CALL, and the reactor builds every statement
    # with the C++ SQL builders directly (src/common/n6k_sql_builder.cpp).
    load_n6k(target)
    bridge(
        source,
        target,
        catalog,
        source_catalog="memory",
        permissions={"main.items": "readwrite", "main.readonly_items": "read"},
        primary_keys={"main.items": ("id",)},
    )
    return target, source


def seed_tables(con: duckdb.DuckDBPyConnection, catalog: str) -> None:
    for statement in fixture_statements(catalog):
        con.execute(statement)
