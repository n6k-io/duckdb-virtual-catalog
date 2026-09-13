"""n6k_table_describe: one fully-typed row per table.

Returns columns (name/type/nullable/default), primary_key, writeable, editable,
and enum domains in a single call — what useTableMeta needs without stitching
DESCRIBE + duckdb_constraints + per-column enum_range together.
"""

from typing import Any

import duckdb

from n6k_server.extension import load_n6k, load_virtual_catalog_bridge

CONN_CONFIG: dict[str, str | bool | int | float | list[str]] = {"allow_unsigned_extensions": "true"}


def _conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(config=CONN_CONFIG)
    load_n6k(conn)
    return conn


def _describe(conn: duckdb.DuckDBPyConnection, catalog: str, schema: str, table: str) -> dict[str, Any]:
    row = conn.sql(
        "SELECT columns, primary_key, writeable, editable, enums "
        f"FROM n6k_table_describe('{catalog}', schema := '{schema}', \"table\" := '{table}')"
    ).fetchone()
    assert row is not None
    cols, pk, writeable, editable, enums = row
    return {
        "columns": cols,
        "primary_key": pk,
        "writeable": writeable,
        "editable": editable,
        "enums": enums,
    }


def test_describe_native_table() -> None:
    conn = _conn()
    conn.execute("CREATE TYPE mood AS ENUM ('happy', 'sad')")
    conn.execute(
        "CREATE TABLE memory.main.t(id INTEGER PRIMARY KEY, name VARCHAR NOT NULL, m mood, n INTEGER DEFAULT 5)"
    )

    d = _describe(conn, "memory", "main", "t")
    by_name = {c["name"]: c for c in d["columns"]}

    assert [c["name"] for c in d["columns"]] == ["id", "name", "m", "n"]
    assert by_name["name"]["nullable"] is False
    assert by_name["n"]["nullable"] is True
    assert by_name["n"]["default"] == "5"
    assert by_name["name"]["default"] is None
    assert d["primary_key"] == ["id"]
    assert d["writeable"] is True
    assert d["editable"] is True
    assert d["enums"] == {"m": ["happy", "sad"]}


def test_describe_view() -> None:
    conn = _conn()
    conn.execute("CREATE TABLE memory.main.t(id INTEGER)")
    conn.execute("CREATE VIEW memory.main.v AS SELECT id FROM memory.main.t")

    d = _describe(conn, "memory", "main", "v")
    assert d["writeable"] is False
    assert d["editable"] is False
    assert d["primary_key"] == []
    assert [c["name"] for c in d["columns"]] == ["id"]


def test_describe_bridge_readwrite_table() -> None:
    from n6k_server.bridge import bridge

    source = duckdb.connect(config=CONN_CONFIG)
    source.sql("CREATE TABLE rw(id INTEGER PRIMARY KEY, label VARCHAR)")

    target = _conn()
    load_virtual_catalog_bridge(target)
    bridge(
        source,
        target,
        "ws",
        source_catalog="memory",
        permissions={"main.rw": "readwrite"},
    )

    d = _describe(target, "ws", "main", "rw")
    assert d["writeable"] is True
    assert d["editable"] is False  # bridge ALTER unsupported
    assert d["primary_key"] == ["id"]
    assert [c["name"] for c in d["columns"]] == ["id", "label"]
