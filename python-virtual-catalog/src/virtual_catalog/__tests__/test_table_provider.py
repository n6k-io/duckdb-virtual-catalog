import asyncio

import duckdb
import pyarrow as pa
import pytest

from virtual_catalog.extension import load_vcatprovider
from virtual_catalog.provider import TableNotFound, TableProvider, register_provider


class _Tables(TableProvider):
    def __init__(self, data: dict[str, pa.Table]) -> None:
        self._data = data

    async def tables(self) -> list[str]:
        return list(self._data)

    async def table(self, name: str) -> pa.Table:
        if name not in self._data:
            raise TableNotFound(name)
        return self._data[name]


def _conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    load_vcatprovider(conn)
    conn.execute("ATTACH ':memory:' AS workspace (TYPE vcatprovider)")
    conn.execute("CREATE SCHEMA workspace.public")
    return conn


@pytest.mark.asyncio
async def test_table_provider_lists_filters_and_projects() -> None:
    conn = _conn()
    provider = _Tables({"t": pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]}), "u": pa.table({"x": [1]})})
    register_provider(conn, catalog="workspace", schema_name="public", provider=provider)

    names = await asyncio.to_thread(
        lambda: conn.sql(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog='workspace' AND table_schema='public' ORDER BY table_name"
        ).fetchall()
    )
    assert [r[0] for r in names] == ["t", "u"]

    rows = await asyncio.to_thread(
        lambda: conn.sql("SELECT name FROM workspace.public.t WHERE id >= 2 ORDER BY name").fetchall()
    )
    assert rows == [("b",), ("c",)]


@pytest.mark.asyncio
async def test_table_provider_unknown_table() -> None:
    with pytest.raises(TableNotFound):
        await _Tables({}).schema("missing")
