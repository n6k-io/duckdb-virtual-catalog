"""Tests for `virtual_catalog.table_functions`. Registration needs the `vcatprovider` extension."""

import asyncio
import datetime
from collections.abc import AsyncIterator, Iterator
from typing import Any

import duckdb
import pyarrow as pa
import pytest

from virtual_catalog.extension import VCATPROVIDER, load_vcatprovider, local_extension_path
from virtual_catalog.table_functions import TableFunction, register_table_functions

CONN_CONFIG: dict[str, Any] = {"allow_unsigned_extensions": "true"}
_OUT_FIELDS: list[pa.Field[Any]] = [pa.field("region", pa.string()), pa.field("n", pa.int64())]
OUT = pa.schema(_OUT_FIELDS)


def _con() -> duckdb.DuckDBPyConnection:
    if local_extension_path(VCATPROVIDER) is None:
        pytest.skip("vcatprovider extension not built")
    con = duckdb.connect(config=CONN_CONFIG)
    load_vcatprovider(con)
    con.execute("ATTACH ':memory:' AS db")
    return con


def _signature(con: duckdb.DuckDBPyConnection, name: str) -> tuple[Any, ...]:
    row = con.execute(
        "SELECT parameters, parameter_types, varargs, description FROM duckdb_functions() "
        "WHERE database_name = 'db' AND function_name = ?",
        [name],
    ).fetchone()
    assert row is not None
    return tuple(row)


async def _query(con: duckdb.DuckDBPyConnection, sql: str) -> list[tuple[Any, ...]]:
    return await asyncio.to_thread(lambda: con.execute(sql).fetchall())


async def _register(con: duckdb.DuckDBPyConnection, *functions: TableFunction) -> Any:
    return await asyncio.to_thread(
        register_table_functions,
        con,
        catalog="db",
        functions=list(functions),
        main_loop=asyncio.get_running_loop(),
    )


@pytest.mark.asyncio
async def test_plain_function_is_typed_described_and_bound_by_name() -> None:
    def forecast(region: str, sims: int) -> Any:
        """Simulate demand."""
        return {"region": [region], "n": [sims * 2]}

    con = _con()
    await _register(con, TableFunction(forecast, returns=OUT))

    assert _signature(con, "forecast") == (["region", "sims"], ["VARCHAR", "BIGINT"], None, "Simulate demand.")
    assert await _query(con, "SELECT * FROM db.main.forecast('north', 4)") == [("north", 8)]
    with pytest.raises(duckdb.Error):
        await _query(con, "SELECT * FROM db.main.forecast('north')")


@pytest.mark.asyncio
async def test_unannotated_and_text_typed_params() -> None:
    def f(anything, day: datetime.date, at: datetime.datetime | None) -> pa.Table:  # type: ignore[no-untyped-def]
        assert isinstance(day, datetime.date) and isinstance(at, datetime.datetime)
        return pa.table({"region": [f"{anything}:{day.isoformat()}:{at.hour}"], "n": [1]}, schema=OUT)

    con = _con()
    await _register(con, TableFunction(f, returns=OUT, description="explicit"))

    assert _signature(con, "f") == (["anything", "day", "at"], ["ANY", "DATE", "TIMESTAMP"], None, "explicit")
    rows = await _query(con, "SELECT * FROM db.main.f(7, DATE '2024-01-02', TIMESTAMP '2024-01-02 05:00:00')")
    assert rows == [("7:2024-01-02:5", 1)]


@pytest.mark.asyncio
async def test_inject_and_pushed_table_are_not_sql_params() -> None:
    calls: list[Any] = []

    def f(session: object, n: int, rows: pa.Table | None) -> pa.Table:
        calls.append((session, rows))
        return pa.table({"region": ["x"], "n": [n]}, schema=OUT)

    con = _con()
    await _register(con, TableFunction(f, returns=OUT, inject={"session": lambda: "S"}))

    assert _signature(con, "f")[:2] == (["n"], ["BIGINT"])
    assert await _query(con, "SELECT * FROM db.main.f(3)") == [("x", 3)]
    assert calls == [("S", None)]


@pytest.mark.asyncio
async def test_generators_and_async_functions_stream() -> None:
    async def agen(n: int) -> AsyncIterator[Any]:
        for i in range(n):
            yield pa.record_batch({"region": ["a"], "n": [i]}, schema=OUT)

    def sgen(n: int) -> Iterator[Any]:
        for i in range(n):
            yield {"region": ["s"], "n": [i]}

    async def coro(n: int) -> pa.Table:
        return pa.table({"region": ["c"], "n": [n]}, schema=OUT)

    con = _con()
    await _register(
        con,
        TableFunction(agen, returns=OUT),
        TableFunction(sgen, returns=OUT),
        TableFunction(coro, returns=OUT, name="renamed"),
    )

    assert await _query(con, "SELECT n FROM db.main.agen(3) ORDER BY n") == [(0,), (1,), (2,)]
    assert await _query(con, "SELECT n FROM db.main.sgen(2) ORDER BY n") == [(0,), (1,)]
    assert await _query(con, "SELECT * FROM db.main.renamed(9)") == [("c", 9)]


@pytest.mark.asyncio
async def test_result_is_cast_to_returns() -> None:
    def f() -> pa.Table:
        return pa.table({"n": pa.array([1], pa.int32()), "region": ["r"], "extra": [0]})

    con = _con()
    await _register(con, TableFunction(f, returns=OUT))
    assert await _query(con, "SELECT * FROM db.main.f()") == [("r", 1)]


def test_bad_signatures_fail_at_registration() -> None:
    def star(*args: int) -> pa.Table:
        raise AssertionError

    def plain(n: int) -> pa.Table:
        raise AssertionError

    con = _con()
    with pytest.raises(TypeError, match=r"\*args"):
        register_table_functions(con, catalog="db", functions=[TableFunction(star, returns=OUT)], main_loop=None)
    with pytest.raises(TypeError, match="no parameter"):
        register_table_functions(
            con, catalog="db", functions=[TableFunction(plain, returns=OUT, inject={"nope": lambda: 1})]
        )
