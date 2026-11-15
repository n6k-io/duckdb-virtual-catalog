"""Register ordinary Python functions as DuckDB table functions.

```python
def forecast(region: str, sims: int) -> pa.Table:
    \"\"\"Simulate demand for a region.\"\"\"
    ...

register_table_functions(con, catalog="db", functions=[TableFunction(forecast, returns=FORECAST)])
```

`SELECT * FROM db.main.forecast('north', 100)` then calls `forecast(region='north', sims=100)`, and
`duckdb_functions()` reports `forecast(region VARCHAR, sims BIGINT)` with the docstring as its
description.

- **SQL parameters** are the function's own parameters, minus `inject` names and a `pa.Table`
  parameter. Each annotation becomes the DuckDB type the binder checks calls against; a missing or
  unmappable annotation becomes `ANY`.
- **`inject`** maps a parameter name to a factory called once per call, for values the host supplies
  rather than SQL (a session, a connection).
- **A parameter annotated `pa.Table`** receives the rows an `RPC_TABLE` call pushed (`None` otherwise).
- **Return**: a plain function returns one table; a sync or async generator yields many. Each table
  may be a `pa.Table`, a `pa.RecordBatch`, or anything `pa.table()` accepts (a pandas DataFrame,
  a dict of columns), and is cast to `returns`.

Built on `register_rpc_streams`, which this only adapts to.
"""

import asyncio
import datetime
import inspect
import typing
import uuid
from collections.abc import AsyncIterator, Callable, Iterable, Mapping
from dataclasses import dataclass, field
from types import NoneType, UnionType
from typing import Any

import duckdb
import pyarrow as pa

from virtual_catalog.rpc_stream import HostStreamUdfs, RpcRegistry, StreamingRpc, register_rpc_streams

ANY = "ANY"

# DuckDBPyType has no mapping for these, and they cross the extension as text, so each also needs a
# parser back to the annotated type.
_TEXT_TYPES: dict[Any, tuple[str, Callable[[str], Any]]] = {
    datetime.datetime: ("TIMESTAMP", datetime.datetime.fromisoformat),
    datetime.date: ("DATE", datetime.date.fromisoformat),
    datetime.time: ("TIME", datetime.time.fromisoformat),
    uuid.UUID: ("UUID", uuid.UUID),
}


@dataclass(frozen=True)
class TableFunction:
    fn: Callable[..., Any]
    returns: pa.Schema
    name: str | None = None
    description: str | None = None
    inject: Mapping[str, Callable[[], Any]] = field(default_factory=dict)


@dataclass(frozen=True)
class _SqlParam:
    name: str
    duckdb_type: str
    parse: Callable[[Any], Any] | None


def _strip_optional(annotation: Any) -> Any:
    if typing.get_origin(annotation) in (typing.Union, UnionType):
        args = [a for a in typing.get_args(annotation) if a is not NoneType]
        if len(args) == 1:
            return args[0]
    return annotation


def _sql_param(name: str, annotation: Any) -> _SqlParam:
    if annotation is inspect.Parameter.empty:
        return _SqlParam(name, ANY, None)
    base = _strip_optional(annotation)
    if base in _TEXT_TYPES:
        duckdb_type, parse = _TEXT_TYPES[base]
        return _SqlParam(name, duckdb_type, parse)
    try:
        return _SqlParam(name, str(duckdb.sqltypes.DuckDBPyType(base)), None)
    except TypeError:
        return _SqlParam(name, ANY, None)


def _signature(fn: Callable[..., Any], inject: Mapping[str, Any]) -> tuple[list[_SqlParam], str | None]:
    """The SQL parameters, in order, and the name of the pushed-rows parameter if there is one."""
    try:
        hints = typing.get_type_hints(fn)
    except (NameError, TypeError):  # unresolvable string annotations: fall back to the raw ones
        hints = {}
    sql_params: list[_SqlParam] = []
    table_param: str | None = None
    for p in inspect.signature(fn).parameters.values():
        if p.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            raise TypeError(f"table function {fn.__name__!r} cannot take *{p.name}: SQL arguments bind by name")
        if p.name in inject:
            continue
        annotation = hints.get(p.name, p.annotation)
        if _strip_optional(annotation) is pa.Table:
            table_param = p.name
            continue
        sql_params.append(_sql_param(p.name, annotation))
    missing = set(inject) - set(inspect.signature(fn).parameters)
    if missing:
        raise TypeError(f"table function {fn.__name__!r} has no parameter(s) {sorted(missing)} to inject")
    return sql_params, table_param


def _as_table(value: Any, returns: pa.Schema) -> pa.Table | pa.RecordBatch:
    if not isinstance(value, (pa.Table, pa.RecordBatch)):
        value = pa.table(value)
    if value.schema.equals(returns):
        return value
    return value.select(returns.names).cast(returns)


def _adapt(spec: TableFunction, name: str) -> tuple[Any, pa.Schema, list[tuple[str, str]], str | None]:
    fn = spec.fn
    sql_params, table_param = _signature(fn, spec.inject)

    def kwargs_for(scalars: list[Any], table: pa.Table | None) -> dict[str, Any]:
        if len(scalars) != len(sql_params):
            raise TypeError(f"table function {name!r} takes {len(sql_params)} argument(s), got {len(scalars)}")
        kwargs = {
            p.name: (p.parse(v) if p.parse is not None and isinstance(v, str) else v)
            for p, v in zip(sql_params, scalars)
        }
        kwargs.update({k: factory() for k, factory in spec.inject.items()})
        if table_param is not None:
            kwargs[table_param] = table
        return kwargs

    entry: Any
    if inspect.isasyncgenfunction(fn) or inspect.isgeneratorfunction(fn) or inspect.iscoroutinefunction(fn):

        async def stream(scalars: list[Any], table: pa.Table | None) -> AsyncIterator[Any]:
            result = fn(**kwargs_for(scalars, table))
            if inspect.isasyncgen(result):
                async for value in result:
                    yield _as_table(value, spec.returns)
            elif inspect.iscoroutine(result):
                yield _as_table(await result, spec.returns)
            else:
                # A sync generator runs on the loop thread, so each step blocks it; yield batches.
                for value in result:
                    yield _as_table(value, spec.returns)
                    await asyncio.sleep(0)

        entry = StreamingRpc(stream)
    else:

        def finite(scalars: list[Any], table: pa.Table | None) -> pa.Table:
            result = _as_table(fn(**kwargs_for(scalars, table)), spec.returns)
            return pa.Table.from_batches([result]) if isinstance(result, pa.RecordBatch) else result

        entry = finite

    description = spec.description if spec.description is not None else inspect.getdoc(fn)
    return entry, spec.returns, [(p.name, p.duckdb_type) for p in sql_params], description


def register_table_functions(
    con: duckdb.DuckDBPyConnection,
    *,
    catalog: str,
    functions: Iterable[TableFunction],
    main_loop: asyncio.AbstractEventLoop | None = None,
) -> HostStreamUdfs:
    """Register `functions` as table functions in `catalog`.main on `con`.

    Same lifetime and threading rules as `register_rpc_streams`: generators and async functions run
    on `main_loop` (default: the loop running at call time); plain functions run on the DuckDB
    worker thread.
    """
    registry: RpcRegistry = {}
    for spec in functions:
        name = spec.name if spec.name is not None else spec.fn.__name__
        if name in registry:
            raise ValueError(f"table function {name!r} is registered twice")
        registry[name] = _adapt(spec, name)
    return register_rpc_streams(con, catalog=catalog, registry=registry, main_loop=main_loop)
