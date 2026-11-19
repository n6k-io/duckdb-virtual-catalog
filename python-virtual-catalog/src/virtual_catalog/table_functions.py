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
  parameter. Each annotation becomes the DuckDB type the binder checks calls against. A missing or
  unmappable annotation is a `TypeError` at registration; annotate `typing.Any` to take any type.
  `Annotated[T, ...]` maps as `T`. Parameters can be passed by name, and one with a default may be
  left out; a default must be `None`, a bool, a number, a string, a date/time or a UUID.
- **`inject`** maps a parameter name to a factory returning a context manager, for values the host
  supplies rather than SQL (a session, a connection). It is entered once per call and exited when the
  call returns or its stream ends or is closed. Wrap a plain value in `contextlib.nullcontext`.
- **A parameter annotated `pa.Table`** receives the rows an `RPC_TABLE` call pushed (`None` otherwise).
- **Return**: a plain function returns one table; a sync or async generator yields many. Each value
  is cast to `returns` and may be a `pa.Table`, a `pa.RecordBatch`, a pandas DataFrame, a `dict`
  (one row) or a `list` of dicts (rows).

Built on `register_rpc_streams`, which this only adapts to.
"""

import asyncio
import dataclasses
import datetime
import inspect
import sys
import typing
import uuid
from collections.abc import AsyncIterator, Callable, Iterable, Mapping
from contextlib import AbstractContextManager, ExitStack
from dataclasses import dataclass, field
from types import NoneType, UnionType
from typing import Any

import duckdb
import pyarrow as pa

from virtual_catalog.rpc_stream import (
    HostStreamUdfs,
    RpcDefault,
    RpcEntry,
    RpcParam,
    RpcRegistry,
    StreamingRpc,
    register_rpc_streams,
)

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
    inject: Mapping[str, Callable[[], AbstractContextManager[Any]]] = field(default_factory=dict)


@dataclass(frozen=True)
class _SqlParam:
    name: str
    duckdb_type: str
    parse: Callable[[Any], Any] | None
    has_default: bool = False
    default: RpcDefault = None


def _strip_optional(annotation: Any) -> Any:
    if typing.get_origin(annotation) in (typing.Union, UnionType):
        args = [a for a in typing.get_args(annotation) if a is not NoneType]
        if len(args) == 1:
            return args[0]
    return annotation


def _sql_param(fn_name: str, name: str, annotation: Any) -> _SqlParam:
    if annotation is inspect.Parameter.empty:
        raise TypeError(
            f"table function {fn_name!r} parameter {name!r} has no annotation; annotate it, or use typing.Any"
        )
    base = _strip_optional(annotation)
    if base is Any:
        return _SqlParam(name, ANY, None)
    if base in _TEXT_TYPES:
        duckdb_type, parse = _TEXT_TYPES[base]
        return _SqlParam(name, duckdb_type, parse)
    try:
        return _SqlParam(name, str(duckdb.sqltypes.DuckDBPyType(base)), None)
    except TypeError as e:
        raise TypeError(
            f"table function {fn_name!r} parameter {name!r}: cannot map {annotation!r} to a DuckDB type"
        ) from e


def _signature(fn: Callable[..., Any], inject: Mapping[str, Any]) -> tuple[list[_SqlParam], str | None]:
    """The SQL parameters, in order, and the name of the pushed-rows parameter if there is one."""
    try:
        hints = typing.get_type_hints(fn)
    except (NameError, TypeError) as e:
        raise TypeError(f"table function {fn.__name__!r} has annotations that cannot be resolved: {e}") from e
    parameters = inspect.signature(fn).parameters
    missing = set(inject) - set(parameters)
    if missing:
        raise TypeError(f"table function {fn.__name__!r} has no parameter(s) {sorted(missing)} to inject")
    sql_params: list[_SqlParam] = []
    table_param: str | None = None
    for p in parameters.values():
        if p.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            raise TypeError(f"table function {fn.__name__!r} cannot take *{p.name}: SQL arguments bind by name")
        if p.name in inject:
            continue
        annotation = hints.get(p.name, inspect.Parameter.empty)
        if _strip_optional(annotation) is pa.Table:
            table_param = p.name
            continue
        sql_param = _sql_param(fn.__name__, p.name, annotation)
        if p.default is not inspect.Parameter.empty:
            sql_param = dataclasses.replace(sql_param, has_default=True, default=_sql_default(fn.__name__, p))
        sql_params.append(sql_param)
    return sql_params, table_param


def _sql_default(fn_name: str, p: inspect.Parameter) -> RpcDefault:
    value = p.default
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (datetime.date, datetime.time)):  # datetime is a date
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    raise TypeError(
        f"table function {fn_name!r} parameter {p.name!r}: default {value!r} is not a None, bool, number, "
        "string, date, time, datetime or UUID"
    )


def _is_dataframe(value: Any) -> bool:
    pd = sys.modules.get("pandas")
    return pd is not None and isinstance(value, pd.DataFrame)


def _as_table(value: Any, returns: pa.Schema) -> pa.Table | pa.RecordBatch:
    table: pa.Table | pa.RecordBatch
    if isinstance(value, (pa.Table, pa.RecordBatch)):
        table = value
    elif isinstance(value, dict):
        return pa.Table.from_pylist([value], schema=returns)
    elif isinstance(value, list):
        return pa.Table.from_pylist(value, schema=returns)
    elif _is_dataframe(value):
        table = pa.Table.from_pandas(value, preserve_index=False)
    else:
        raise TypeError(
            f"a table function returned {type(value).__name__}; expected pa.Table, pa.RecordBatch, "
            "a pandas DataFrame, a dict (one row) or a list of dicts"
        )
    if table.schema.equals(returns):
        return table
    return table.select(returns.names).cast(returns)


def _adapt(spec: TableFunction, name: str) -> RpcEntry:
    fn = spec.fn
    sql_params, table_param = _signature(fn, spec.inject)

    def call_with(stack: ExitStack, scalars: list[Any], table: pa.Table | None) -> Any:
        if len(scalars) != len(sql_params):
            raise TypeError(f"table function {name!r} takes {len(sql_params)} argument(s), got {len(scalars)}")
        kwargs = {
            p.name: (p.parse(v) if p.parse is not None and isinstance(v, str) else v)
            for p, v in zip(sql_params, scalars)
        }
        kwargs.update({k: stack.enter_context(factory()) for k, factory in spec.inject.items()})
        if table_param is not None:
            kwargs[table_param] = table
        return fn(**kwargs)

    entry: Any
    if inspect.isasyncgenfunction(fn) or inspect.isgeneratorfunction(fn) or inspect.iscoroutinefunction(fn):

        async def stream(scalars: list[Any], table: pa.Table | None) -> AsyncIterator[Any]:
            with ExitStack() as stack:
                result = call_with(stack, scalars, table)
                if inspect.isasyncgen(result):
                    async for value in result:
                        yield _as_table(value, spec.returns)
                elif inspect.iscoroutine(result):
                    yield _as_table(await result, spec.returns)
                else:
                    # Pulled off the loop so a blocking producer doesn't stall it.
                    done = object()
                    try:
                        while (value := await asyncio.to_thread(next, result, done)) is not done:
                            yield _as_table(value, spec.returns)
                    finally:
                        result.close()

        entry = StreamingRpc(stream)
    else:

        def finite(scalars: list[Any], table: pa.Table | None) -> pa.Table:
            with ExitStack() as stack:
                result = _as_table(call_with(stack, scalars, table), spec.returns)
            return pa.Table.from_batches([result]) if isinstance(result, pa.RecordBatch) else result

        entry = finite

    description = spec.description if spec.description is not None else inspect.getdoc(fn)
    return RpcEntry(
        fn=entry,
        schema=spec.returns,
        params=[RpcParam(p.name, p.duckdb_type, p.has_default, p.default) for p in sql_params],
        description=description,
    )


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
