"""Provider-backed dynamic virtual tables.

A `Provider` describes a namespace of virtual tables. Bind it 1:1 to
`(connection, catalog, schema_name)` via `register_provider()`. DuckDB
caches what the provider returns; `invalidate_provider_tables()` bumps a
version counter so the next lookup refills.

The extension sees one provider per catalog whose table names are `schema.table`;
`register_provider` multiplexes every schema's `Provider` behind one UDF set per
catalog and splits the qualified name on the way in.

Primary keys (required for UPDATE/DELETE) live in schema-level metadata
keyed by `PK_METADATA_KEY`, comma-separated.

## Example

```python
import asyncio, duckdb, pyarrow as pa
from n6k_server.provider import Provider, PK_METADATA_KEY, TableNotFound, register_provider

class Users(Provider):
    async def list_tables(self):
        return ["users"]
    async def schema(self, name):
        if name != "users":
            raise TableNotFound(name)
        return pa.schema(
            [("id", pa.int64()), ("name", pa.utf8())],
            metadata={PK_METADATA_KEY: b"id"},
        )
    async def scan(self, name, columns, filters):
        return pa.table({"id": [1, 2], "name": ["a", "b"]})
    async def insert(self, name, rows): return rows.num_rows
    async def update(self, name, rows, keys): return rows.num_rows
    async def delete(self, name, keys): return keys.num_rows

async def main():
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute("ATTACH ':memory:' AS app (TYPE virtual_catalog_provider)")
    register_provider(con, catalog="app", schema_name="public", provider=Users())

asyncio.run(main())
```

## Optional write/edit operations

`list_tables`, `schema`, and `scan` are required. `insert`, `update`, `delete`,
and `alter` are **optional** — a provider supports an operation only by
implementing the method. An unimplemented op is rejected at plan time and
reported as `writeable` / `editable = false` by the `n6k_table_permissions`
SQL function (`writeable` keys on INSERT support; `editable` on `alter`).

## Limitations

- Provider methods must not call `conn.sql()` on the same connection
  (deadlock). Use `conn.cursor()`.
- `RETURNING` on INSERT/UPDATE/DELETE is not supported.
- Only `add_column`, `drop_column`, `rename_column` reach `alter()`; other
  ALTER kinds raise `BinderException` client-side.
- Primary-key columns must be a fixed-width signed integer, `pa.utf8()`, or
  `pa.large_string()`. UPDATE/DELETE read the key values straight out of the
  Arrow buffers `scan` returned, so the layout has to be one the key reader
  knows; any other is refused with an error naming it, never misread. Non-key
  columns are unrestricted.

## How it crosses into C++

Every UDF below speaks Arrow, in both directions. `schema` answers with an Arrow
IPC *schema message* (`pa.Schema.serialize()`), carrying column names, types and
the `PK_METADATA_KEY` metadata in one payload; `scan` answers with a whole IPC
*stream*; and the write ops receive one. Nothing is rendered into SQL text and
nothing lands in a temp table on the way past, so the rows the provider returns
are decoded exactly once, by the extension that asked for them.
"""

import asyncio
import io
import json
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Optional

import duckdb
import pyarrow as pa
import pyarrow.ipc as ipc

from n6k_server.extension import load_virtual_catalog_provider
from n6k_protocol.filters import Filters, filters_from_wire

_VARCHAR = duckdb.sqltype("VARCHAR")
_VARCHAR_LIST = duckdb.sqltype("VARCHAR[]")
_BIGINT = duckdb.sqltype("BIGINT")
_BLOB = duckdb.sqltype("BLOB")

PK_METADATA_KEY = b"vcat.primary_keys"


class TableNotFound(KeyError):
    """Raised by `Provider` methods when the requested table name is unknown."""


@dataclass(frozen=True)
class AlterChange:
    """`ALTER TABLE` payload passed to `Provider.alter`.

    - `kind` — one of `"add_column"`, `"drop_column"`, `"rename_column"`.
    - `details` — kind-specific fields (e.g. `name` / `type` for add,
      `old_name` / `new_name` for rename).
    """

    kind: str
    details: dict[str, Any]


class Provider(ABC):
    """Abstract base for a Python-backed virtual-table namespace. Subclass and implement."""

    _binding: Optional[tuple[duckdb.DuckDBPyConnection, str, str, asyncio.AbstractEventLoop]] = None
    _probe_id: Optional[str] = None

    @abstractmethod
    async def list_tables(self) -> list[str]: ...

    @abstractmethod
    async def schema(self, name: str) -> pa.Schema:
        """Return the Arrow schema for `name`. Raise `TableNotFound` if unknown."""

    @abstractmethod
    async def scan(
        self,
        name: str,
        columns: Optional[list[str]],
        filters: Filters,
    ) -> pa.Table:
        """Read rows from `name`.

        - `columns` — projection DuckDB wants, or `None` for all. Push down
          if you can; a superset is also fine (the library projects down).
        - `filters` — list of `(col, op, value)` tuples or `None`. The
          provider **must** apply them; DuckDB does not re-filter. Operators
          are in `n6k_protocol.filters.FilterOp`.

        Can't push down? Use `n6k_protocol.filters.filter_and_project()` on the
        full table. Parquet-backed? `split_pyarrow_filters()` partitions
        the clauses pyarrow understands from the `is_null` / `is_not_null`
        leftover.

        LIMIT is not pushed. Return a full Arrow table; no streaming in v1.
        """

    async def insert(self, name: str, rows: pa.Table) -> int:
        """Append `rows`. Return the row count written.

        Optional; override to support INSERT."""
        raise NotImplementedError(f"{type(self).__name__} does not implement insert()")

    async def update(self, name: str, rows: pa.Table, keys: list[str]) -> int:
        """Update by primary key. `rows` has PK + changed cols; `keys` names the changed cols.

        Optional; override to support UPDATE (also requires a primary key)."""
        raise NotImplementedError(f"{type(self).__name__} does not implement update()")

    async def delete(self, name: str, keys: pa.Table) -> int:
        """Delete by primary key. `keys` is an Arrow table of PK column values.

        Optional; override to support DELETE (also requires a primary key)."""
        raise NotImplementedError(f"{type(self).__name__} does not implement delete()")

    async def alter(self, name: str, change: AlterChange) -> None:
        """Apply a schema change. Optional; override to support ALTER TABLE."""
        raise NotImplementedError(f"{type(self).__name__} does not implement alter()")

    async def invalidate(self) -> None:
        """Bump the version counter so the next catalog lookup refills from this provider."""
        if self._binding is None:
            raise RuntimeError("Provider is not registered; call register_provider(...) first")
        con, catalog, _, _ = self._binding
        await invalidate_provider_tables(con, catalog=catalog)

    async def unregister(self) -> None:
        """Detach this provider from its `(catalog, schema)` binding.

        Drops the C++ registry entry, clears the schema's provider pointer,
        removes the registered per-probe UDFs from the connection, and clears
        `_binding` so the instance can be re-registered elsewhere. Plans
        bound before this call keep working via captured refs."""
        if self._binding is None:
            raise RuntimeError("Provider is not registered; call register_provider(...) first")
        con, catalog, schema_name, _ = self._binding
        await unregister_provider(con, catalog=catalog, schema_name=schema_name, provider=self)


_OPTIONAL_OPS = ("insert", "update", "delete", "alter")


def _implemented_optional_ops(provider: "Provider") -> set[str]:
    return {op for op in _OPTIONAL_OPS if getattr(type(provider), op) is not getattr(Provider, op)}


def _run_async(coro: Any, loop: asyncio.AbstractEventLoop, timeout: Optional[float] = None) -> Any:
    """Bridge an async call to a sync context. Called from DuckDB worker
    threads — never from the event-loop thread (would deadlock)."""
    fut = asyncio.run_coroutine_threadsafe(coro, loop)
    return fut.result(timeout=timeout)


def _encode_table(table: pa.Table) -> bytes:
    """``table`` as one Arrow IPC stream, which is what C++ decodes."""
    sink = io.BytesIO()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()


def _decode_rows(payload: bytes) -> pa.Table:
    """The rows C++ encoded for a write op."""
    return ipc.open_stream(payload).read_all()


_UDF_SIGNATURES: dict[str, tuple[list[Any], Any]] = {
    "list": ([], _VARCHAR),
    "schema": ([_VARCHAR], _BLOB),
    "scan": ([_VARCHAR, _VARCHAR_LIST, _VARCHAR], _BLOB),
    "insert": ([_VARCHAR, _BLOB], _BIGINT),
    "update": ([_VARCHAR, _BLOB, _VARCHAR], _BIGINT),
    "delete": ([_VARCHAR, _BLOB], _BIGINT),
    "alter": ([_VARCHAR, _VARCHAR, _VARCHAR], _VARCHAR),
}
_ALL_OPS = tuple(_UDF_SIGNATURES)


class _CatalogProviders:
    """The one UDF set the extension sees for a catalog, fanned out to a `Provider` per schema."""

    def __init__(self, con: duckdb.DuckDBPyConnection, catalog: str) -> None:
        self.con = con
        self.catalog = catalog
        self.probe_id = uuid.uuid4().hex
        self.providers: dict[str, Provider] = {}
        self.registered_ops: set[str] = set()

    def udf_name(self, op: str) -> str:
        return f"__n6k_provider_{op}_{self.probe_id}"

    def _split(self, name: str) -> tuple[Provider, str]:
        schema, _, table = name.rpartition(".")
        provider = self.providers.get(schema)
        if provider is None:
            raise TableNotFound(name)
        return provider, table

    def _loop(self, provider: Provider) -> asyncio.AbstractEventLoop:
        assert provider._binding is not None
        return provider._binding[3]

    def _do_list(self) -> str:
        names: list[str] = []
        for schema, provider in self.providers.items():
            names.extend(f"{schema}.{t}" for t in _run_async(provider.list_tables(), self._loop(provider)))
        return "|".join(names)

    def _do_schema(self, name: str) -> bytes:
        provider, table = self._split(name)
        schema = _run_async(provider.schema(table), self._loop(provider))
        if not isinstance(schema, pa.Schema):
            raise TypeError(f"Provider.schema({table!r}) must return pa.Schema, " f"got {type(schema).__name__}")
        # One Arrow IPC schema message. Column names, types and the primary keys
        # (metadata, PK_METADATA_KEY) all travel inside it, so C++ reads the
        # provider's own declaration rather than a re-rendering of it.
        return bytes(schema.serialize().to_pybytes())

    def _do_scan(self, name: str, columns: Optional[list[str]], filters_json: str) -> bytes:
        provider, table_name = self._split(name)
        # `columns` is the exact output layout C++ wants: the projection with the
        # primary key columns trailing it, which repeats a key that is also
        # projected. The provider is asked only for the distinct ones — the
        # duplication is a row-id implementation detail it should never see.
        output = list(columns or [])
        wanted = list(dict.fromkeys(output)) or None
        # Wire format from C++ is ``[[col, op, value], ...]``, with a 4th element
        # naming how to rebuild a literal that crossed as text. The tag is what
        # makes this cheap: the type travels with the value, so a filtered scan
        # costs no extra call to the provider.
        filters: Filters = filters_from_wire(json.loads(filters_json)) if filters_json else None
        table = _run_async(provider.scan(table_name, columns=wanted, filters=filters), self._loop(provider))
        if not isinstance(table, pa.Table):
            raise TypeError(f"Provider.scan({table_name!r}) must return pa.Table, " f"got {type(table).__name__}")
        # Post-project as a safety net for providers that ignored `columns` or
        # returned a superset, and as the step that lays the columns out the way
        # C++ reads them.
        if output and list(table.column_names) != output:
            table = table.select(output)
        return _encode_table(table)

    def _do_insert(self, name: str, rows_ipc: bytes) -> int:
        provider, table = self._split(name)
        return int(_run_async(provider.insert(table, _decode_rows(rows_ipc)), self._loop(provider)))

    def _do_update(self, name: str, rows_ipc: bytes, changed_csv: str) -> int:
        provider, table = self._split(name)
        changed = changed_csv.split(",") if changed_csv else []
        return int(_run_async(provider.update(table, _decode_rows(rows_ipc), changed), self._loop(provider)))

    def _do_delete(self, name: str, keys_ipc: bytes) -> int:
        provider, table = self._split(name)
        return int(_run_async(provider.delete(table, _decode_rows(keys_ipc)), self._loop(provider)))

    def _do_alter(self, name: str, kind: str, details_json: str) -> str:
        provider, table = self._split(name)
        details = json.loads(details_json) if details_json else {}
        _run_async(provider.alter(table, AlterChange(kind, details)), self._loop(provider))
        return ""

    def _impl(self, op: str) -> Callable[..., Any]:
        return getattr(self, f"_do_{op}")  # type: ignore[no-any-return]

    def sync(self) -> None:
        """Make the extension's UDF set match the union of what the bound providers implement."""
        wanted = {"list", "schema", "scan"}
        for provider in self.providers.values():
            wanted |= _implemented_optional_ops(provider)
        for op in wanted - self.registered_ops:
            args, ret = _UDF_SIGNATURES[op]
            self.con.create_function(self.udf_name(op), self._impl(op), args, ret)
        for op in self.registered_ops - wanted:
            self.con.remove_function(self.udf_name(op))
        self.registered_ops = wanted
        with self.con.cursor() as cur:
            cur.sql(
                "SELECT provider_register($1,$2,$3,$4,$5,$6,$7,$8)",
                params=[self.catalog] + [self.udf_name(op) if op in wanted else "" for op in _ALL_OPS],
            ).fetchone()

    def close(self) -> None:
        with self.con.cursor() as cur:
            cur.sql("SELECT provider_unregister($1)", params=[self.catalog]).fetchone()
        for op in self.registered_ops:
            self.con.remove_function(self.udf_name(op))
        self.registered_ops = set()


_catalogs: dict[tuple[int, str], _CatalogProviders] = {}


def register_provider(
    con: duckdb.DuckDBPyConnection,
    *,
    catalog: str,
    schema_name: str,
    provider: Provider,
    main_loop: Optional[asyncio.AbstractEventLoop] = None,
) -> None:
    """Bind `provider` to `(con, catalog, schema_name)`.

    The target catalog must be `TYPE virtual_catalog_provider`; the schema is
    materialised by the extension when the provider lists tables in it.

    Provider coroutines run on `main_loop` (default: the loop running at
    call time). That loop must own any loop-bound resources the provider
    uses (e.g. an asyncpg pool). After registration, the loop thread must
    not make synchronous DuckDB calls against `con` — use `asyncio.to_thread`.
    """
    if main_loop is None:
        main_loop = asyncio.get_running_loop()

    load_virtual_catalog_provider(con)
    key = (id(con), catalog)
    entry = _catalogs.get(key)
    if entry is None:
        entry = _CatalogProviders(con, catalog)
    if schema_name in entry.providers:
        raise RuntimeError(f"a provider is already registered for {catalog}.{schema_name}")
    provider._binding = (con, catalog, schema_name, main_loop)
    provider._probe_id = entry.probe_id
    entry.providers[schema_name] = provider
    try:
        entry.sync()
    except Exception:
        del entry.providers[schema_name]
        provider._binding = None
        provider._probe_id = None
        if not entry.providers:
            for op in entry.registered_ops:
                con.remove_function(entry.udf_name(op))
            _catalogs.pop(key, None)
        raise
    _catalogs[key] = entry


async def invalidate_provider_tables(con: duckdb.DuckDBPyConnection, *, catalog: str) -> None:
    """Bump the version counter on `catalog`; next lookup refills.

    In-flight queries keep their already-bound plans (DuckDB's standard
    catalog-versioning behavior). Only plans compiled after this call see
    the refreshed state.
    """

    def _call() -> None:
        with con.cursor() as cur:
            cur.sql("SELECT provider_invalidate_tables($1)", params=[catalog]).fetchone()

    await asyncio.to_thread(_call)


async def unregister_provider(
    con: duckdb.DuckDBPyConnection,
    *,
    catalog: str,
    schema_name: str,
    provider: Provider,
) -> None:
    """Detach `provider` from `(con, catalog, schema_name)`.

    The last provider on a catalog drops the extension's registry entry and the
    UDFs; any other re-registers the catalog without this schema. Clears
    `provider._binding`.

    In-flight queries keep their already-bound plans (DuckDB's standard
    catalog-versioning behavior). Only plans compiled after this call see
    the detach.
    """
    if provider._probe_id is None:
        raise RuntimeError("Provider is not registered; call register_provider(...) first")
    key = (id(con), catalog)
    entry = _catalogs.get(key)
    if entry is None or entry.providers.get(schema_name) is not provider:
        raise RuntimeError(f"{type(provider).__name__} is not registered for {catalog}.{schema_name}")

    def _call() -> None:
        del entry.providers[schema_name]
        if entry.providers:
            entry.sync()
            with con.cursor() as cur:
                cur.sql("SELECT provider_invalidate_tables($1)", params=[catalog]).fetchone()
        else:
            entry.close()
            del _catalogs[key]

    await asyncio.to_thread(_call)

    provider._binding = None
    provider._probe_id = None
