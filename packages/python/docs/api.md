
# n6k-duckdb

The distribution is `n6k-duckdb`; it installs two importable packages —
`n6k_protocol` (wire format, frame codec, filter helpers) and
`n6k_server` (bridge, provider, byte pump, FastAPI adapter).

Two ways to plug Python into DuckDB:

- **Bridge** — mirror tables from one DuckDB connection to another with
  per-table permissions. No network, no server.
- **Provider** — back a DuckDB schema with a Python class. DuckDB issues
  `SELECT` / `INSERT` / `UPDATE` / `DELETE`; your code answers.

An optional extra mounts a FastAPI server that speaks the n6k network protocol.

## Install

```bash
pip install n6k-duckdb                  # bridge + provider
pip install "n6k-duckdb[test-server]"   # + FastAPI server framework
```

---

## Bridge

```python
bridge(
    source: duckdb.DuckDBPyConnection,
    target: duckdb.DuckDBPyConnection,
    name: str,
    *,
    source_catalog: str,
    permissions: dict[str, str],
    primary_keys: dict[str, tuple[str, ...]] | None = None,
    lock: threading.Lock | None = None,
) -> str

unbridge(target: duckdb.DuckDBPyConnection, name: str) -> None
```

Mirror tables from `source` into a new catalog `name` on `target`.

One bridge is one attached catalog: `name` must not exist on `target` yet, and
`unbridge` (`DETACH name`) tears it down. Each granted source schema lands in the
target schema of the same name.

## Args

- `source`, `target` — DuckDB connections.
- `name` — catalog created on the target.
- `source_catalog` — where the source tables live.
- `permissions` — `{'schema.table': 'read' | 'readwrite'}`.
- `primary_keys` — `{'schema.table': (col, ...)}`; overrides PK auto-discovery.
- `lock` — serialises source access if given.

Returns the bridge id. Raises `duckdb.Error` for an empty, invalid, or
unqualified `permissions` key.

## Example

```python
import duckdb
from n6k_server.bridge import bridge

cfg = {"allow_unsigned_extensions": "true"}
source = duckdb.connect(config=cfg)
source.sql("CREATE TABLE users(id INTEGER PRIMARY KEY, name VARCHAR)")

target = duckdb.connect(config=cfg)
bridge(source, target, "app", source_catalog="memory", permissions={"main.users": "readwrite"})

target.sql("SELECT * FROM app.main.users")
```


---

## Provider

Provider-backed dynamic virtual tables.

A `Provider` describes a namespace of virtual tables. Bind it 1:1 to
`(connection, catalog, schema_name)` via `register_provider()`. DuckDB
caches what the provider returns; `invalidate_provider_tables()` bumps a
version counter so the next lookup refills.

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
- Primary-key columns must be a fixed-width signed integer or `pa.utf8()`.
  UPDATE/DELETE read the key values straight out of the Arrow buffers `scan`
  returned, and that reader knows those layouts only — `pa.large_string()` for a
  key column would be misread rather than rejected. Non-key columns are
  unrestricted.

## How it crosses into C++

Every UDF below speaks Arrow, in both directions. `schema` answers with an Arrow
IPC *schema message* (`pa.Schema.serialize()`), carrying column names, types and
the `PK_METADATA_KEY` metadata in one payload; `scan` answers with a whole IPC
*stream*; and the write ops receive one. Nothing is rendered into SQL text and
nothing lands in a temp table on the way past, so the rows the provider returns
are decoded exactly once, by the extension that asked for them.


### `Provider`

```python
class Provider()
```

Abstract base for a Python-backed virtual-table namespace. Subclass and implement.


#### `Provider.list_tables`

```python
list_tables(self) -> list[str]
```


#### `Provider.schema`

```python
schema(self, name: str) -> pyarrow.Schema
```

Return the Arrow schema for `name`. Raise `TableNotFound` if unknown.


#### `Provider.scan`

```python
scan(
    self,
    name: str,
    columns: list[str] | None,
    filters: Filters,
) -> pyarrow.Table
```

Read rows from `name`.

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


#### `Provider.insert`

```python
insert(self, name: str, rows: pyarrow.Table) -> int
```

Append `rows`. Return the row count written.

Optional; override to support INSERT.


#### `Provider.update`

```python
update(self, name: str, rows: pyarrow.Table, keys: list[str]) -> int
```

Update by primary key. `rows` has PK + changed cols; `keys` names the changed cols.

Optional; override to support UPDATE (also requires a primary key).


#### `Provider.delete`

```python
delete(self, name: str, keys: pyarrow.Table) -> int
```

Delete by primary key. `keys` is an Arrow table of PK column values.

Optional; override to support DELETE (also requires a primary key).


#### `Provider.alter`

```python
alter(self, name: str, change: AlterChange) -> None
```

Apply a schema change. Optional; override to support ALTER TABLE.


#### `Provider.invalidate`

```python
invalidate(self) -> None
```

Bump the version counter so the next catalog lookup refills from this provider.


### `register_provider`

```python
register_provider(
    con: duckdb.DuckDBPyConnection,
    *,
    catalog: str,
    schema_name: str,
    provider: Provider,
    main_loop: asyncio.AbstractEventLoop | None = None,
) -> None
```

Bind `provider` to `(con, catalog, schema_name)`.

The target catalog must be `TYPE virtual_catalog_provider`; the schema is
materialised by the extension when the provider lists tables in it. Several
schemas of one catalog may each have their own provider.

Provider coroutines run on `main_loop` (default: the loop running at
call time). That loop must own any loop-bound resources the provider
uses (e.g. an asyncpg pool). After registration, the loop thread must
not make synchronous DuckDB calls against `con` — use `asyncio.to_thread`.


### `invalidate_provider_tables`

```python
invalidate_provider_tables(
    con: duckdb.DuckDBPyConnection,
    *,
    catalog: str,
) -> None
```

Bump the version counter on `catalog`; next lookup refills.

In-flight queries keep their already-bound plans (DuckDB's standard
catalog-versioning behavior). Only plans compiled after this call see
the refreshed state.


### `TableNotFound`

```python
class TableNotFound
```

Raised by `Provider` methods when the requested table name is unknown.


### `AlterChange`

```python
class AlterChange(kind: str, details: dict[str, Any]) -> None
```

`ALTER TABLE` payload passed to `Provider.alter`.

- `kind` — one of `"add_column"`, `"drop_column"`, `"rename_column"`.
- `details` — kind-specific fields (e.g. `name` / `type` for add,
  `old_name` / `new_name` for rename).


### `PK_METADATA_KEY`

```python
PK_METADATA_KEY = b'n6k.primary_keys'
```


---

## Filter helpers

Filter-clause types and Arrow helpers for the `Provider` API.

Shape-compatible with pyarrow's tuple-form filter argument — a list of
`(col, op, value)` tuples, structurally identical to what
`pyarrow.parquet.read_table(filters=...)` and `pyarrow.dataset` accept.
Providers that read parquet can pass an n6k `filters` value straight to
pyarrow for the operators pyarrow understands; `is_null` and `is_not_null`
are n6k extras with no pyarrow tuple-form equivalent — use
`split_pyarrow_filters()` to partition them out, then apply the leftover
via `filter_and_project()`.


### `filter_and_project`

```python
filter_and_project(
    table: pyarrow.Table,
    columns: list[str] | None,
    filters: Filters,
) -> pyarrow.Table
```

Apply filters, then project, on an in-memory Arrow table.

Filters run first so a clause can reference a column that's absent from
`columns` (projected away on output). Every n6k operator is handled,
including `is_null` and `is_not_null`. Projection honours `columns`
(`None` keeps all columns).

Use this when your provider can't push projection or filters down at
all — load the full table and return `filter_and_project(tbl, columns, filters)`.


### `split_pyarrow_filters`

```python
split_pyarrow_filters(
    filters: Filters,
) -> tuple[list[FilterClause], list[FilterClause]]
```

The first list is directly passable to `pq.read_table(filters=...)` or
`pyarrow.dataset` — the operators `=`, `!=`, `<`, `<=`, `>`, `>=`, `in`
all map to pyarrow's tuple form unchanged.

The second list contains `is_null` / `is_not_null` clauses that
pyarrow's tuple form cannot express. Apply them after the parquet read
via `filter_and_project()` or `pyarrow.compute` directly.


---

## SQL functions (extension)

Table functions registered by the `n6k` DuckDB extension. Load it first
(`LOAD n6k_client;`), then call them like any table function.

### `n6k_table_permissions`

Enumerate tables in a catalog along with their write/edit capability.

```
n6k_table_permissions(catalog VARCHAR, schema := VARCHAR, table := VARCHAR)
```

- `catalog` *(positional, required)* — catalog/database name to inspect.
- `schema` *(named, optional)* — restrict to one schema.
- `table` *(named, optional)* — restrict to one table. `table` is a reserved
  word, so quote it: `"table" := 'foo'`.

Returns one row per table:

| column        | type           | description                                                            |
| ------------- | -------------- | ---------------------------------------------------------------------- |
| `schema`      | VARCHAR        | schema the table lives in                                              |
| `name`        | VARCHAR        | table name                                                             |
| `kind`        | VARCHAR        | `n6k_remote` for network tables; `native_table` / `native_view` for plain/bridge catalogs |
| `writeable`   | BOOLEAN        | INSERT/UPDATE/DELETE allowed                                           |
| `editable`    | BOOLEAN        | schema (DDL) changes allowed                                           |
| `primary_key` | VARCHAR[]      | primary-key column names in key order; empty list if none             |

```sql
-- every table in a catalog
SELECT * FROM n6k_table_permissions('app');

-- one schema
SELECT name, kind, writeable, editable, primary_key
FROM n6k_table_permissions('app', schema := 'main') ORDER BY name;

-- one table (note the quoted reserved word)
SELECT name, kind FROM n6k_table_permissions('app', schema := 'main', "table" := 'nt');
```

### `n6k_table_describe`

A single-row, fully-typed descriptor for one table — columns, primary key,
write/edit capability, and enum domains in one call (so a client doesn't have to
stitch `DESCRIBE` + `duckdb_constraints()` + per-column `enum_range()` together,
none of which see remote/bridge/provider tables correctly).

```
n6k_table_describe(catalog VARCHAR, schema := VARCHAR, table := VARCHAR)
```

- `catalog` *(positional, required)* — catalog/database name.
- `schema` *(named, optional)* — defaults to `main`.
- `table` *(named, required)* — `table` is a reserved word, so quote it:
  `"table" := 'foo'`.

Returns exactly one row:

| column        | type                       | description                                            |
| ------------- | -------------------------- | ------------------------------------------------------ |
| `columns`     | STRUCT(name, type, nullable, "default")[] | ordered column metadata                 |
| `primary_key` | VARCHAR[]                  | primary-key column names in key order; empty if none   |
| `writeable`   | BOOLEAN                    | INSERT/UPDATE/DELETE allowed                            |
| `editable`    | BOOLEAN                    | column DDL (ALTER) allowed                              |
| `enums`       | MAP(VARCHAR, VARCHAR[])    | enum-typed column name → its labels                    |

```sql
SELECT columns, primary_key, writeable, editable, enums
FROM n6k_table_describe('app', schema := 'main', "table" := 'users');
```

Note: for remote (`n6k_remote`) tables, `nullable` and `default` are not yet
transmitted over the wire, so columns report `nullable = true` / `default = NULL`;
columns, types, primary key, and the capability bits are populated for all kinds.

### `n6k_split_statements`

Split a multi-statement SQL string into its individual statements using
DuckDB's parser, so semicolons inside string literals, comments, or quoted
identifiers don't cause a wrong split (unlike a naive `split(';')`).

```
n6k_split_statements(sql VARCHAR)
```

Returns one row per statement, in order:

| column           | type    | description                                            |
| ---------------- | ------- | ------------------------------------------------------ |
| `ordinality`     | BIGINT  | 1-based position of the statement                      |
| `statement`      | VARCHAR | verbatim statement text, trimmed, trailing `;` removed |
| `statement_type` | VARCHAR | DuckDB statement type, e.g. `SELECT`, `INSERT`, `DELETE` |

```sql
SELECT * FROM n6k_split_statements('SELECT 1; INSERT INTO t VALUES (1);');
-- ordinality | statement                | statement_type
--     1      | SELECT 1                 | SELECT
--     2      | INSERT INTO t VALUES (1) | INSERT

-- a semicolon inside a literal stays in one statement
SELECT statement FROM n6k_split_statements('SELECT ''a;b;c''');
-- SELECT 'a;b;c'
```

---

## Server framework (optional)

Mount the n6k WebSocket endpoint on a FastAPI app.

    register(app, "/db/{tenant}", connect=open_db)

Mounts `<prefix>/ws`. `connect` is called once per WebSocket and returns the
DuckDB connection to serve it from — that is the whole contract:

    def open_db(ws, tenant):
        con = duckdb.connect()
        load_n6k_server(con)
        con.execute(f'ATTACH \':memory:\' AS "{tenant}"')
        return con

Every catalog that connection ATTACHed is served — the extension picks them, this
process does not name them. (It skips the startup in-memory database a connection
carries without asking for it, unless that is all there is; see
`n6k::ResolveServedCatalogs`.) Nothing else is configurable per connection, because
nothing else varies: the protocol is implemented by the `n6k_server` DuckDB
extension (C++), and this process only accepts the socket, hands it over, and
shuttles bytes. It never decodes a frame.

`connect` may be sync, async, or an async generator. Yield instead of returning
when you need teardown — the same shape as a FastAPI `Depends`:

    async def open_db(ws):
        con = duckdb.connect()
        live.append(con.cursor())     # a way in while the connection is served
        try:
            yield con                 # served here; resumes when the socket drops
        finally:
            live.pop()

Two ways to refuse a connection:

- **`raise WsReject(4401, "...")`** from `connect`, for a credential the transport
  carries — an `Authorization` header or a query param. This process terminates
  the HTTP upgrade, so it is the only thing that can see those.
- **Define `n6k_authorize(token, catalog) -> BOOLEAN`** on the returned connection
  (`con.create_function`). If it exists, the extension calls it for every FT_HELLO
  and refuses the session on `False`; raising sends your message to the client.
  That covers the credential a *browser* must send, which rides inside the
  handshake frame because a browser cannot set a header on a WS upgrade — and
  reading it here would mean parsing frames.

Registering the function is the whole opt-in; there is no auth setting.


### `register`

```python
register(
    app: fastapi.FastAPI,
    prefix: str,
    *,
    connect: Callable[..., duckdb.DuckDBPyConnection | Awaitable[duckdb.DuckDBPyConnection] | AsyncIterator[duckdb.DuckDBPyConnection]],
) -> fastapi.APIRouter
```

Mount `<prefix>/ws` on `app` and return the router.

`prefix` follows FastAPI's convention: empty for root, else starts with `/` and
does not end with `/`. Path params it captures are passed to `connect` as
keyword arguments, after the WebSocket.

Set `catalog_from_handshake=True` when this mount's factory resolves the
catalog with `resolve_ws_catalog` and clients may supply it in the FT_HELLO
frame instead of `?catalog=`. The adapter then reads one handshake frame when
the URL omits a catalog. Leave it False for mounts whose catalog comes from
the path / query only (e.g. a `{workspace}` path param) — otherwise the
adapter would block waiting for a HELLO frame such clients never send.

Set `multiplex=True` to serve MANY catalog sessions over one WebSocket (a
socket the host shares via `registerWebsocket`/`wsId`). The `/ws` route then
drives a `WsV2Router`: each client FT_HELLO opens a session and the `session`
factory is invoked per session as `session(token, catalog, **path_params)`
(not `session(ws, ...)` — the socket is shared). `catalog_from_handshake` and
`?catalog=` do not apply (a shared socket can't name N catalogs in its URL);
the catalog always rides each session's HELLO frame. `authenticate` still runs
once at connection accept, but it must NOT read a frame (the first frame is a
per-session HELLO, not a connection auth frame).


### `serve_and_close_connection`

```python
serve_and_close_connection(
    ws: 'WebSocketLike',
    con: 'duckdb.DuckDBPyConnection',
    catalogs: 'Optional[list[str]]' = None,
    *,
    ping_interval_ms: 'Optional[int]' = None,
    auth_function: 'Optional[str]' = None,
) -> 'None'
```

Serve `ws` from `con` until either side drops, then close `con`.

Everything `con` deliberately attached is served unless `catalogs` narrows it —
the extension decides which those are (`build_serve_sql`) — and if `auth_function`
is named that function must already be registered on `con`.

Ownership passes here: the serving CALL holds `con` for the connection's life,
so nothing else may touch it. A caller that needs to reach the data meanwhile
must take a `con.cursor()` — an independent connection on the same database —
*before* calling this.


### `WsReject`

```python
class WsReject(code: 'int' = 4400, reason: 'str' = '') -> 'None'
```

Raise from a `connect` factory to close the WebSocket with a specific code.

Transport-level, which is why it lives here rather than with the protocol: the
codes are RFC 6455's (4400 bad request, 4401 unauthorized, 4404 not found) and
the rejection happens before any frame is exchanged.

