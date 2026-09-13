# Architecture

## Extension layout

The repo builds three DuckDB extensions:

- **n6k_client** — network extension. Queries remote HTTP servers using Arrow IPC over HTTP(S). Requires an external server implementing the [n6k network protocol](n6k-network-protocol.md).
- **n6k_server** — serves attached catalogs over the n6k protocol.
- **n6k_testing** — test-only functions; never published.

The in-process bridge and provider catalogs (`virtual_catalog_bridge`,
`virtual_catalog_provider`) come from the sibling `duckdb-virtual-catalog` repo.
The only contract between the two repos is SQL: `n6k_table_permissions` /
`n6k_table_describe` read `bridge_table_permissions` / `provider_table_permissions`
for those catalog types, `n6k_server` finds host stream functions through
`provider_stream_functions()`, and providers declare primary keys in Arrow schema
metadata under `vcat.primary_keys`. The Python package (`bridge.py`, `provider.py`,
`rpc_stream.py`) drives their SQL API; local builds resolve through
`VIRTUAL_CATALOG_EXT_DIR`.

## n6k_client (Network Extension)

### Attach Flow

```
ATTACH 'n6k://host:port/prefix' AS db (TYPE n6k, token '...')
```

The storage extension parses the URL, resolves `n6k://` to `http://` and `n6ks://` to `https://`, and creates a catalog backed by HTTP endpoints.

### Schema and Table Discovery

Attached catalogs route everything through the `/ws` WebSocket session — `REQ CATALOG_LIST` for schemas and `REQ TABLES_LIST` for tables. `TABLES_LIST` rows carry `{schema, name, writable}`; the extension uses those fields directly to address later ops (no opaque `path` string is returned).

The WebSocket session is the only data path. There is no HTTP surface: the
extension's HTTP client is reduced to the three OAuth endpoints, and the browser
build makes no HTTP request at all.

### Reads

Table scans bind to DuckDB's Arrow table function and flow as `REQ SCAN` frames
on the WS session with `{schema, table, columns?, filters?}`.

### Writes

- **INSERT**: Arrow IPC serialized, sent as `REQ INSERT` on the WS session.
- **UPDATE/DELETE**: SQL forwarded via `REQ EXEC` on the WS session.
- **CREATE TABLE AS**: JSON schema sent as `OP_CREATE_TABLE` over the `/ws` session, then rows inserted.

### RPC

Dynamic table functions are bound at schema discovery time: `n6k_catalog_rpc`
(scalar args as JSON) and `n6k_catalog_rpc_table` (Arrow IPC input + optional JSON
args), both routed over the WebSocket session. The server resolves the name to a
DuckDB function registered in the served catalog and calls it.

### WASM

WebSocket sessions (`/ws`) are managed by the JavaScript layer for session
persistence and view re-registration on reconnect. The WASM build has no HTTP
path: every operation travels over that session.

## Build System

The extensions are registered with DuckDB's build system via a shared extension config. Each extension has its own CMake configuration; `src/common/n6k_common.cmake` declares the sources they share.

- n6k links against DuckDB's internal yyjson library and embeds nanoarrow for Arrow IPC handling

CI builds the extensions across 9 architectures: Linux (amd64, arm64, musl), macOS (amd64, arm64), Windows (amd64), and WASM (mvp, eh, threads).

## Dependencies

| Dependency | Used by | Purpose |
|------------|---------|---------|
| DuckDB | all | Core database engine and extension APIs |
| yyjson | n6k | JSON serialization for RPC arguments |
| nanoarrow | n6k | Arrow IPC serialization/deserialization |

## Python server framework

The Python side ships as one distribution (`n6k-duckdb`) containing two
importable packages, split by dependency weight:

- **`packages/python/src/n6k_protocol/`** — the wire format and its codec, with
  no FastAPI/Starlette/duckdb dependency: `protocol.py` (the constants, and the
  single source of truth the TypeScript and C++ mirrors are generated from),
  `engine.py` (`pack_frame` / `unpack_frame` / header validation / handshake
  helpers), and `filters.py`.
- **`packages/python/src/n6k_server/`** — adapters and consumers: `pump.py` (the
  byte pump), `server_fastapi/` (the FastAPI adapter, `register(app, prefix, *,
  connect=...)`), the `Provider` bridge (`provider.py`), and the test server
  (`test_server/`).

It is consumed by the in-repo test server
(`packages/python/src/n6k_server/test_server/`) and is the intended shared
surface for the sibling production servers (`data-service`, `server`).

### Shape

Python does not implement the protocol. The server is the `n6k_server` DuckDB
extension (C++); Python accepts the WebSocket, decides which catalogs to serve,
hands the socket to `CALL n6k_serve_fd(...)`, and shuttles bytes. It never
decodes a frame and cannot tell a SCAN from a PING.

- **Protocol layer** (`n6k_protocol/`): the frame codec and the constants. No
  FastAPI `app`, no auth, no duckdb imports. A Python client or a wire test uses
  it to speak the protocol directly; the server does not.
- **Pump** (`n6k_server/pump.py`): `socketpair()`, one end's integer fd passed to
  `n6k_serve_fd` in SQL, the other pumped against the WebSocket. In-process
  rather than a spawned `duckdb`, because the reactor's workers must open
  connections on *this* `DatabaseInstance` — that is what lets a table backed by
  Python callbacks (`provider.py`) be served at all.
- **Test server** (`n6k_server/test_server/`): owns a `FastAPI` app,
  Bearer-token auth, request counters, `/debug/*` endpoints, and seeded DuckDB
  fixtures. Calls `register()` to mount the WS route.

### Key design decisions

- **Consumer owns the app.** `register()` takes a FastAPI app and mounts the
  route via APIRouter. No module-global `FastAPI()` in the framework.
- **Native FastAPI routing.** Uses `APIRouter(prefix=path)` directly; no
  custom path matcher or registry dict.
- **The factory returns a database, not a handler.** `connect=` hands back a
  DuckDB connection, because the C++ reactor serves *catalogs*, not Python objects
  implementing ops. Everything that connection ATTACHed is served; its own default
  database is not.
- **Transport rejection in the factory.** The factory receives the `WebSocket`
  and can `raise WsReject(code=4401)` to close with any application-defined
  close code (RFC 6455 4xxx range) before a byte is pumped.
- **Auth is a DuckDB function, opted into by defining it.** A scalar named
  `n6k_authorize(token, catalog) -> BOOLEAN` on the connection is called by the
  reactor for every `FT_HELLO`; absent, handshake auth is off. A browser's
  credential arrives *inside* the frame — it cannot set a header on a WS upgrade —
  so checking it in Python would mean parsing frames. Credentials the *transport*
  carries (header, query param) are checked in `connect`, which is the only side
  that can see them.

### Catalog handling

On WS connect, the C++ driver names its catalog two ways: it sends
`?catalog=<name>` on the dial URL for a normal attach
(`ATTACH 'n6k://...' AS foobar` → `?catalog=foobar`), and it declares the same
name in its `FT_HELLO` handshake frame. The Python side reads `?catalog=` from
the URL — it must ATTACH before it can serve — and the reactor reads the HELLO
`catalog` to bind the session's namespace. SQL forwarded over
`n6k_catalog_exec` / `n6k_catalog_query` referencing the client-side catalog name
resolves on the server because the server's internal catalog has the same name.
