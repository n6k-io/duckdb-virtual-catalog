# n6k_server — native n6k protocol server

A DuckDB-loadable C++ extension that **serves** one or more DuckDB catalogs over
the n6k database protocol, with no Python. It is the server-side counterpart to
the client `n6k` extension (`src/n6k`), reusing the same wire protocol
(`src/common/include/n6k_protocol_generated.hpp`), the same SQL builder
(`src/common/n6k_sql_builder.cpp`) and the same permissions collector
(`src/common/n6k_permissions.cpp`) as the client.

This is the only implementation of the protocol. Python once had a second one; it
was deleted, and `packages/python` is now a byte pump that hands this extension a
socket (`n6k_server.pump`) without decoding a frame.

## Functions

Two entry points, the same protocol and the same reactor behind both. They differ
only in who owns the socket.

| | `n6k_serve_socket` | `n6k_serve_http` |
|---|---|---|
| Socket | dials `$N6K_DB_SOCKET` (`AF_UNIX`) | listens on `host:port` (TCP) |
| Framing | `[4-byte BE length][frame]` | WebSocket message boundaries |
| Connections | exactly one, then returns | many, concurrently |
| Catalogs | that one connection's | **shared** by every client |
| Ends when | the peer disconnects | interrupted (Ctrl-C) |

### `n6k_serve_socket`

```sql
CALL n6k_serve_socket('<catalog>' [, '<catalog>' ...]);
CALL n6k_serve_socket();   -- every attached non-system catalog
```

Serves the named **already-attached** catalogs; it does not `ATTACH` anything
(use `ATTACH … (READ_ONLY)` yourself for a read-only surface). The zero-arg form
discovers every attached catalog bar system/temp, frozen at bind time. It also
drops the startup **in-memory** database when anything else is attached — a
connection carries that one without asking, and serving it beside a real catalog
would turn a single-catalog session into a multiplexed one and change the wire. A
file-backed startup database is kept, as is an in-memory one that is all there is.
The call blocks until the peer disconnects, then returns one status row
`(connected BOOLEAN, requests_handled BIGINT, socket VARCHAR)`.

Bind rejects a NULL argument, a catalog that is not attached, and a repeated
catalog name.

```sh
N6K_DB_SOCKET=/run/n6k.sock duckdb mydata.duckdb -c "CALL n6k_serve_socket('mydata')"
```

### `n6k_serve_http`

```sql
CALL n6k_serve_http('<host>', <port> [, '<catalog>' ...]);
CALL n6k_serve_http('0.0.0.0', 7823);   -- every attached non-system catalog
```

Listens for WebSocket clients and serves n6k frames as binary messages — no
length prefix, since WebSocket already delimits them. Catalog arguments are
validated exactly as `n6k_serve_socket`'s are (same helper,
`src/common/serve_bind_common.cpp`); `N6K_DB_SOCKET` is not consulted. A port outside
1–65535 is a bind error.

Each accepted connection gets its own `ServeReactor` on its own thread, so the
call serves many clients at once and outlives any one of them. It blocks until
interrupted, then stops the listener and drains every reactor.

The declared return schema is `(connections BIGINT, requests_handled BIGINT,
url VARCHAR)`, but you will not see that row: the only exit is `Ctrl-C`, and
DuckDB aborts an interrupted statement rather than materialising its result. The
schema exists because a table function must declare one. `n6k_serve_socket`'s row
*is* reachable, because its normal exit is the peer disconnecting.

All clients share one `DatabaseInstance` and therefore one catalog set — the
point of this form, and its one behavioural difference from every other
deployment (`n6k_serve_fd`, `n6k ws` and `n6k_serve_socket` each give a client
its own `DatabaseInstance`). Concurrent writers meet DuckDB's MVCC; transaction
conflicts come back as `FT_ERR` with `"retriable":true`.

**The ATTACH alias must name a served catalog.** A native attach sends the alias
as the HELLO's `catalog` (`src/n6k_client/n6k_catalog_session_native.cpp:353-367`), so
attaching under a name the server does not serve gets a `HELLO_ERR`, and the
request that follows fails with `unknown session ns=N`. Either match the alias to
the catalog, or name the remote catalog explicitly:

```sql
ATTACH 'n6k://127.0.0.1:7823' AS whatever (TYPE n6k, CATALOG 'mydata');
```

## Sessions (`ns`)

One socket carries N catalog sessions, keyed by the protocol's `ns`. Full routing
table in [`docs/n6k-server.md`](../../docs/n6k-server.md#sessions-ns).

- **One catalog served** → wire-identical to the pre-multi-catalog server:
  default session from connect, unprompted `HELLO_ACK`, no `ns` on any frame.
- **Several served** → no unprompted ack; `HELLO {ns, catalog}` opens a session
  and gets `HELLO_ACK {ns}`; every reply is `ns`-stamped; each session owns its
  own `req_id` space; `CANCEL {ns, id:0}` closes a session; `PONG` is never
  stamped.
- A `REQ` with no `ns` against a multi-catalog serve is refused, never routed to
  a default — guessing would answer a `SCAN` from an arbitrary catalog.

Three deliberate divergences from `WsV2Router`: a duplicate `ns` is answered with
`HELLO_ERR` rather than silently dropped; a default (`ns`-less) session exists
when exactly one catalog is served; and `FT_READY` is never emitted, because
binding a session here is a lookup against already-attached catalogs rather than
a build.

## Transport

- **`N6K_DB_SOCKET`** names a Unix domain socket (`AF_UNIX`, `SOCK_STREAM`) that
  a peer is already listening on; `n6k_serve_socket` dials out, so it is the
  socket *client* but the protocol *server*.
- **`n6k_serve_http`** binds and listens at both layers; ixwebsocket does the
  HTTP `Upgrade` per client.
- **Framing:** over the Unix socket each frame is `[4-byte big-endian length]
  [n6k frame]`, the length counting the frame bytes (msgpack header + optional
  body) — the "transport owns length-framing" contract. WebSocket supplies
  message boundaries itself, so
  `n6k_serve_http` uses no prefix.
- **Handshake:** serving one catalog, the server speaks first, sending
  `FT_HELLO_ACK` (`{protocol_version, max_concurrent_reqs,
  default_batch_credits}`) on connect. Serving several, it waits for each
  `FT_HELLO` and answers `FT_HELLO_ACK {ns}`, using the frame's `catalog` to
  select among the served catalogs.

## Architecture

A naive read→handle loop deadlocks under backpressure (a streaming op blocks
waiting for an `FT_CREDIT` it cannot read while busy), so the reactor
(`serve_reactor.cpp`) uses three roles:

- a **reader** (the `CALL`'s thread) reads frames and routes them by `ns` —
  `FT_HELLO` opens a session, `FT_REQ` spawns a worker, `FT_CREDIT`/`FT_CANCEL`
  signal the matching worker, `FT_PING`→`FT_PONG`. It polls with a 250 ms timeout
  so `Ctrl-C` (`ClientContext::IsInterrupted`) ends the loop even on an idle
  peer. `sessions_` is reader-owned, which is why it carries no lock;
- a **writer thread** serializes all outbound frames onto the socket;
- one **worker thread per in-flight `(ns, req_id)`**, each with its own DuckDB
  `Connection` (connections aren't thread-safe), credit counter and cancel flag.
  A worker holds a *copy* of its `SessionRef`, so it never reads `sessions_` and
  survives its session being closed mid-request. Shutdown drains workers before
  the `CALL` returns.

## Op coverage

| Op | Status |
|---|---|
| `OP_CATALOG_LIST` | ✅ schemas of the session's catalog |
| `OP_TABLES_LIST` | ✅ tables/views + writable/editable + primary keys (optional schema filter) |
| `OP_TABLE_SCHEMA` | ✅ Arrow schema (`SELECT … LIMIT 0`) |
| `OP_QUERY` | ✅ raw passthrough SQL, streamed |
| `OP_SCAN` | ✅ projection + filter pushdown → `WHERE`, streamed |
| `OP_EXEC` | ✅ `{"rowcount":N}` |
| `OP_INSERT` | ✅ Arrow IPC body → `INSERT`, `{"rowcount":N}` |
| `OP_CREATE_TABLE` / `OP_ALTER_TABLE` | ✅ `{"ok":true}` |
| `OP_RPC_SCALAR` / `OP_RPC_TABLE` | ✅ calls a table function, macro or host stream function resolved in the served DuckDB |

Streaming ops honor credit-based backpressure (`default_batch_credits = 8`) and
`FT_CANCEL` (→ `RESP_END {"cancelled":true}`). Errors are typed `FT_ERR`
(`{exception_type, exception_message}`), with `"retriable":true` for transaction
conflicts.

## Limitations

- **No authentication.** `FT_HELLO`'s `token` is accepted and ignored. That is
  defensible for a Unix socket, whose filesystem permissions are the trust
  boundary; it is not for a TCP port. Bind `n6k_serve_http` to `127.0.0.1`
  unless something else authenticates in front of it. Plain `ws://` only —
  terminate TLS at a proxy.
- **`OP_QUERY` is not catalog-qualified.** It is raw passthrough SQL, so a query
  can reach another attached catalog (same as the Python server). Every other op
  qualifies as `"catalog"."schema"."table"` and stays inside its session.
  Multi-catalog serving makes this far more visible — use a `READ_ONLY` or
  otherwise restricted connection if you need hard isolation.
- **RPC has no registry, by design.** A name resolves through ordinary DuckDB resolution:
  a table macro, table function or host stream function in the served catalog first, then the
  bare name (where an extension's table functions live). So what is callable is whatever the
  host put in the database — there is no separate allow-list, and `OP_QUERY` is unfiltered
  passthrough SQL
  anyway, so one would contain nothing that is not already reachable. Restrict the serving
  connection (`enable_external_access`, `lock_configuration`, a `READ_ONLY` attach) if that
  matters.
- **`OP_RPC_TABLE` picks its calling convention from the target.** A function declaring a
  `TABLE` parameter is handed the pushed rows as a sub-select; anything else — notably a SQL
  macro, which cannot declare one — receives the name of a temp view and opens it with
  `query_table()`.

## Testing

Integration tests: `integration_tests/test_reactor_wire.py` (drives the in-process
`n6k_serve_fd` mount over WebSocket with `pyarrow`) and
`integration_tests/test_n6k_serve_http.py` (drives `n6k_serve_http`, which serves
WebSocket itself and needs no peer).

⚠️ `n6k_serve_socket` has no end-to-end coverage. It dials *out* to a Unix socket,
so exercising it needs a listening peer; the Python pump that used to provide one
(`uds_ws_bridge.py`) and the test that drove it were removed. Bind-time behaviour
is still covered below.

Binder-contract tests: `test/sql/n6k_server.test`. The serving path can't go in
sqllogictest — its scan blocks until disconnect — so only bind-time errors are
covered there. There is deliberately no `n6k_serve_socket()` case, which would
clear every bind check and hang if the shell exports `N6K_DB_SOCKET`.

```sh
uv run pytest integration_tests/test_reactor_wire.py -v
./build/release/test/unittest test/sql/n6k_server.test
```

## Build

Built alongside `n6k_client` via the root `extension_config.cmake`.
Links `duckdb_yyjson` and compiles the vendored nanoarrow sources for Arrow IPC.

On native builds it also compiles the vendored ixwebsocket objects for
`n6k_serve_http`, which forces an OpenSSL link even though this server speaks
only plain `ws://` — the vendored library is built with
`IXWEBSOCKET_USE_TLS`/`IXWEBSOCKET_USE_OPEN_SSL`, so its objects reference
OpenSSL unconditionally. `ixwebsocket` is an OBJECT library defined by the root
`CMakeLists.txt` only when `NOT WASM_LOADABLE_EXTENSIONS`; this extension adds it
only `if(NOT TARGET ixwebsocket)`, so extension ordering does not matter. A wasm
build defines neither `N6K_SERVER_WITH_WS` nor `n6k_serve_http` and keeps only
the Unix-socket form.
