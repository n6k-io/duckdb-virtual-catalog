# n6k Server Extension (`n6k_server`)

A DuckDB-loadable C++ extension that **serves** a DuckDB catalog over the n6k
protocol — the server-side counterpart to the client `n6k` extension, with no
Python. It reuses the same wire protocol and Arrow IPC framing the client
speaks; this doc covers how to start serving, the transport, and which protocol
ops it answers. For the **on-the-wire** frame format, see
[`n6k-network-protocol.md`](n6k-network-protocol.md). For the **client / SQL**
side — `ATTACH`, auth, and the function registry — see
[`n6k-network.md`](n6k-network.md).

This is the only implementation of the protocol — Python once had a second one,
and it was deleted. `packages/python` now hands this extension a socket and pumps
bytes (`n6k_server.pump`) without decoding a frame.

The extension exposes three `CALL`-able entry points, all of which block the
calling thread and serve one or more already-attached catalogs.

- **`n6k_serve_socket`** dials a Unix domain socket someone else is listening on
  and serves exactly one connection, then returns.
- **`n6k_serve_fd(<fd>, ...)`** is the same, on a socket the *caller* already
  owns and passes by file descriptor. This is what an embedding host uses: it
  accepts the WebSocket itself, makes a `socketpair()`, and hands over one end.
- **`n6k_serve_http`** listens on a TCP host/port and serves many WebSocket
  clients concurrently until interrupted.

This is the same split as `n6k database` in the sibling `server` repo, which
branches on `N6K_DB_SOCKET` (`n6k_app/cli.py`) between a socket worker
(`n6k_app/socket_app.py`) and a uvicorn listener.

Both run the same `ServeReactor`; only the `ServeTransport` behind it differs
(`src/n6k_server/include/serve_transport.hpp`).

## `n6k_serve_socket`

```sql
CALL n6k_serve_socket('<catalog>' [, '<catalog>' ...]);
CALL n6k_serve_socket();   -- every attached non-system catalog
```

`n6k_serve_socket` serves the named **already-attached** catalogs. Getting them open is
the caller's job — `n6k_serve_socket` does not `ATTACH` anything. The zero-argument form
discovers every attached catalog except the system and temp ones; the set is
frozen at bind time, so a later `ATTACH` is **not** picked up by a running serve.

One further exclusion: the **startup in-memory database**, when anything else is
attached. A connection opened with no path carries one it never asked for (the
DuckDB Python API names it `memory`), and serving it beside a deliberately attached
catalog would silently make the session multiplexed — suppressing the unprompted
`HELLO_ACK` and requiring the client to name a catalog in its handshake. A
file-backed startup database was asked for explicitly and is always served; and if
the in-memory default is the *only* catalog, it is served too. Naming catalogs
explicitly overrides all of this.
The call **blocks**, serving requests until the peer disconnects (or `Ctrl-C`),
then returns one status row:

| Column | Type | Meaning |
|--------|------|---------|
| `connected` | BOOLEAN | a peer connected and was served |
| `requests_handled` | BIGINT | count of `FT_REQ` frames processed |
| `socket` | VARCHAR | the Unix socket path that was served |

```sh
# Serve the file the CLI opened, over the socket named by N6K_DB_SOCKET:
N6K_DB_SOCKET=/run/n6k.sock duckdb mydata.duckdb -c "CALL n6k_serve_socket('mydata')"
```

Several catalogs share the one socket, each addressed by its own session:

```sql
ATTACH 'sales.duckdb' AS sales (READ_ONLY);
ATTACH 'ops.duckdb'   AS ops;
CALL n6k_serve_socket('sales', 'ops');
```

For a read-only surface, attach the catalog read-only yourself before serving.

Every catalog name must resolve to an attached catalog, no name may repeat, and
`N6K_DB_SOCKET` must be set — all validated at bind time, raising a
`BinderException` otherwise (`CALL n6k_serve_socket(NULL)`, `CALL n6k_serve_socket('nope')`,
and `CALL n6k_serve_socket('a', 'a')` all fail before serving).

## `n6k_serve_http`

```sql
CALL n6k_serve_http('<host>', <port> [, '<catalog>' ...]);
CALL n6k_serve_http('0.0.0.0', 7823);   -- every attached non-system catalog
```

Binds `host:port` and accepts WebSocket clients; each n6k frame is one binary
message, with **no** length prefix (WebSocket already delimits messages). Catalog
arguments are validated by the same helper as `n6k_serve_socket`
(`src/common/serve_bind_common.cpp`), so the NULL / not-attached / duplicate
errors are identical. `N6K_DB_SOCKET` is not read. A port outside 1–65535 is a
bind error.

Every accepted connection gets its own `ServeReactor` on its own thread, so this
form serves many clients at once and survives any one of them disconnecting. It
blocks until interrupted (`Ctrl-C`), then stops the listener and drains every
reactor.

Its declared return schema is:

| Column | Type | Meaning |
|--------|------|---------|
| `connections` | BIGINT | WebSocket clients accepted over the run |
| `requests_handled` | BIGINT | count of `FT_REQ` frames processed, all clients |
| `url` | VARCHAR | the `ws://host:port` that was served |

**That row is not observable in practice.** The serve loop's only exit is the
interrupt, and DuckDB aborts an interrupted statement instead of materialising
its result — the same thing happens to any long query you `Ctrl-C`. The schema is
declared because a table function must declare one. Contrast `n6k_serve_socket`,
whose row *is* returned, because its normal termination is the peer
disconnecting rather than an interrupt.

```sh
duckdb mydata.duckdb -c "CALL n6k_serve_http('127.0.0.1', 7823, 'mydata')"
# from another duckdb:
#   ATTACH 'n6k://127.0.0.1:7823' AS x (TYPE n6k);
```

Two things follow from all clients sharing one `DatabaseInstance`:

- **One catalog set, shared.** Every other deployment gives a client its own
  `DatabaseInstance` — `n6k_serve_socket` by construction, `n6k ws` by spawning
  one worker per attach, and a `n6k_serve_fd` host by opening a fresh
  `duckdb.connect()` per WebSocket before handing over its fd. Here concurrent writers
  meet DuckDB's MVCC; transaction conflicts come back as `FT_ERR` with
  `"retriable":true`.
- **No authentication.** `auth_function` is reachable only from `n6k_serve_fd`, so
  this form ignores `FT_HELLO`'s `token`. A Unix socket has file permissions as its
  trust boundary; a TCP port has no equivalent. Bind `127.0.0.1` unless something in
  front is doing authn. There is also no TLS — plain `ws://` only, terminate at a proxy.

## Sessions (`ns`)

The wire carries a session key, `ns`, on every session-scoped frame, so one
socket carries many catalog sessions (`n6k-network-protocol.md`). A client opens
a session with `FT_HELLO {ns, catalog}`; every response for it is stamped with
that `ns`, and each session owns its own `req_id` space — two sessions may both
have `req_id` 1 in flight.

**Serving one catalog** is wire-identical to the pre-multi-catalog server: a
default session exists from connect, the server speaks first with an unprompted
`HELLO_ACK`, and no frame carries `ns`. A client that never sends `HELLO` works
unchanged.

**Serving several** switches on multiplexing:

| Client frame | Server response |
|---|---|
| `HELLO {ns, catalog}` naming a served catalog | `HELLO_ACK {ns}` — session open |
| `HELLO` with no `catalog` | `HELLO_ERR` — the catalog must be named |
| `HELLO` naming an unserved catalog | `HELLO_ERR {ns}`, listing what *is* served |
| `HELLO` for an `ns` already open | `HELLO_ERR {ns}` — duplicate |
| `REQ` for an unknown `ns` | `FT_ERR {ns}` — unknown session |
| `REQ` with no `ns` | `FT_ERR` — send `HELLO` first |
| `CANCEL {ns, id: 0}` | closes that session (what a client sends on `DETACH`) |
| `PING` | `PONG`, never `ns`-stamped — keepalive is connection-scoped |

A multiplexed serve sends **no unprompted `HELLO_ACK`**: a client must name the
catalog it wants. This matches `WsV2Router`, and it matters twice over — acking
on connect would mark a client ready before its session exists, and would make a
later `HELLO_ERR` invisible (the client only surfaces one if it has not already
seen an ack).

A `REQ` with no `ns` against a multi-catalog serve is refused rather than routed
to some default. Guessing would answer a `SCAN` from an arbitrary catalog:
wrong data, no error.

### Divergences from `WsV2Router`

- A **duplicate `ns`** gets a `HELLO_ERR`; the Python router logs and drops it.
  This server has no log channel a client can see, and a silent drop leaves the
  client's connect waiter hanging until it times out.
- A **default (`ns`-less) session** exists when exactly one catalog is served.
  `WsV2Router` has no equivalent — it always requires an integer `ns`. This is
  what keeps every pre-multi-catalog client working.
- **No `FT_READY`, ever.** Binding a session is a lookup against catalogs that
  are already attached, so there is no deferred build: the per-`ns` `HELLO_ACK`
  never sets `session_pending`, which the protocol defines as the "fast build"
  path.

## Transport

- **`N6K_DB_SOCKET`** (env) names a Unix domain socket (`AF_UNIX`,
  `SOCK_STREAM`), read once at bind time. A peer is already listening;
  `n6k_serve_socket` is the socket **client** — it dials out. At the *protocol* layer it
  is the **server**.
- **`n6k_serve_http`** is the **server** at both layers: it binds and listens, and
  ixwebsocket performs the HTTP `Upgrade` handshake per client.
- **Framing.** Over the Unix socket each n6k frame is length-delimited as
  `[4-byte big-endian length][n6k frame]`, where the length counts the frame
  bytes (msgpack header + optional Arrow body). A WebSocket supplies message
  boundaries for free; a raw stream socket needs this prefix. This is the same
  "transport owns length-framing" convention used by the
  [native `wsFd` attach path](n6k-network.md#attaching-an-existing-socket-fd-native)
  (`src/n6k_client/ws_transport_fd.cpp`), so a pump can shuttle frames between this
  socket and a real WebSocket unchanged — which is what `n6k_server.pump` does for
  `n6k_serve_fd`. Over `n6k_serve_http` there is no prefix: one binary message is
  one frame.
- **Handshake.** Serving one catalog, the server speaks first: it sends
  `HELLO_ACK` (`{protocol_version, max_concurrent_reqs, default_batch_credits}`)
  immediately on connect, then serves requests. Serving several, it waits for
  each client `HELLO` and answers it with a `HELLO_ACK {ns}` — see
  [Sessions](#sessions-ns). A `HELLO`'s `catalog` field selects among the served
  catalogs; its `token` is checked when the serve call set an `auth_function`, and
  ignored otherwise (see [Caveats](#caveats)).

## Architecture

A naive read→handle loop would deadlock under backpressure (a streaming op
blocks waiting for an `FT_CREDIT` it can't read while busy). The reactor uses
three roles, mirroring the asyncio model in the Python engine:

- a **reader** (the `CALL`'s thread) reads length-prefixed frames and routes
  them by `ns` — `FT_HELLO` opens a session; `FT_REQ` spawns a worker;
  `FT_CREDIT`/`FT_CANCEL` signal the matching worker; `FT_PING`→`FT_PONG`. It
  polls with a 250 ms timeout so `Ctrl-C` (`ClientContext::IsInterrupted`) ends
  the serve loop even against an idle peer. The session table is **reader-owned**:
  nothing else reads or writes it, which is why it needs no lock;
- a **writer thread** serializes all outbound frames onto the socket so frames
  never interleave;
- one **worker thread per in-flight `(ns, req_id)`**, each with its own DuckDB
  `Connection` (connections aren't thread-safe), its own credit counter, and a
  cancel flag. A worker gets a *copy* of its session's `{ns, catalog}`, so it
  never reaches back into the reader's table and survives its session being
  closed mid-request. Shutdown drains workers (each closes its `Connection`)
  before the `CALL` returns.

## Op coverage

The extension answers the catalog-scoped [REQ ops](n6k-network-protocol.md#req-ops):

| Op | Status |
|---|---|
| `CATALOG_LIST` | ✅ schemas of the session's catalog (system schemas excluded) |
| `TABLES_LIST` | ✅ tables/views + `writable`/`editable` + primary keys (optional schema filter) |
| `TABLE_SCHEMA` | ✅ Arrow schema (`SELECT … LIMIT 0`) |
| `QUERY` | ✅ raw passthrough SQL, streamed |
| `SCAN` | ✅ projection + filter pushdown → `WHERE`, streamed |
| `EXEC` | ✅ → `{"rowcount":N}` |
| `INSERT` | ✅ Arrow IPC body → `INSERT`, → `{"rowcount":N}` |
| `CREATE_TABLE` / `ALTER_TABLE` | ✅ → `{"ok":true}` (ALTER: add/drop/rename column) |
| `RPC_SCALAR` / `RPC_TABLE` | ✅ calls a host registration, or a table function or macro resolved in the served DuckDB |

Streaming ops (`SCAN`, `QUERY`) honor the credit-based backpressure
(`default_batch_credits = 8`): each worker waits for a `CREDIT` refill before
sending the next `RESP_CHUNK`, and `CANCEL` ends the stream with
`RESP_END {"cancelled":true}`. Errors are typed `ERR` frames
(`{exception_type, exception_message}`), with `"retriable":true` for transaction
conflicts — the same [error reporting](n6k-network-protocol.md#error-reporting)
contract the client expects.

## Host-backed RPC

An RPC that resolves through the catalog can only be what SQL can express, and
no SQL construct produces a relation that never ends. A subscription — a
generator in the host process that yields a batch every so often and may run
forever — therefore has no catalog form at all.

So the host gives it one: a table function whose rows come from the generator.
DuckDB's Python API cannot register a table function (`create_function` is
scalar-only), so the host registers three scalar UDFs and asks the extension to
build the catalog entry over them:

```sql
SELECT provider_create_stream_function(catalog, schema, name, open_udf, next_udf, close_udf);
SELECT provider_drop_stream_function(catalog, schema, name);
```

Both come from **`virtual_catalog_provider`** (sibling `duckdb-virtual-catalog`
repo), not `n6k_server`; the server only recognises such an entry — by finding it
in `provider_stream_functions()` — to forward the generator's Arrow bytes untouched.
`register_rpc_streams()` loads `virtual_catalog_provider` for you.

That entry is an ordinary table function, so it needs nothing passed to the
serving call and it is callable as plain SQL on the host's own connection:

```sql
SELECT * FROM db.main.random_walk(100);   -- no client, no reactor
```

Because a catalog entry lives in the database, every connection on that
`DatabaseInstance` sees it. The UDF *names* carry a per-connection suffix so two
connections sharing an instance do not collide when registering (`create_function`
raises on a duplicate name), but nothing looks that suffix up.

For a stream function the worker drives the UDFs instead of building SQL:

| UDF | Signature | Meaning |
|---|---|---|
| open | `(handle, function, args_json, input_ipc BLOB) -> BLOB` | starts the call; returns the Arrow IPC **schema message** sent as `RESP_SCHEMA` |
| next | `(handle) -> BLOB` | one Arrow IPC record batch, or `NULL` once the source is exhausted |
| close | `(handle) -> BLOB` | runs the source's teardown; returns the trailing end-of-stream bytes |

The UDFs speak Arrow IPC because the host must declare its result schema up
front anyway — the wire sends `RESP_SCHEMA` before the first batch — and once it
has, the bytes cross the boundary once instead of being decoded into DuckDB
vectors only to be re-encoded on the way out. `RPC_TABLE`'s pushed rows reach
`open` as the raw request body, so they are never staged through a temp view.

Each batch is pulled *before* its credit is acquired, so a source that paces
itself in wall-clock time keeps its cadence rather than starting its next
interval only once the client's credit arrives. `close` runs on every exit —
end of stream, `CANCEL`, or an error mid-stream — which is what lets a
subscription unsubscribe.

`packages/python/src/n6k_server/rpc_stream.py` is the Python half:
`register_rpc_streams(con, catalog=…, registry={name: (fn, schema)})`, where
`fn` is either a plain callable returning one `pa.Table` or a `StreamingRpc`
wrapping an async iterator. It creates the UDFs and one stream function per
name, in an explicit transaction — the entry is created from inside a scalar
function, and DuckDB plans a `SELECT` as read-only, so under auto-commit the new
entry would be discarded when the statement ends. Nothing needs tracking
afterwards; the returned handle is only for un-registering without closing the
connection.

### Caveats

- **RPC** (`RPC_SCALAR`/`RPC_TABLE`) resolves in two steps: a table macro, table
  function or stream function in the served catalog, then the bare name, where an
  extension's table functions live. `TABLE_MACRO_ENTRY` and `TABLE_FUNCTION_ENTRY`
  share one catalog set, so a single lookup finds all three and the entry itself
  says which it is. What is callable is therefore whatever the host put in the
  database. A catalog function
  declaring a `TABLE` parameter receives `RPC_TABLE`'s pushed rows as a sub-select;
  anything else, including a SQL macro (which cannot declare one), receives the
  name of a temp view to open with `query_table()`.
- **Auth** is opt-in and only on `n6k_serve_fd`: its `auth_function` names a scalar
  `(token, catalog) -> BOOLEAN` called for every `FT_HELLO`, defaulting to
  `n6k_authorize` when that is defined. `n6k_serve_socket` and `n6k_serve_http` have
  no way to set it and are unauthenticated — the local socket is trusted.
- **`QUERY`** is raw passthrough SQL and is *not* catalog-qualified, so a query
  could reach another attached catalog (same as the Python server). Every other
  op qualifies as `"catalog"."schema"."table"` and so stays inside its session,
  but `QUERY` does not — serving several catalogs makes that far more visible,
  since a `QUERY` on one session can read another's data. `EXEC` is the same. Note
  this outranks `auth_function`: its `catalog` argument gates which session opens,
  not what that session's SQL can reach. Use a `READ_ONLY` / restricted connection
  if you need hard isolation.
- The `writable`/`editable` flags from `TABLES_LIST` mark native tables writable
  regardless of a `READ_ONLY` attach (the Python fallback additionally checks
  catalog read-only state).

## Testing

- **Integration:** `integration_tests/test_reactor_wire.py` drives the in-process
  `n6k_serve_fd` mount over WebSocket with `pyarrow` — every op, plus the mux
  cases (interleaved streams reusing `req_id` 1 on both sessions, per-session
  catalog listing, every `HELLO_ERR` case, unknown/absent `ns`, session close).
  `integration_tests/test_n6k_serve_http.py` does the same against
  `n6k_serve_http`, which serves WebSocket itself and needs no peer.
- ⚠️ **`n6k_serve_socket` is not covered end to end.** It dials *out* to a Unix
  socket, so driving it needs a listening peer; the Python pump that provided one
  (`uds_ws_bridge.py`) and the test that spawned it were removed. Only the bind
  contract below still covers it.
- **Binder contract:** `test/sql/n6k_server.test` covers bind-time errors only —
  the serving path can't go in sqllogictest because the scan blocks until the
  peer disconnects. Note there is deliberately no `CALL n6k_serve_socket()` case there:
  with a catalog attached it clears every bind check, so a developer shell that
  exports `N6K_DB_SOCKET` would reach the scan and hang the suite.

```sh
uv run pytest integration_tests/test_n6k_serve.py -v
./build/release/test/unittest test/sql/n6k_server.test
```

## Build

Built alongside `n6k_client` via the root `extension_config.cmake`.
The extension links `duckdb_yyjson` and compiles the vendored nanoarrow sources
(for Arrow IPC encode/decode). Native builds additionally compile in the vendored
ixwebsocket objects for `n6k_serve_http`, which forces an OpenSSL link even though
the listener only speaks plain `ws://` — the vendored library is built with
`IXWEBSOCKET_USE_TLS`/`IXWEBSOCKET_USE_OPEN_SSL`. A wasm build defines neither
`N6K_SERVER_WITH_WS` nor `n6k_serve_http` and keeps only the Unix-socket form.
Source lives in `src/n6k_server/` (`README.md` there is the implementation
reference).

## See also

- [`n6k-network-protocol.md`](n6k-network-protocol.md) — the on-the-wire frame
  format, ops, and lifecycle this extension implements.
- [`n6k-conformance.md`](n6k-conformance.md) — the shared test harness. This
  extension is the `native` target there, driven through the `/native` mount.
- [`n6k-network.md`](n6k-network.md) — the client / SQL side: `ATTACH`, auth,
  `wsId`/`wsFd`, and the function registry.
</content>
