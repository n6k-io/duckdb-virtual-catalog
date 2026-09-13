# n6k Network Protocol

The **on-the-wire** protocol for `TYPE n6k`: the frame format, ops, and lifecycle
a client and server exchange over the `/ws` WebSocket session. This is the spec
for building a compatible server or an alternate client. For the **SQL / host-integration** side — how to `ATTACH`,
authenticate, ride an existing socket, and which functions exist — see
[`n6k-network.md`](n6k-network.md).

Attached catalogs run over a single WebSocket per ATTACH. Every operation —
schema discovery, scans, inserts, DDL, exec, query, and RPC — is one frame
multiplexed on that socket. There is no HTTP data path.

## Wire format

- Tabular responses: Arrow IPC (schema + record-batch messages)
- Frame headers: one self-delimiting **msgpack map** per frame (see below)
- A frame's optional body is **raw Arrow IPC bytes**, appended after the header

## Constants

| Constant | Value | Notes |
|----------|-------|-------|
| `protocol_version` | 6 | Hard compatibility gate — a client whose version differs from the server's closes the connection rather than negotiating. |
| `default_batch_credits` | 8 | Starting stream budget, advertised per connection. |
| `max_concurrent_reqs` | 64 | Advertised only. The reference server does **not** enforce it; it is a client-side contract. |

These, the frame-type numbers, and the op numbers are all generated from the SSOT
(`packages/python/src/n6k_protocol/protocol.py`) into the TypeScript, C++ and
JSON-Schema mirrors. `make protocol-check` fails if a mirror drifts, so treat the
SSOT — not this document — as authoritative if they ever disagree.

## WebSocket /ws — frame format

Each WebSocket **binary** message is exactly one frame: a **msgpack header map**
optionally followed by a **raw body**. There is no length prefix — a single
msgpack map is self-delimiting, so the decoder reads the header map and
everything after it is the body (zero-copy: the body is sliced from the same
buffer, never re-wrapped through msgpack `bin`).

```
 +---------------------------+--------------------------+
 | msgpack header map        | raw body (optional)      |
 | {t, id, op?, ...meta}     | Arrow IPC bytes or empty |
 +---------------------------+--------------------------+
```

| Key | Type | Notes |
|-----|------|-------|
| `t` | int | frame type (table below); present on every frame |
| `id` | int | the `req_id`: client-assigned, monotonic per socket. Present on request/response frames; absent on connection-scoped frames (HELLO*, READY, PUSH) |
| `op` | int | REQ/PUSH discriminator (op tables below) |
| …meta | — | per-frame fields (see tables) |

The old fixed `flags` byte is **gone**: `end_of_stream` is implied by the
`RESP_END`/`ERR` frame types, and an Arrow body is marked by `arrow: true` on
`RESP_CHUNK` (`RESP_SCHEMA` always carries one). Header keys are strictly
JSON-typed (no msgpack `bin`/`ext`) so each decoded header validates directly.

The exact header shape of every frame is generated from the protocol SSOT
(`packages/python/src/n6k_protocol/protocol.py`) into
`packages/python/src/n6k_protocol/schema.json` — a JSON-Schema `oneOf`
discriminated on `t`/`op`. The server validates every decoded/encoded header
against it.

## Frame types

`id` is carried in the header where noted in the §WebSocket frame format table;
the columns below list only the **other** header keys plus the body.

`ns` (integer, optional) is the **session routing key** and may appear on every
frame below except `PING`/`PONG`, which are connection-scoped. It exists so one
socket can carry several catalog sessions: the client stamps a distinct `ns` per
session on its `HELLO`, and the server stamps every session-scoped response with
the same `ns` so the client can demultiplex. **Absent means the sole/default
session** — which is exactly how a single-session connection and every
pre-multiplexing client behave, so `ns` is purely additive. Because it is
omitted at 0, a single-session connection is byte-identical with or without
multiplexing support on either end. See
[Multiplexed sockets](#multiplexed-sockets).

| Value | Name | Direction | Header keys + body |
|-------|------|-----------|--------------------|
| 1 | REQ | C→S | `{op, …op-args}` (see REQ ops); INSERT/RPC_TABLE append a raw Arrow body |
| 2 | RESP_SCHEMA | S→C | `{}`; body = Arrow-IPC schema message |
| 3 | RESP_CHUNK | S→C | Arrow chunk `{arrow:true}` + Arrow body; **or** list chunk `{schemas:[..]}` / `{tables:[..]}` (no body) |
| 4 | RESP_END | S→C | `{rowcount?|cancelled?|ok?}`; terminal frame (implies end-of-stream) |
| 5 | ERR | S→C | `{exception_type, exception_message, retriable?}`; terminal frame |
| 6 | CANCEL | C→S | `{}` (just `id`); early termination of `id` |
| 7 | CREDIT | C→S | `{n}` — refill count for `id` |
| 8 | PUSH | S→C | `{op, …}`; server-initiated, no `id` (see PUSH ops) |
| 9 | PING | either | `{id?}` |
| 10 | PONG | either | `{id?}` |
| 11 | HELLO | C→S | `{token?, catalog?}` — handshake. `token` carries auth where the client can't set a WS upgrade header (browsers). `catalog` selects the server catalog when the dial URL omits `?catalog=` — e.g. a host that owns the socket (`wsFd`/`wsId`) dials a bare `/ws`. Both fields optional. On a multiplexed socket a HELLO **opens** the session named by its `ns`, and `catalog` is required. |
| 12 | HELLO_ACK | S→C | `{protocol_version, max_concurrent_reqs, default_batch_credits}`, plus optional `session_pending: true` when a slow session build is deferred (a `READY` follows once it completes), plus optional `capabilities: [..]` (see Capabilities). A deferred ack omits `capabilities` — the handler does not exist yet — and the `READY` carries them instead. |
| 13 | HELLO_ERR | S→C | `{exception_type, exception_message, retriable?}`; connection-scoped (no `id`). Sent **in lieu of** HELLO_ACK when auth or the session factory fails — i.e. a connection-scoped error before any request exists. The client's connect waiter surfaces it as the ATTACH error instead of a bare "socket closed". The server still closes the socket after sending it; the WS close is the fallback when even HELLO_ERR can't be sent. |
| 14 | READY | S→C | `{capabilities?: [..]}`; no `id`. Session/catalog is now usable. Sent **after** a `HELLO_ACK` that carried `session_pending: true`, once the deferred session build completes. The client gates its first request on this frame. A build that fails sends `HELLO_ERR` instead (then closes). Fast (non-deferred) sessions send a ready `HELLO_ACK` with no `session_pending` and never emit `READY`. |

## REQ ops

A REQ header is `{t:1, id, op, …op-args}`. INSERT and RPC_TABLE append a raw
Arrow-IPC body after the header; all other ops are header-only.

| Op | Name | Header op-args | Body | Response stream |
|----|------|----------------|------|-----------------|
| 1 | CATALOG_LIST | — | — | one `RESP_CHUNK` (`{schemas:[..]}`) + `RESP_END` |
| 2 | TABLES_LIST | `{schema?}` | — | one `RESP_CHUNK` (`{tables:[..]}`) + `RESP_END` |
| 3 | TABLE_SCHEMA | `{schema, table}` | — | one `RESP_SCHEMA` (Arrow) + `RESP_END` |
| 4 | SCAN | `{schema, table, columns?, filters?, _batch_rows?}` | — | `RESP_SCHEMA` + N × `RESP_CHUNK` + `RESP_END` |
| 5 | INSERT | `{schema, table}` | Arrow-IPC stream | `RESP_END` (`{rowcount}`) |
| 6 | EXEC | `{sql}` | — | `RESP_END` (`{rowcount}`) |
| 7 | QUERY | `{sql, _batch_rows?}` | — | `RESP_SCHEMA` + N × `RESP_CHUNK` + `RESP_END` |
| 8 | RPC_SCALAR | `{function, args?}` | — | `RESP_SCHEMA` + N × `RESP_CHUNK` + `RESP_END` |
| 9 | RPC_TABLE | `{function, args?}` | Arrow-IPC stream (optional) | `RESP_SCHEMA` + N × `RESP_CHUNK` + `RESP_END` |
| 10 | CREATE_TABLE | `{schema, name, columns}` | — | `RESP_END` (`{ok: true}`) |
| 11 | ALTER_TABLE | `{schema, table, kind, details?}` | — | `RESP_END` (`{ok: true}`) |
| 13 | AGGREGATE | `{schema, table, columns?, filters?, group_by, aggregates, _batch_rows?}` | — | `RESP_SCHEMA` + N × `RESP_CHUNK` + `RESP_END` |

Ops that stream Arrow send a `RESP_SCHEMA` (no header keys beyond `id`/`ns`,
Arrow body) followed by `RESP_CHUNK{arrow:true}` frames (Arrow body). The list
ops (CATALOG_LIST, TABLES_LIST) instead send a single `RESP_CHUNK` whose header
carries the result array (`schemas`/`tables`) and no body.

### Arrow message framing

The bodies of `RESP_SCHEMA` and the `RESP_CHUNK{arrow:true}` frames that follow
it form **one continuous Arrow IPC stream**, split across frames. Concatenating
them in order yields a valid IPC stream: schema message, then one record-batch
message per chunk, then the end-of-stream marker.

Where the boundaries fall is **not** fixed, and a client must not assume it:

- A server that flushes eagerly puts the schema message in `RESP_SCHEMA` and one
  record batch in each `RESP_CHUNK`.
- A server whose Arrow writer flushes lazily — which is what pyarrow does, and
  therefore what the reference server does — sends an **empty** `RESP_SCHEMA`
  body and prepends the schema message to the first `RESP_CHUNK`, so that one
  frame carries two complete IPC messages.

Both are conformant. A client must therefore treat each frame payload as a
*stream of IPC messages*: loop decode-header → decode-message, advancing by
`header_size + body_size`, until the payload is consumed, and tolerate an empty
payload as a no-op. `N6kArrowFrameDecoder`
(`src/common/include/n6k_arrow_frame_decoder.hpp`) is the reference for this.

The trailing end-of-stream marker is sent as a final `RESP_CHUNK{arrow:true}`
when the writer emits one. It does **not** consume a credit, and it is sent even
when the stream ends via `CANCEL` — before the terminal `RESP_END`.

Streams the server *reads* may contain dictionary-encoded fields and dictionary
batches (replacement and delta), e.g. a pandas Categorical column from a
provider; they decode to their value type (a `dictionary<values=string>` column
becomes `VARCHAR`). Streams the server *writes* never contain dictionary
encoding: `ENUM` columns travel as `VARCHAR`, because nanoarrow's IPC writer
cannot emit dictionary batches. Clients cannot reconstruct the enum-ness.

`_batch_rows` asks the server for a specific number of rows per emitted
`RecordBatch`. It is a **testing affordance**, not a tuning knob: the underscore
marks it as such, and it exists so a suite can force a small scan to arrive as
several chunks and exercise the credit path. The reference server defaults it to
2 for exactly that reason. Production clients omit it and let the server pick a
batch size per [Chunk sizing](#chunk-sizing); a server is free to treat it as
advisory, but honouring it is what makes the flow-control tests meaningful.

`filters` is a JSON array of clauses, AND-joined at the top level. A clause is
either a flat `[col, op, value]` tuple:
`[["age", ">=", 18], ["name", "in", ["a", "b"]]]` — structurally identical
to pyarrow's tuple-form filter argument (`pq.read_table(filters=...)`) — or a
nested group `["or", [clause, ...]]` / `["and", [clause, ...]]`:
`[["or", [["age", "<", 18], ["age", ">", 65]]]]`.
Operators: `=`, `!=`, `<`, `<=`, `>`, `>=`, `in`, `is_null`, `is_not_null`.
A flat clause always has exactly 3 elements — for `is_null` / `is_not_null` the
value slot is JSON `null` rather than omitted, so clause arity distinguishes a
flat clause from a group unambiguously.

A filter set that reaches the server is **complete**: the client does not
re-apply pushed filters locally, so a server must apply every clause it
receives. Correspondingly, the client refuses to send a filter set it cannot
render exactly rather than sending a partial one — a partial set would return
rows the query excluded. Nested groups exist for that reason: flattening a
disjunction into the AND-joined top level changes `a > 5 OR a < 2` into
`a > 5 AND a < 2`.

Only the network dialect carries nested groups. The in-process provider bridge
emits flat clauses only, preserving the pyarrow tuple shape that
`n6k_protocol.filters.Filters` documents; an OR filter is refused there instead.

`ALTER_TABLE` `kind` is one of `"add_column"` (details: `{name, type}`),
`"drop_column"` (details: `{name}`), `"rename_column"` (details:
`{old_name, new_name}`). Unsupported alter kinds (column type change,
table rename, defaults, constraints) are rejected client-side with
`BinderException` before the RPC is sent.

## AGGREGATE

`AGGREGATE` asks the server to group and reduce, so only the aggregated result
crosses the wire instead of every matching row. It carries structure, never SQL
text — the server composes the statement from validated parts, the same posture
as `SCAN`, and unlike `QUERY` it stays qualified to the session's catalog.

```json
{"schema": "main", "table": "events",
 "columns": ["region", "amount"],
 "filters": [["ts", ">=", "2026-01-01"]],
 "group_by": ["region"],
 "aggregates": [{"fn": "sum", "col": "amount"}, {"fn": "count"}]}
```

- `group_by` names plain columns; it may be empty, which means an ungrouped
  aggregate over the whole (filtered) table.
- `aggregates[].fn` is one of `count`, `sum`, `min`, `max`, `avg`. A server
  **must** reject anything else rather than interpolate it.
- `aggregates[].col` is absent for `count(*)`; present for everything else,
  including `count(col)` — the two differ on NULLs, so the distinction matters.
- `columns` lists the base columns the request references. It is advisory (a
  hook for per-column authorization); the SQL is built from `group_by` and
  `aggregates`.
- `filters` is the same clause format as `SCAN`.

**Result columns are positional**: every group key in request order, then every
aggregate in request order. Result *names* are the server's own (`g0…`, `a0…`)
and carry no meaning — clients bind by position. A server should generate those
aliases itself rather than echo anything client-supplied.

An ungrouped aggregate returns exactly one row even when nothing matches
(`count` → 0, others → NULL); a grouped aggregate over no rows returns none.

Result **types** follow from the server engine's own aggregate rules. Note that
DuckDB's `sum()` over any integer returns `HUGEINT`, which has no default
lossless Arrow encoding and therefore ships as `decimal128(38,0)`; a client that
planned for `HUGEINT` must reconcile that itself.

## Capabilities

Every handler op is optional, so two servers on the same `protocol_version` can
differ in what they implement. `capabilities` is how a server says which
optional features it actually supports:

| Name | Meaning |
|------|---------|
| `aggregate_pushdown` | `AGGREGATE` (op 13) is implemented. |

Advertised on `HELLO_ACK`, or on `READY` when the session build was deferred and
no handler existed at ack time. A client must merge both, and treat an absent or
empty list as "nothing optional supported".

This exists because a run-time `NotImplementedException` is too late for some
clients. The DuckDB driver's aggregate-pushdown optimizer rewrites the query
plan *before* any request is sent and cannot fall back once it has, so it must
know the answer at planning time. Capability is read per attached catalog, since
one client can hold several n6k connections of different vintages.

## PUSH ops

PUSH frames are server-initiated (no `id`). The header is `{t:8, op, …}` —
op + op-specific keys, mirroring the REQ convention.

| Op | Name | Header keys | Semantics |
|----|------|-------------|-----------|
| 12 | CATALOG_INVALIDATED | `{schemas: ["a", "b"]}` | Client should drop its cached table list for each named schema in this WebSocket's catalog and refetch on next access. Unknown schema names are ignored. |

Clients that don't recognise a PUSH op MUST ignore the frame (no error).
This keeps the op space additive: a new op can ship server-side without
forcing a protocol-version bump.

## Error reporting

Any op can be terminated by a single `ERR` frame (frame type 5) carrying
`{exception_type, exception_message}` in the header — the same shape DuckDB
uses internally for `ErrorData` serialization. An optional `retriable: true`
flags transient faults the client may safely retry (e.g. a
`TransactionException` write-write conflict).

`exception_type` is the name of the DuckDB `Exception` subclass the
client should throw: `IOException`, `CatalogException`, `BinderException`,
`ConstraintException`, `InvalidInputException`, `NotImplementedException`,
`ParserException`, `SyntaxException`, `ConversionException`,
`InvalidTypeException`, `TypeMismatchException`, `OutOfRangeException`,
`OutOfMemoryException`, `PermissionException`, `TransactionException`,
`ConnectionException`, `FatalException`, `InternalException`,
`SerializationException`, `InterruptException`, `SequenceException`,
`HTTPException`, `DependencyException`. Unknown values MUST be treated
as `IOException` by the client.

`exception_message` is the server's `str(exc)`. For non-DuckDB Python
exceptions, the server prepends the originating class name
(`"RuntimeError: <msg>"`) so debuggability is preserved while keeping
`exception_type` bounded to a class the client can reconstruct.

Server-side mapping:

| Python exception | `exception_type` | HTTP status |
|------------------|------------------|-------------|
| `NotSupported` | `NotImplementedException` | 501 |
| `KeyError` | `CatalogException` | 404 |
| `ValueError` / `JSONDecodeError` | `InvalidInputException` | 400 |
| `duckdb.Error` subclass | `type(exc).__name__` | per class (user-error → 4xx, system-error → 5xx) |
| anything else | `IOException` | 500 |

The client prefixes the reconstructed message with
`n6k[<catalog_name>] <OP>: ` so multi-catalog queries disambiguate the
source.

## TABLES_LIST row shape

Each entry in the `tables` array:

| Column | Type | Required | Default |
|--------|------|----------|---------|
| `schema` | string | no | `"main"` |
| `name` | string | yes | — |
| `writable` | bool | no | `false` |
| `editable` | bool | no | `false` |
| `primary_keys` | string[] | no | `[]` |

`writable` reports whether data may be modified (INSERT/UPDATE/DELETE);
`editable` reports whether columns may be changed (ALTER TABLE);
`primary_keys` lists the primary-key column names in key order. All three are
surfaced client-side by `n6k_table_permissions` on the attached `TYPE n6k`
catalog (as `writeable`, `editable`, `primary_key`). A server omitting
`editable` or `primary_keys` yields `false` / `[]` (back-compatible).

The client uses `{schema, name}` as the table identity on subsequent
`TABLE_SCHEMA` / `SCAN` / `INSERT` frames.

## Lifecycle

1. Client opens `wss://host/ws?catalog=<name>` with
   `Authorization: Bearer <token>`, and sends a `HELLO` frame. The server catalog
   is selected by **either** the `?catalog=` URL query param **or** the `catalog`
   field of the `HELLO` frame — the latter lets a host that owns the socket
   (`wsFd`/`wsId`) dial a bare `/ws` without baking the catalog into the URL. If
   both are supplied and disagree, the server rejects the connection with a
   conflict error (`HELLO_ERR`, connection-scoped).
2. Server authenticates, then sends `HELLO_ACK` with
   `{protocol_version:6, max_concurrent_reqs, default_batch_credits}`. If the
   session builds quickly the ack is sent once the build completes (no
   `session_pending`, no `READY`). If the build runs long, the server acks early
   with `{..., session_pending:true}` so the client's transport is up
   immediately, keeps the connection alive (answering `PING`), and sends `READY`
   (connection-scoped, no `id`) once the build finishes. The client gates its
   first request until the ack opens it (fast build) or `READY` arrives (slow
   build). If auth or the session factory fails, the server instead sends
   `HELLO_ERR` (`{exception_type, exception_message}`, connection-scoped) and closes — a fast
   failure precedes any `HELLO_ACK`; a failure during a deferred build follows
   the `HELLO_ACK{session_pending}`. Either way the client reports that message
   rather than a bare "socket closed".
3. Client issues `REQ` frames; server replies with matching `req_id`.
4. **Flow control.** Streaming responses start with
   `default_batch_credits` batches of budget. Server decrements on each
   `RESP_CHUNK`; pauses at 0. Client sends `CREDIT{id, n}` (the refill count
   `n` is a header field) to refill.
5. **Cancellation.** Client sends `CANCEL{id}`; server stops producing
   and emits a terminal `RESP_END` with `{"cancelled": true}`.
6. **Drop.** Either side closing fails all in-flight `req_id`s with a
   transport error. The client must re-ATTACH — session resume is not
   supported.

### Multiplexed sockets

When one socket carries several catalog sessions, each frame above is scoped by
`ns` (see the field note above the frame-type table). The lifecycle repeats per session:
a `HELLO {ns, catalog}` opens one, its `HELLO_ACK` is stamped with that `ns`, and
every subsequent `REQ`/`CREDIT`/`CANCEL`/response for it carries the same `ns`.
Sessions are independent — separate `req_id` spaces, and a `HELLO_ERR {ns}`
takes down only its own session. `PING`/`PONG` stay connection-scoped and are
never `ns`-stamped. `CANCEL {ns, id: 0}` (req id 0 is reserved) closes a single
session without touching the socket.

Two implementations serve this shape: `WsV2Router`
(`packages/python/src/n6k_protocol/engine.py`, via `register(..., multiplex=True)`)
and the native `n6k_server` extension when a serve function is given more than one
catalog ([`n6k-server.md`](n6k-server.md#sessions-ns), which also lists where the
native server deliberately differs). On the client side, a native `wsFd` may be
shared across several `ATTACH`es; a hub owns the fd and demultiplexes inbound
frames by `ns` ([`n6k-network.md`](n6k-network.md)).

## Chunk sizing

One Arrow `RecordBatch` per `RESP_CHUNK`. Target **256 KB – 1 MB**
uncompressed. Final-tail batches may be smaller.

## HTTP endpoints

None. Every operation — including DDL, via `OP_CREATE_TABLE` and
`OP_ALTER_TABLE` — flows over the `/ws` WebSocket session. A conforming server
needs to serve exactly one endpoint.

The only HTTP a client speaks is OAuth: discovery, token exchange and the
device-code flow, against the authorization server rather than against an n6k
server. See [`n6k-network.md`](n6k-network.md).

## See also

- [`n6k-network.md`](n6k-network.md) — the SQL / host-integration side: `ATTACH`,
  authentication, `wsId`/`wsFd`, and the function registry.
- [`n6k-conformance.md`](n6k-conformance.md) — what a server must provide *beyond*
  this protocol to be driven by the test suites, and how to register it as a target.
