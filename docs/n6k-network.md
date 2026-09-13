# n6k Network Extension (`TYPE n6k`)

Attach a remote catalog served over WebSocket + HTTP. This documents the **SQL /
host-integration** surface — how to attach, authenticate, ride a socket your app
already opened, and which functions route to the server. For the **on-the-wire**
frame format the client and server speak, see
[`n6k-network-protocol.md`](n6k-network-protocol.md). The in-process bridge and
provider catalogs (no network, no server) live in the sibling
`duckdb-virtual-catalog` repo.

Attached catalogs run over a single WebSocket per ATTACH; every operation —
schema discovery, scans, inserts, DDL, exec, query, and RPC — is multiplexed on
that socket, `CREATE TABLE` included (`OP_CREATE_TABLE`). Catalog-scoped table
functions use the `n6k_catalog_*` prefix (e.g. `n6k_catalog_exec`,
`n6k_catalog_rpc`). The only HTTP a client speaks is OAuth discovery / token
exchange, and that is against the authorization server, not an n6k server.

## Base URL

```sql
ATTACH 'n6k://host:port/prefix' AS db (TYPE n6k);
```

`TYPE n6k` is required — DuckDB does not auto-detect the storage type from
the `n6k://` URL scheme.

The URL scheme is translated for HTTP helpers (`n6k://` → `http://`,
`n6ks://` → `https://`) and for the WebSocket upgrade (`http://` → `ws://`,
`https://` → `wss://`, path `/ws?catalog=<name>` where `<name>` is the
`ATTACH ... AS <name>` alias). Trailing slashes on the base URL are
stripped. All paths below are relative to the base URL.

## Authentication

### OAuth (standards-based, native)

The native extension can obtain and auto-renew a short-lived data-service JWT
from a durable credential, with no n6k-specific endpoints — the auth server need
only implement these standards:

- RFC 8414 — discovery (`/.well-known/oauth-authorization-server`)
- RFC 8628 — device login (`n6k_login`, Phase 3)
- RFC 6749 — token endpoint (underlies the device grant + the exchange)
- RFC 8693 — token exchange (durable subject token → `aud=data-service` JWT)
- RFC 7519 + 7517 + 7518/8037 — JWT, published key set (JWKS), EdDSA

A **`n6k_refresh` secret** stores the durable subject token plus the `issuer`
(auth base URL), `client_id`, and `resource` (the data-service audience). When an
attach/request needs a token and none is cached (or the cached JWT is within ~60s
of `exp`), the extension performs RFC 8414 discovery against `issuer`, then an
RFC 8693 token exchange at the discovered token endpoint, and caches the minted
JWT as a **temporary `n6k` access secret**. The exchange is single-flight per
host. This whole path is **native-only** — in the browser the host app owns the
OAuth lifecycle and supplies the `n6k` access secret directly.

```sql
CREATE PERSISTENT SECRET n6k_auth (
  TYPE n6k_refresh, SUBJECT_TOKEN '…', ISSUER 'https://auth.example',
  CLIENT_ID 'n6k-duckdb', RESOURCE 'data-service', SCOPE 'data.example:443'
);
ATTACH 'n6k://data.example:443' AS db (TYPE n6k);  -- mints + renews automatically
```

### bearer token

The bearer token sent as `Authorization: Bearer <token>` on the WebSocket
upgrade is resolved with this precedence:

1. **Inline `token` option** on the ATTACH (legacy, plaintext in SQL):

   ```sql
   ATTACH 'n6k://host:port/prefix' AS db (TYPE n6k, token 'your_token');
   ```

2. **`TYPE n6k` secret** — the token lives in DuckDB's Secret Manager
   (redacted in `duckdb_secrets()`, never in the ATTACH text):

   ```sql
   CREATE SECRET my_n6k (TYPE n6k, TOKEN 'your_token', SCOPE 'host:port');
   ATTACH 'n6k://host:port/prefix' AS db (TYPE n6k);   -- no token in SQL
   ```

   `SCOPE` is matched as a literal **prefix** of the scheme-stripped base URL
   (`host[:port][/prefix]`), so `SCOPE 'host'` matches any port/path on that
   host and an omitted `SCOPE` is a catch-all. The scheme is stripped on both
   sides, so the bare hostname works regardless of how ATTACH was spelled
   (`n6k://…` or `(host '…', port '…')`). Use `CREATE OR REPLACE SECRET` to
   rotate; the next ATTACH (and every subsequent HTTP request) picks it up.

3. **`TYPE n6k_refresh` secret (native)** — if no valid `n6k` access token is
   present, an on-demand OAuth mint (see *OAuth* above) produces and caches one.

4. Otherwise **no header** is sent (anonymous); the server rejects with
   401 / WS close 4401 if it requires auth.

Passing **`anonymous true`** on the ATTACH forces case 4 directly: it skips the
`TYPE n6k` secret lookup (2) and any `n6k_refresh` mint (3) — at attach time and
on every subsequent request — so the connection stays anonymous regardless of
what secrets exist. It is mutually exclusive with the inline `token` option (1).

```sql
ATTACH 'n6k://host:port' AS db (TYPE n6k, anonymous true);
```

This is useful for unauthenticated local/test servers, and it keeps a stale
`n6k_refresh` secret from breaking an otherwise-anonymous attach (a dead issuer
would otherwise fail the connect during OAuth discovery).

Note: WebSocket auth is checked once at the upgrade handshake and never
re-validated on a live connection.

## Attaching an existing WebSocket (WASM/browser)

By default the driver opens and owns one WebSocket per ATTACH. In the browser
build you can instead hand it a socket your app already opened — so the app
keeps control of the socket's auth and reconnection, and can keep using the same
connection for its own traffic:

```js
const { conn, registerWebsocket, replaceWebsocket } = await createDuckDB();
const wsId = registerWebsocket(ws); // ws: a WebSocket you opened; returns "ext_1"
await conn.query(`ATTACH '' AS foo (TYPE n6k, wsId '${wsId}')`);
```

The `wsId` ATTACH option is **WASM/browser only** — native throws. Because the
socket is already open, several things the driver normally owns become the app's
responsibility, established by *how the socket was opened*:

- **Catalog binding** — the catalog comes from `?catalog=<name>` on the socket's
  URL, or, when the URL omits it, from the `catalog` field the driver sends in its
  `FT_HELLO` frame (the local `ATTACH ... AS <name>` alias). So you can either open
  the socket against a route that already binds the catalog (`?catalog=…`, read at
  upgrade time) **or** dial a bare `/ws` and let the driver declare the catalog in
  the handshake. When the URL carries `?catalog=` it takes effect and the alias
  stays independent of the server catalog; if both the URL and the handshake
  supply a catalog and they disagree, the server rejects with a conflict error.
- **Auth** — whatever the socket's upgrade carried (cookie, `Authorization`
  header, `?token=`) already authenticated it. The driver still sends an
  `FT_HELLO` frame (carrying the catalog above, and an empty token by default) so
  it receives the server's `HELLO_ACK` (protocol version + credit tunables; see
  the [handshake lifecycle](n6k-network-protocol.md#lifecycle)); an
  already-authenticated server ignores the `HELLO` token. Pass a `token` ATTACH
  option only if you want the driver to perform the auth itself.

**Binary split.** Every n6k frame is a binary WebSocket message, so the driver
claims **binary** frames and leaves **text** for the app. It attaches its
listener with `addEventListener` (never overwriting `onmessage`) and forwards
only `ArrayBuffer` messages, so the app keeps sending/receiving its own text
traffic on the same socket. `binaryType` is forced to `"arraybuffer"`; don't put
your own binary traffic on a shared socket. For this to work end-to-end on a
*shared* endpoint, your **server** must demultiplex the mirror image: route
inbound binary frames into an n6k `WsV2Connection` and keep your own text
handling separate. A socket dedicated to n6k needs no server-side change — point
it at the standard `/ws?catalog=<name>` endpoint.

**Reconnection.** The driver cannot reopen a socket it does not own. On a drop it
reports `disconnected` status; the app opens a fresh socket and hands it over,
keeping the catalog attached — no DETACH/ATTACH, and in-flight requests fail so
the app re-issues them:

```js
replaceWebsocket(wsId, newWs);
```

One registered socket backs **one** catalog (matching the server's
one-connection-one-catalog model); use a separate socket per catalog.

## Attaching an existing socket fd (native)

The native build has the fd-level analogue of `wsId`: hand the extension one end
of a connected socket — typically `socket.socketpair()` — as a raw file
descriptor, and it speaks n6k frames over that fd instead of dialing the server
itself. The host process opens the real WebSocket, performs the HTTP upgrade plus
any custom pre-`HELLO_ACK` handshake and auth, then pumps frames between the
WebSocket and its end of the pair:

```python
import socket, struct, threading
import duckdb
from websockets.sync.client import connect as ws_connect

py_sock, ext_sock = socket.socketpair()
ws = ws_connect("ws://localhost:8099/ws?catalog=db")  # handshake done here

# Two daemon threads pump *length-framed* frames both ways. Each n6k frame is
# prefixed with a 4-byte big-endian length — the same wire convention as
# src/n6k_server/uds_transport.cpp.
def ws_to_sock():
    for msg in ws:
        if isinstance(msg, str):
            msg = msg.encode()
        py_sock.sendall(struct.pack(">I", len(msg)) + msg)

def sock_to_ws():
    while True:
        hdr = py_sock.recv(4)
        if len(hdr) < 4:
            break
        (n,) = struct.unpack(">I", hdr)
        ws.send(py_sock.recv(n, socket.MSG_WAITALL))

threading.Thread(target=ws_to_sock, daemon=True).start()
threading.Thread(target=sock_to_ws, daemon=True).start()

con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
con.load_extension("/path/to/n6k_client.duckdb_extension")
con.execute(f"ATTACH '' AS db (TYPE n6k, wsFd {ext_sock.fileno()})")
ext_sock.close()  # the extension dup()'d the fd; drop our copy

con.execute("SELECT * FROM db.main.users")
```

The `wsFd` ATTACH option is **native only** — the WASM build throws (use `wsId`
there instead). Because the socket is already open, the same responsibilities
that `wsId` shifts to the app apply here:

- **Catalog binding** — the catalog comes from `?catalog=<name>` on the
  WebSocket's URL, or, when the URL omits it, from the `catalog` field the
  extension sends in its `FT_HELLO` frame (the local `ATTACH ... AS <name>`
  alias). So the host can open the socket against a route that binds the catalog
  (`?catalog=…`) **or** dial a bare `/ws` and let the extension declare the
  catalog in the handshake. When the URL carries `?catalog=` it takes effect and
  the alias stays independent of the server catalog; if both supply a catalog and
  they disagree, the server rejects with a conflict error.
- **Auth / handshake** — whatever the upgrade carried already authenticated the
  socket, and the host runs any custom pre-ack handshake before the `ATTACH`. The
  extension sends an `FT_HELLO` frame (carrying the catalog above, with an empty
  token) and waits for the server's `HELLO_ACK` (protocol version + credit
  tunables), which arrives over the fd via the pump.

**Length framing (required).** Over a WebSocket each binary message is exactly
one n6k frame, so message boundaries come for free. A raw byte stream has none —
so the fd path **must** length-frame: every frame is written as
`[4-byte big-endian length][frame bytes]`, and the reader reads the length, then
exactly that many bytes. This is the `src/n6k_server/uds_transport.cpp` wire
convention; `packages/python/src/n6k_server/pump.py` is a reference pump.
The extension framing is implemented in `src/n6k_client/ws_transport_fd.cpp`.

**fd ownership.** The extension `dup()`s the fd at `ATTACH` time and owns the
copy, so the host closes its own `ext_sock` right after attaching — no
double-close race. On `DETACH` (or connection teardown) the extension closes its
dup, which the host observes as EOF on `py_sock`, letting its pump threads exit.

**Sharing one fd across several `ATTACH`es.** The same `wsFd` may back more than
one catalog. A process-global hub (`src/n6k_client/ws_fd_hub.cpp`) owns the fd: exactly
one `dup()`, one reader thread, one serialized writer, no matter how many
sessions ride it. Inbound frames are demultiplexed by the protocol's `ns` key —
each `ATTACH` stamps a distinct `ns` on its `FT_HELLO`, the server stamps every
session-scoped reply with the same `ns`, and the hub routes on it. Frames with no
`ns` (`PONG`, and a pre-multi-catalog server's `HELLO_ACK`/`HELLO_ERR`) are
connection-scoped and reach every bound session; a frame naming an `ns` nobody
claims is dropped rather than handed to a sibling.

Without the hub each `ATTACH` ran its own blocking reader on its own `dup()` of
the same socket. The byte stream has no per-session boundaries, so the readers
split frames between themselves at arbitrary points — invisible with one session,
silent corruption with two. `test/sql/n6k_wsfd_hub.test` pins the routing.

The fd is closed only when the **last** session on it detaches, and teardown
never calls `shutdown()` — that would tear the socket down for every sibling.
Because the hub registry is keyed by fd *number*, a host must not close and
recycle an fd number while an `ATTACH` still rides it.

**Conflicts.** `wsFd` is mutually exclusive with a URL / `host` / `port` / `secure`
/ `prefix`: the socket is already connected, so any dial target is rejected as a
hard `IOException` rather than silently ignored. Token resolution is skipped too —
the host already authenticated the socket.

A full worked example (with the bidirectional pump and a `DETACH`-closes-the-fd
assertion) is in `integration_tests/test_wsfd_attach.py`.

## Function registry

| Function | Transport | Scope |
|----------|-----------|-------|
| `n6k_catalog_exec('db', sql)` | WS EXEC | attached catalog |
| `n6k_catalog_query('db', sql)` | WS QUERY | attached catalog |
| `n6k_catalog_rpc('db', 'func', args)` | WS RPC_SCALAR | attached catalog |
| `n6k_catalog_rpc_table('db', 'func', sub, args)` | WS RPC_TABLE | attached catalog |
| `db.exec(sql)` | WS EXEC | attached catalog |
| `db.query(sql)` | WS QUERY | attached catalog |
| `db.func(args)` | WS RPC | attached catalog |
| `n6k_version()` | none | utility |
| `n6k_parse_sql_get_tables(sql)` | none | utility — returns `(catalog, schema, table_name, ref_type, privilege_type)`; `catalog`/`schema` are NULL when the user did not write them, `ref_type` is `'base_table'` or `'table_function'`, `privilege_type` is the access the statement needs for that table: `'select'`/`'insert'`/`'update'`/`'delete'` for DML and `'create'`/`'drop'`/`'alter'` for DDL targets (CREATE/DROP/ALTER TABLE and VIEW). DDL views are reported with `ref_type='base_table'`. `DESCRIBE`/`SUMMARIZE` of a table (or sub-select) report their underlying table(s) as `'select'`, so cached `DESCRIBE` queries invalidate on writes; bare catalog listings (`SHOW TABLES`/`SHOW DATABASES`) reference no specific table and return nothing. TRUNCATE is not parseable by DuckDB and is unsupported. |

Each op above maps to one WS REQ op; the request args and streamed Arrow
responses are specified in
[`n6k-network-protocol.md` → REQ ops](n6k-network-protocol.md#req-ops).

There is no URL-first variant of these functions. `n6k_scan`, `n6k_rpc` and
`n6k_rpc_table` took a bare URL and used the HTTP surface; both they and it are
gone. ATTACH the catalog and use the `n6k_catalog_*` form.

## See also

- [`n6k-network-protocol.md`](n6k-network-protocol.md) — the on-the-wire frame
  format, ops, and lifecycle (for server / alternate-client implementers).
