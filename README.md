# n6k DuckDB Extensions

Two DuckDB extensions for remote and in-process data access.

## Extensions

**n6k** — WebSocket bridge for querying remote servers via Arrow IPC streaming.

- Attach remote databases: `ATTACH 'n6k://host:port/path' AS db (TYPE n6k, token '...');`
- Credentials via DuckDB secrets (keeps the token out of SQL/logs):
  `CREATE SECRET (TYPE n6k, TOKEN '...', SCOPE 'host:port');` then attach without a token
- All attached-catalog traffic multiplexed over one WebSocket per ATTACH
- Projection and filter pushdown
- INSERT, UPDATE, DELETE, CREATE TABLE AS, views
- RPC table functions with dynamic binding
- WASM/browser support (Emscripten)
- Attach onto a WebSocket the host app already opened (browser): `registerWebsocket(ws)` then `ATTACH '' AS db (TYPE n6k, wsId '…')` — the catalog can ride the HELLO handshake, so the host may dial a bare `/ws`; see [n6k-network.md](n6k-network.md#attaching-an-existing-websocket-wasmbrowser)
- Attach onto a connected socket fd the host owns (native): `ATTACH '' AS db (TYPE n6k, wsFd <fileno>)` — the fd-level analogue of `wsId` (same bare-`/ws` catalog-in-HELLO support), see [n6k-network.md](n6k-network.md#attaching-an-existing-socket-fd-native)

The in-process bridge and provider catalogs (`virtual_catalog_bridge`,
`virtual_catalog_provider`) live in the sibling `duckdb-virtual-catalog` repo. The
Python package drives them; point `VIRTUAL_CATALOG_EXT_DIR` at that repo's
`build/release/extension` to use a local build.

## Authentication (n6k)

The bearer token can be supplied inline on the ATTACH, or — preferred — via a
DuckDB secret so it stays out of SQL text and logs (redacted in
`duckdb_secrets()`):

```sql
-- store the credential once (SCOPE is the host; scheme is ignored)
CREATE SECRET my_n6k (TYPE n6k, TOKEN 'your_token', SCOPE 'host:port');

-- attach with no token in the statement
ATTACH 'n6k://host:port' AS db (TYPE n6k);

-- rotate in place; the next ATTACH / request uses the new value
CREATE OR REPLACE SECRET my_n6k (TYPE n6k, TOKEN 'new_token', SCOPE 'host:port');
```

Precedence: an inline `token '…'` option wins; otherwise a matching `TYPE n6k`
secret is used; otherwise the connection is anonymous. `SCOPE` is matched as a
prefix of the host (`'host'` matches any port/path; omit it for a catch-all).

To force an **anonymous** connection — no token, skipping the secret lookup and
any `n6k_refresh` OAuth mint — pass `anonymous true`:

```sql
ATTACH 'n6k://host:port' AS db (TYPE n6k, anonymous true);
```

Useful for local/test servers that need no auth, and it prevents a stale
`n6k_refresh` secret from hijacking the attach (an unreachable issuer would
otherwise fail the connect during OAuth discovery). Mutually exclusive with the
inline `token` option.

For OAuth deployments, a `TYPE n6k_refresh` secret (native only) holds a durable
subject token and auto-mints/renews short-lived data-service JWTs via standard
OAuth discovery + RFC 8693 token exchange — see
[n6k-network.md](n6k-network.md#oauth-standards-based-native).
`n6k_login` runs the interactive device flow and stores that secret for you:

```sql
-- opens a verification URL, waits for approval, stores a persistent n6k_refresh secret
SELECT * FROM n6k_login('https://auth.example', resource := 'data-service');
ATTACH 'n6k://data.example:443' AS db (TYPE n6k);  -- mints + renews automatically
```

`n6k_login(auth_url, client_id := 'n6k-duckdb', resource := NULL)` is native-only
(throws in the browser, where the host app owns the OAuth lifecycle).

## Connection readiness & timeouts (n6k)

A server session can take seconds to build (remote attach, catalog warm). The
driver splits "transport up" from "session usable" so a slow build doesn't fail
the connection:

- **ATTACH returns as soon as the WebSocket handshake completes** — it no longer
  blocks on the session build, and creates only the default schema (no network
  round-trip). The first catalog access (a query, or `SHOW SCHEMAS`) is what waits
  for the session to become usable.
- **`ready_timeout`** (ATTACH option, default **`'60s'`**) bounds that wait. A
  duration string (`'60s'`, `'5000ms'`) or bare number of seconds. If the build
  isn't ready in time, the **first query** fails with a typed
  `n6k session not ready after …` error (not a bare "socket closed"). A build that
  *fails* surfaces the server's error on the first query instead.

  ```sql
  ATTACH 'n6k://host:port' AS db (TYPE n6k, ready_timeout '120s');
  ```

- **Handshake timeout** is separate and fixed at **5s** (native): it bounds only
  the WebSocket connect + auth handshake (`HELLO_ACK`), which is fast and does not
  include the session build. It is not a tunable ATTACH option.

> **wasm/browser note:** `ready_timeout` is honored on native only. The wasm build
> gates the first request the same way, but surfaces a stuck/failed build via the
> socket closing rather than a client-side timer — there is no `ready_timeout`
> countdown in the browser.

## Building

Prerequisites: CMake, C++17 compiler, Python 3.10+ with `uv`.

```sh
make release
```

Output:

```
build/release/extension/n6k_client/n6k_client.duckdb_extension
build/release/extension/n6k_server/n6k_server.duckdb_extension
```

## Testing

First start the test server (defaults to `:8099`) in another shell — the
integration and TypeScript suites connect to it:

```sh
uv run test-server
```

Then, per area (each block starts from the repo root):

`packages/python` — unit + bridge:

```sh
cd packages/python
uv run pytest
uv run pytest --external "postgres://user@host/db"  # also test an external DB (opt-in)
```

`integration_tests` — driver ⇄ server:

```sh
uv run pytest integration_tests
```

`packages/npm` — TypeScript driver:

```sh
cd packages/npm
bun test               # current backend
bun integration-tests  # all backends: native, wasm-node, browser
```

## Golden state

Golden = every suite above passes, plus format / lint / types below.

C++ and protocol mirrors — these `make` targets need the venv (for `python`
plus the clang tools):

```sh
source .venv/bin/activate
make format-check    # clang-format (make format-fix to apply)
make tidy-check      # clang-tidy
make protocol-check  # generated TS/C++ protocol mirrors in sync
make protocol-gen    # regenerate the TS/C++ mirrors after editing protocol.py
```

The `make` targets call bare `python`, so the venv must be activated first —
without it you'll get `python: No such file or directory`.

`packages/python`:

```sh
cd packages/python
uv run black --check .
uv run flake8 .
uv run mypy . --strict
```

`packages/npm`:

```sh
cd packages/npm
bun run ci  # prettier + eslint + tsc + test
```

Protocol constants live in a single Python source of truth
(`packages/python/src/n6k_protocol/protocol.py`); TypeScript and C++ mirrors are
generated via `make protocol-gen` and verified by `make protocol-check`.

## Docs

- [n6k-network.md](n6k-network.md) — `TYPE n6k` network extension: `ATTACH`, auth/secrets, `wsId`/`wsFd`, function registry.
- [n6k-network-protocol.md](n6k-network-protocol.md) — the on-the-wire protocol for `TYPE n6k` (frame format, ops, lifecycle, HTTP).
- [packages/python/docs/api.md](packages/python/docs/api.md) — Python API reference (`n6k_protocol` + `n6k_server`) (bridge helper, Provider, server framework).

## Python

```sh
cd packages/python && uv pip install -e .
```

```python
from n6k_server.bridge import bridge

bridge(source, target, "my_bridge",
       source_catalog="memory", source_schema="main",
       permissions={"users": "readwrite", "logs": "read"})

target.sql("SELECT * FROM my_bridge.main.users")
```

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for system design, data flow, and key file map.

## Relationship to Quack

[Quack](https://duckdb.org/2026/05/12/quack-remote-protocol) replaces one layer:
plain remote attach — read, write, wasm. Real overlap, and it's free in v2.0.

It can't replace anything above that, because Quack's connection is a request and
n6k's is a stateful, isolated session. Per-client sandboxed databases, streaming
RPCs, session identity, socket adoption — all downstream of that one difference.

So you can't swap the bottom layer out either: it's what supplies the semantics
the rest depends on.

Quack connects DuckDBs inside a trust boundary. n6k serves DuckDB to clients you
don't trust.
