# n6k-duckdb

In-process bridge between DuckDB connections with permission-based access control.

## Install

```bash
pip install n6k-duckdb
```

## Usage

```python
import duckdb
from n6k_server.bridge import bridge

cfg = {"allow_unsigned_extensions": "true"}
source = duckdb.connect(config=cfg)
source.sql("CREATE TABLE users(id INTEGER PRIMARY KEY, name VARCHAR)")
source.sql("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")

target = duckdb.connect(config=cfg)
bridge(source, target, "app", source_catalog="memory", permissions={"main.users": "readwrite"})

# Query through the bridge
target.sql("SELECT * FROM app.main.users").show()

# Insert through the bridge (requires 'readwrite')
target.sql("INSERT INTO app.main.users VALUES (3, 'charlie')")

# Update and delete also work with 'readwrite'
target.sql("UPDATE app.main.users SET name = 'Alice' WHERE id = 1")
target.sql("DELETE FROM app.main.users WHERE id = 2")
```

## Permissions

| Permission | Allows |
|------------|--------|
| `'read'` | SELECT only |
| `'readwrite'` | SELECT, INSERT, UPDATE, DELETE |

Tables not listed in permissions are inaccessible.

## How it works

`bridge()` loads the `virtual_catalog_bridge` DuckDB extension — from `$VIRTUAL_CATALOG_EXT_DIR` when set, otherwise installed from the n6k extension repository. The extension creates an in-process bridge between two DuckDB database instances using a token-based handshake; one `bridge()` call is one attached catalog, and `unbridge()` detaches it.

No network server is required — data flows directly between connections in the same process.

## Server framework (optional extra)

This package also ships a reusable FastAPI framework for serving the n6k
protocol — install with `pip install "n6k-duckdb[test-server]"`.

The protocol itself is **not implemented in Python**. It is served by the
`n6k_server` DuckDB extension (C++); this package accepts the WebSocket, decides
which catalogs to serve, hands the socket to the extension, and shuttles bytes.
It never decodes a frame.

Minimal server with a DuckDB backend:

```python
import duckdb
from fastapi import FastAPI, WebSocket
from n6k_server.extension import load_n6k_server
from n6k_server.pump import WsReject
from n6k_server.server_fastapi.register import register

app = FastAPI()


def open_db(ws: WebSocket, **_):
    catalog = ws.query_params.get("catalog")
    if not catalog:
        raise WsReject(4400, "missing catalog")
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    load_n6k_server(con)
    con.execute(f'ATTACH \':memory:\' AS "{catalog}"')
    # ... populate tables in `con` ...
    return con


register(app, "", connect=open_db)

# Run: `uvicorn your_module:app --port 8099`
```

`connect` runs once per WebSocket and returns the connection to serve it from.
That is the entire contract — every catalog it ATTACHed is served, and the
connection's own default database (`memory`, for a plain `duckdb.connect()`) is
not.

`con` is surrendered to the serving call for the connection's lifetime and closed
on teardown, so nothing else may touch it. `connect` may be sync, async, or an
async generator — yield when you need a hook on both ends:

```python
async def open_db(ws):
    con = duckdb.connect()
    ...
    live[id(ws)] = con.cursor()   # a way into the data while it is being served
    try:
        yield con                 # served here; resumes when the socket drops
    finally:
        del live[id(ws)]
```

A worked example — auth, per-WS observability, `/debug/*` hooks — lives in
`src/n6k_server/test_server/app.py`. Run it with
`python -m n6k_server.test_server --port 8099`.

### Auth

Two credentials arrive on two channels, and each is checked by whichever side can
see it.

**Transport** — `Authorization: Bearer` or a query param, on the HTTP upgrade.
This process terminates the upgrade, so only it can read them. Check them in
`connect` and `raise WsReject(4401, "unauthorized")`.

**Handshake frame** — where a browser must put its token, since a browser cannot
set a header on a WebSocket upgrade. Define a function named `n6k_authorize` on
the connection and the extension calls it for every handshake:

```python
con.create_function(
    "n6k_authorize",
    lambda token, catalog: token == expected,   # or raise, to say why
    [duckdb.sqltype("VARCHAR"), duckdb.sqltype("VARCHAR")],
    duckdb.sqltype("BOOLEAN"),
)
```

Registering it is the whole opt-in — there is no auth setting. `False` refuses the
session; raising sends your message to the client. Reading that token in Python
would mean parsing frames, which is the extension's job.

### Notifying clients of stale catalog entries

When the server's catalog state changes outside of a client's own DDL (e.g.
another writer dropped a view), peers' cached table listings go stale. Call

```sql
CALL n6k_serve_push_invalidate('<catalog>', 'main')
```

on any connection to the same database (a `cursor()` of the served connection
works) to send one `FT_PUSH(OP_CATALOG_INVALIDATED)` to every session serving
that catalog; each client drops its cached entries for the named schemas and
refetches on next access. It returns how many sessions it reached.

Nothing fires this automatically — the server decides when a catalog changed.
