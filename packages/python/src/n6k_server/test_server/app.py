"""The FastAPI app: wires auth middleware, per-connection observability,
and the WebSocket mounts the conformance suite targets.

Run via `python -m n6k_server.test_server --port 8099`.
"""

import asyncio
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, Callable, Optional

import duckdb
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from n6k_server.extension import load_virtual_catalog_provider, load_n6k, load_n6k_server, load_n6k_testing
from n6k_server.provider import register_provider
from n6k_server.pump import WsReject, serve_and_close_connection
from n6k_server.test_server.fixtures import fixture_statements, rpc_fixture_statements
from n6k_server.test_server.host_rpc import register_host_rpcs
from n6k_server.server_fastapi.register import register
from n6k_server.test_server.auth import (
    http_present_token,
    is_auth_enabled,
    is_authorized_http,
    ws_present_token,
)
from n6k_server.test_server.oauth import mount_oauth_as
from n6k_server.test_server.providers import (
    SLOW_CATALOG,
    CategoricalProvider,
    SlowProvider,
)
from n6k_server.test_server.seeding import (
    BRIDGE_CATALOG,
    seed_bridge,
    seed_memory,
)

logger = logging.getLogger("n6k_server.test_server")

app = FastAPI()

# Always-on auth modes selected by URL prefix, so tests pick a mode by attaching
# to n6k://host:port/<mode> instead of spinning up a server per mode:
#   (root)  open / no auth        ws://…/ws
#   /auth   static bearer token   ws://…/auth/ws   (token == AUTH_TOKEN)
#   /oauth  OAuth EdDSA JWT       ws://…/oauth/ws  (minted by the mounted AS)
# The OAuth Authorization Server (discovery/JWKS/token/device) is mounted here
# too and backs the /oauth verifier. The legacy global --token/--oauth gating
# (set in __main__, --no-reload) still gates the ROOT mount for existing tests.
AUTH_TOKEN = "n6k-test-token"
# Defining a function of this name is the whole opt-in: n6k_serve_fd finds it itself.
AUTH_FUNCTION = "n6k_authorize"
oauth_server = mount_oauth_as(app)


# ── Test-only observability ─────────────────────────────────────────────────


@dataclass
class _Live:
    """A connection currently being served, and a way to reach its data.

    `side` is a `cursor()` — an independent connection on the same database, taken
    before the serving CALL claimed the original. It is the only way in while a
    connection is live: the served connection blocks inside the CALL for the whole
    session, and duckdb serializes per connection, so reusing it would deadlock.

    Test scaffolding. A real consumer keeps a registry only if it wants one; the
    framework does not hand out either the socket or a side connection.
    """

    ws: WebSocket
    side: duckdb.DuckDBPyConnection
    catalogs: list[str]


_live: dict[int, _Live] = {}

_http_counts: dict[str, int] = {}


# Never auth-gated: test hooks (/debug/*) and the public OAuth AS surface
# (discovery, JWKS) plus the token/device endpoints clients fetch before they
# hold a token. NB /oauth/ is NOT a blanket open prefix anymore — /oauth/* data
# routes require a JWT; only the two AS endpoints below are open.
_OPEN_PREFIXES = ("/debug/", "/.well-known/", "/jwks")
_OPEN_EXACT = ("/oauth/token", "/oauth/device")


def _prefix_auth_mode(path: str) -> Optional[str]:
    if path.startswith("/auth/"):
        return "token"
    if path.startswith("/oauth/"):
        return "jwt"
    return None


@app.middleware("http")
async def _http_auth_and_count(request: Request, call_next: Callable[..., Any]) -> Any:
    path = request.url.path
    is_open = path in _OPEN_EXACT or any(path.startswith(p) for p in _OPEN_PREFIXES)
    if not is_open:
        mode = _prefix_auth_mode(path)
        if mode == "token":
            if http_present_token(request) != AUTH_TOKEN:
                return JSONResponse({"detail": "unauthorized"}, status_code=401)
        elif mode == "jwt":
            token = http_present_token(request)
            if not token or not oauth_server.verify(token):
                return JSONResponse({"detail": "unauthorized"}, status_code=401)
        elif is_auth_enabled() and not is_authorized_http(request):
            return JSONResponse({"detail": "unauthorized"}, status_code=401)
    key = f"{request.method} {request.url.path}"
    _http_counts[key] = _http_counts.get(key, 0) + 1
    logger.info("HTTP %s %s", request.method, request.url.path)
    return await call_next(request)


@app.get("/debug/counts")
def debug_counts() -> dict[str, dict[str, int]]:
    """HTTP request counts, plus flow-control counters read out of the C++ reactor."""
    totals = {"credit_pauses": 0, "cancels": 0, "connections": 0, "requests_handled": 0}
    for live in list(_live.values()):
        try:
            row = live.side.execute("SELECT * FROM n6k_serve_stats()").fetchone()
        except duckdb.Error:
            continue
        if row:
            totals["connections"] += int(row[0])
            totals["requests_handled"] += int(row[1])
            totals["credit_pauses"] += int(row[2])
            totals["cancels"] += int(row[3])
    return {"http": dict(_http_counts), "ws": totals}


@app.post("/debug/counts/reset")
def debug_counts_reset() -> dict[str, bool]:
    # Only the HTTP tally is resettable: the reactor's counters live for a connection's
    # lifetime and a new connection starts them at zero anyway.
    _http_counts.clear()
    return {"ok": True}


def _sessions_for(catalog: str) -> list[_Live]:
    """Live sessions serving `catalog`. A `/tuned` session serves several, so this
    matches membership rather than equality.

    "Live" is approximate by construction. A session is unregistered by the `finally`
    of the generator `register()` drives, which resumes only *after* `serve_and_close_connection`
    has already closed the served connection — so between those two moments an entry is
    still here with a `side` cursor onto a closed database. Callers must tolerate that;
    `_with_side` does.
    """
    return [live for live in _live.values() if catalog in live.catalogs]


async def _with_side(sessions: list[_Live], work: Callable[[duckdb.DuckDBPyConnection], Any]) -> list[Any]:
    """Run `work` on each session's side connection, skipping the ones already torn down.

    A session whose connection closed between `_sessions_for` and here is finished, not
    broken: it has no client left to hear a PUSH and nothing to run SQL against. Letting
    that surface as a 500 turns an ordinary teardown race into a test failure.
    """
    results: list[Any] = []
    for live in sessions:
        try:
            results.append(await asyncio.to_thread(work, live.side))
        except duckdb.Error:
            continue
    return results


@app.post("/debug/server_exec")
async def debug_server_exec(catalog: str, sql: str) -> dict[str, int]:
    """Run SQL against every live session serving `catalog`, bypassing the client.

    Goes in through the side connection, so nothing the client caches is refreshed as
    a side effect — which is what makes it usable for testing PUSH-driven invalidation.
    """
    ran = await _with_side(_sessions_for(catalog), lambda side: side.execute(sql))
    return {"handlers": len(ran)}


@app.get("/debug/server_query")
async def debug_server_query(catalog: str, sql: str) -> dict[str, Any]:
    def _fetch(side: duckdb.DuckDBPyConnection) -> list[Any]:
        return [list(r) for r in side.execute(sql).fetchall()]

    sessions = _sessions_for(catalog)
    # Only the first session is queried — they all serve the same catalog, so one answer
    # is the answer — but which one is "first" may be a corpse, hence the walk.
    for live in sessions:
        try:
            return {"handlers": len(sessions), "rows": await asyncio.to_thread(_fetch, live.side)}
        except duckdb.Error:
            continue
    return {"handlers": len(sessions), "rows": []}


@app.post("/debug/push_invalidate")
async def debug_push_invalidate(catalog: str, schemas: str) -> dict[str, int]:
    """Tell every client attached to `catalog` that those schemas changed.

    Frame construction belongs to the server, so this asks for the PUSH via SQL.
    """
    names = [s for s in schemas.split(",") if s]
    args = ", ".join(f"'{n}'" for n in [catalog] + names)
    sql = f"CALL n6k_serve_push_invalidate({args})"

    def _call(side: duckdb.DuckDBPyConnection) -> int:
        row = side.execute(sql).fetchone()
        return int(row[0]) if row else 0

    counts = await _with_side(_sessions_for(catalog), _call)
    return {"handlers": len(counts), "sent": sum(counts)}


@app.post("/debug/ws/close_all")
async def debug_ws_close_all() -> dict[str, int]:
    sessions = list(_live.values())
    for live in sessions:
        try:
            await live.ws.close(code=1011, reason="forced close")
        except RuntimeError:
            pass
    return {"closed": len(sessions)}


# ── Handlers ────────────────────────────────────────────────────────────────


def _native_con(catalog: str) -> duckdb.DuckDBPyConnection:
    """A connection with `catalog` attached and seeded, ready for the C++ reactor."""
    if catalog == SLOW_CATALOG:
        # Provider-backed: `slowdb.main.slow` routes into SlowProvider, whose scan
        # sleeps on the event loop. This is the case that forced in-process serving —
        # the provider's rows come from Python callbacks marshalled to this loop, so a
        # subprocess could never serve it.
        con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
        load_virtual_catalog_provider(con)
        load_n6k(con)
        load_n6k_server(con)
        load_n6k_testing(con)
        con.execute(f"ATTACH ':memory:' AS \"{catalog}\" (TYPE virtual_catalog_provider)")
        register_provider(con, catalog=catalog, schema_name="main", provider=SlowProvider())
        register_provider(con, catalog=catalog, schema_name="cats", provider=CategoricalProvider())
        return con
    if catalog == BRIDGE_CATALOG:
        # Bridge-backed: n6k_table_permissions routes to the bridge's own collector, so
        # tables_list reports the bridge's writeable/editable and primary keys.
        target, _source = seed_bridge(catalog)
        load_n6k_server(target)
        load_n6k_testing(target)
        return target
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    load_n6k(con)
    load_n6k_server(con)
    # rpc_fixture_statements() wraps n6k_testing_stream_counter / _sum_table in
    # macros, and the conformance suite RPCs those table functions by name.
    load_n6k_testing(con)
    seed_memory(con, catalog)
    for statement in rpc_fixture_statements(catalog):
        con.execute(statement)
    return con


def _credential_ok(mode: str, token: str) -> bool:
    return oauth_server.verify(token) if mode == "jwt" else token == AUTH_TOKEN


def _register_ws_auth(con: duckdb.DuckDBPyConnection, mode: str) -> None:
    """Register the verifier the reactor calls for every FT_HELLO.

    A browser puts its token inside the HELLO frame, and reading frames is the
    reactor's job — so the reactor reads it and calls back in here. Raising is how
    the reason reaches the client.
    """

    def _check(token: Optional[str], _catalog: Optional[str]) -> bool:
        if not token or not _credential_ok(mode, token):
            raise ValueError("unauthorized")
        return True

    varchar = duckdb.sqltype("VARCHAR")
    con.create_function(AUTH_FUNCTION, _check, [varchar, varchar], duckdb.sqltype("BOOLEAN"))


async def _open_db(
    ws: WebSocket, mode: Optional[str] = None, **_params: str
) -> AsyncIterator[duckdb.DuckDBPyConnection]:
    """Build the database this connection is served from, and register it as live.

    The catalog comes from `?catalog=` only: the pump must not parse frames, and both
    clients already put it in the URL (`n6k_catalog_session_native.cpp:313`).

    Auth keeps its documented precedence — header, then `?token=`, then the HELLO
    frame — split across the two layers that can each see one part. The first two are
    transport and are checked here; only when neither is present is `n6k_authorize`
    registered, deferring to the frame the reactor reads. Registering it regardless
    would demand a token in the frame from a client that already authenticated by
    header, which is exactly how native clients connect.

    Yields rather than returns so the `/debug/*` endpoints get a registry with real
    teardown: the side cursor must be taken before the serving CALL claims `con`, and
    dropped when the socket does.
    """
    catalog = ws.query_params.get("catalog")
    if not catalog:
        raise WsReject(code=4400, reason="missing required catalog (?catalog= query param)")

    con = _native_con(catalog)
    # Registered here rather than in `_native_con`: the generators run on this loop, and only an
    # async caller has one to give them. Off the loop thread, because creating the entry in a
    # provider catalog lists that provider's tables, and the provider answers on this loop.
    loop = asyncio.get_running_loop()
    await asyncio.to_thread(register_host_rpcs, con, catalog, loop)
    if mode is not None:
        presented = await ws_present_token(ws)
        if presented is not None:
            if not _credential_ok(mode, presented):
                raise WsReject(code=4401, reason="unauthorized")
        else:
            _register_ws_auth(con, mode)

    key = id(ws)
    _live[key] = _Live(ws=ws, side=con.cursor(), catalogs=[catalog])
    try:
        yield con
    finally:
        _live.pop(key, None)


# Every WS mount below is served by the C++ reactor; this process only pumps bytes.
# The mounts differ solely in which verifier the reactor calls for each FT_HELLO.
register(app, prefix="", connect=_open_db)

register(app, prefix="/auth", connect=lambda ws, **p: _open_db(ws, "token", **p))

register(app, prefix="/oauth", connect=lambda ws, **p: _open_db(ws, "jwt", **p))


# ── /tuned: the same reactor, with per-connection knobs ─────────────────────
#
# `register()` deliberately exposes nothing per connection — a consumer picks a
# `connect` factory and that is the whole contract. The conformance suite needs
# three things that contract has no room for, so this route calls the pump
# directly instead of going through the adapter:
#
#     ?catalogs=db,other     serve several catalogs on one socket (multiplexed by `ns`)
#     ?ping_interval_ms=200  override the keepalive, which is otherwise process-wide
#                            ($N6K_PING_INTERVAL, default 25s) and far too slow to assert on
#     ?require_token=secret  an arbitrary credential, checked inside the HELLO frame
#
# It is still a pure byte pump — no protocol logic — and registers into the same
# `_live` map as every other mount, so the `/debug/*` control plane above reaches it.


def _parse_catalogs(raw: Optional[str]) -> list[str]:
    """Catalog names from `?catalogs=`, defaulting to the fixture catalog.

    Interpolated as SQL identifiers when the prelude runs, so anything but a bare
    alphanumeric name is refused here rather than passed along.
    """
    names = [n.strip() for n in (raw or "db").split(",") if n.strip()]
    for name in names:
        if not name.replace("_", "").isalnum():
            raise ValueError(f"invalid catalog name {name!r}")
    if len(set(names)) != len(names):
        raise ValueError("duplicate catalog name")
    return names or ["db"]


def _parse_ping_interval_ms(raw: Optional[str]) -> Optional[int]:
    """`?ping_interval_ms=` as an int, or None when the query param is absent."""
    if raw is None:
        return None
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"invalid ping_interval_ms {raw!r}")
    if value < 0:
        raise ValueError("ping_interval_ms must not be negative")
    return value


def _seed_tuned_con(catalogs: list[str]) -> duckdb.DuckDBPyConnection:
    """A fresh connection with every named catalog attached, seeded and RPC-ready.

    The tables come from the same `fixtures.sql` the other mounts use, so a client
    that works against those works here unchanged. On top of it each catalog gets a
    `main.marker` row naming itself — a cheap way to see, in the data, whether a
    session ever reads the wrong catalog.
    """
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    load_n6k(con)
    load_n6k_server(con)
    load_n6k_testing(con)
    for name in catalogs:
        con.execute(f"ATTACH ':memory:' AS {name}")
        for statement in fixture_statements(name):
            con.execute(statement)
        con.execute(f"CREATE TABLE {name}.main.marker (catalog VARCHAR)")
        con.execute(f"INSERT INTO {name}.main.marker VALUES ('{name}')")
        for statement in rpc_fixture_statements(name):
            con.execute(statement)
        # Every served catalog gets them, so a multiplexed session can call a host RPC on either.
        register_host_rpcs(con, name)
    return con


def _register_literal_token_auth(con: duckdb.DuckDBPyConnection, expected: str) -> None:
    """Register the verifier the reactor calls for every FT_HELLO, accepting `expected`.

    Raising is how the answer carries a reason: the message becomes the HELLO_ERR's.
    """

    def _check(token: Optional[str], catalog: Optional[str]) -> bool:
        if token != expected:
            raise ValueError(f"bad token for catalog {catalog!r}")
        return True

    varchar = duckdb.sqltype("VARCHAR")
    con.create_function(AUTH_FUNCTION, _check, [varchar, varchar], duckdb.sqltype("BOOLEAN"))


@app.websocket("/tuned/ws")
async def _tuned_ws(ws: WebSocket) -> None:
    await ws.accept()
    try:
        catalogs = _parse_catalogs(ws.query_params.get("catalogs"))
        ping_interval_ms = _parse_ping_interval_ms(ws.query_params.get("ping_interval_ms"))
    except ValueError as exc:
        await ws.close(code=4400, reason=str(exc))
        return

    require_token = ws.query_params.get("require_token")
    con = _seed_tuned_con(catalogs)
    if require_token is not None:
        _register_literal_token_auth(con, require_token)

    key = id(ws)
    # The side cursor must be taken before the serving CALL claims `con` — after that
    # this is the only way in, and duckdb serializes per connection.
    _live[key] = _Live(ws=ws, side=con.cursor(), catalogs=catalogs)
    try:
        await serve_and_close_connection(
            ws,
            con,
            catalogs,
            ping_interval_ms=ping_interval_ms,
            auth_function=AUTH_FUNCTION if require_token is not None else None,
        )
    except WebSocketDisconnect:
        pass
    except Exception as exc:  # noqa: BLE001 - surface the reason, don't hang the client
        logger.exception("tuned ws failed")
        try:
            await ws.close(code=1011, reason=str(exc)[:120])
        except RuntimeError:
            pass
    finally:
        _live.pop(key, None)


# CORS so a cross-origin-isolated browser page (served from an ephemeral port by
# the browser test harness) can reach this server (the WS upgrade and /debug).
# Added last → outermost middleware → handles the OPTIONS preflight before the
# auth/count middleware. A successful CORS response also satisfies the page's
# COEP: require-corp.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
