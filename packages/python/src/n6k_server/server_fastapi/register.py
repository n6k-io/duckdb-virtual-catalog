"""Mount the n6k WebSocket endpoint on a FastAPI app.

    register(app, "/db/{tenant}", connect=open_db)

Mounts `<prefix>/ws`. `connect` is called once per WebSocket and returns the
DuckDB connection to serve it from — that is the whole contract:

    def open_db(ws, tenant):
        con = duckdb.connect()
        load_n6k_server(con)
        con.execute(f'ATTACH \\':memory:\\' AS "{tenant}"')
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
  (`con.create_function`). The extension looks for it itself and, if it exists,
  calls it for every FT_HELLO and refuses the session on `False`; raising sends
  your message to the client.
  That covers the credential a *browser* must send, which rides inside the
  handshake frame because a browser cannot set a header on a WS upgrade — and
  reading it here would mean parsing frames.

Registering the function is the whole opt-in; there is no auth setting.
"""

import inspect
import logging
import time
from typing import Any, AsyncIterator, Awaitable, Callable, Union

import duckdb
from fastapi import APIRouter, FastAPI, WebSocket, WebSocketDisconnect

from n6k_server.pump import WsReject, serve_and_close_connection

logger = logging.getLogger("n6k_server.server_fastapi.register")

Connect = Callable[
    ...,
    Union[
        duckdb.DuckDBPyConnection,
        Awaitable[duckdb.DuckDBPyConnection],
        AsyncIterator[duckdb.DuckDBPyConnection],
    ],
]


def register(app: FastAPI, prefix: str, *, connect: Connect) -> APIRouter:
    """Mount `<prefix>/ws` on `app` and return the router.

    `prefix` follows FastAPI's convention: empty for root, else starts with `/` and
    does not end with `/`. Path params it captures are passed to `connect` as
    keyword arguments, after the WebSocket.
    """
    if prefix and not prefix.startswith("/"):
        raise ValueError(f"prefix must be empty or start with '/', got {prefix!r}")
    if prefix.endswith("/"):
        raise ValueError(f"prefix must not end with '/' (FastAPI convention), got {prefix!r}")

    router = APIRouter(prefix=prefix)

    @router.websocket("/ws")
    async def _ws(ws: WebSocket) -> None:
        await ws.accept()
        started = time.perf_counter()
        params = {str(k): str(v) for k, v in ws.path_params.items()}
        logger.info("ws accept path=%s params=%s", ws.url.path, params)
        try:
            await _serve(ws, connect, params)
        except WsReject as reject:
            await _close(ws, reject.code, reject.reason)
        except WebSocketDisconnect:
            pass
        except Exception as exc:  # noqa: BLE001 - surface the reason rather than hang the client
            logger.exception("ws failed path=%s", ws.url.path)
            await _close(ws, 1011, str(exc))
        finally:
            logger.info("ws close path=%s duration_ms=%.1f", ws.url.path, (time.perf_counter() - started) * 1000)

    app.include_router(router)
    return router


async def _serve(ws: WebSocket, connect: Connect, params: dict[str, str]) -> None:
    """Open the connection `connect` describes, serve it, and let it clean up.

    Past `serve_and_close_connection` nothing here touches `ws.receive()`: the socket belongs
    to the extension and this coroutine only shuttles bytes. That is the line, and
    it is why this module imports no frame codec.
    """
    opened: Any = connect(ws, **params)

    if inspect.isasyncgen(opened):
        con = await opened.__anext__()
        try:
            await serve_and_close_connection(ws, con)
        finally:
            # Resumes the generator past its `yield` so its `finally` runs. It must not
            # yield again — a second connection has nothing to be served on.
            await opened.aclose()
        return

    con = await opened if inspect.isawaitable(opened) else opened
    await serve_and_close_connection(ws, con)


async def _close(ws: WebSocket, code: int, reason: str) -> None:
    """Close with a reason, truncated to the 123 bytes RFC 6455 allows."""
    try:
        await ws.close(code=code, reason=reason[:123])
    except RuntimeError:
        pass  # already closed
