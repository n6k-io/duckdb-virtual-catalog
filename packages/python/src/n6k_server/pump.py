"""Serve a WebSocket by handing it to the C++ reactor and shuttling bytes.

This module is the whole of Python's involvement in a served connection. It
imports nothing from `n6k_protocol`, and deliberately so: it never decodes a
frame, never inspects an op, and cannot tell a SCAN from a PING. The protocol
lives in `src/n6k_server`; this is transport.

The mechanism is a `socketpair()`. Python keeps one end and passes the *number*
of the other to SQL — `CALL n6k_serve_fd(<fd>, 'db')` — which the extension
`dup()`s and then reads and writes like any socket. The same trick the client
side already uses for `ATTACH … (wsFd <n>)`.

In-process rather than a spawned `duckdb`, because the reactor's workers open
connections on *this* `DatabaseInstance`. That is what lets a table backed by
`n6k_server.provider` — whose rows come from Python callbacks marshalled to this
event loop — be served at all. A subprocess would have its own database and no
interpreter to call back into. `duckdb` releases the GIL for the whole of
`execute()`, so the blocking `CALL` parks a thread without starving the loop.
"""

from __future__ import annotations

import asyncio
import socket
import struct
from typing import Any, Optional, Protocol

import duckdb

# Bounds a corrupt length prefix from the extension; far above any real frame.
MAX_FRAME_BYTES = 1 << 30
# How long to wait for the serving thread to unwind after the socket drops.
SERVE_JOIN_TIMEOUT_S = 5.0


class WsReject(Exception):
    """Raise from a `connect` factory to close the WebSocket with a specific code.

    Transport-level, which is why it lives here rather than with the protocol: the
    codes are RFC 6455's (4400 bad request, 4401 unauthorized, 4404 not found) and
    the rejection happens before any frame is exchanged.
    """

    def __init__(self, code: int = 4400, reason: str = "") -> None:
        super().__init__(reason or f"ws reject {code}")
        self.code = code
        self.reason = reason


class WebSocketLike(Protocol):
    """The transport surface the pump needs, so this module imports no framework."""

    async def receive_bytes(self) -> bytes: ...

    async def send_bytes(self, data: bytes) -> None: ...

    async def close(self, code: int = 1000, reason: str = "") -> None: ...


def build_serve_sql(
    fd: int,
    catalogs: Optional[list[str]] = None,
    ping_interval_ms: Optional[int] = None,
    auth_function: Optional[str] = None,
) -> str:
    """`CALL n6k_serve_fd(...)` for one connection.

    `catalogs=None` — the normal case — names none, and the extension picks: every
    attached catalog bar the startup in-memory database, exactly as the zero-argument
    `n6k_serve_socket()` and `n6k_serve_http()` do. One rule, in one place. Pass a list
    only to serve a *subset* of what is attached.

    Any names passed are interpolated as SQL string literals, so a caller that passes
    them must have validated them; nothing a client can influence reaches here.
    """
    args = [str(fd)]
    args += [f"'{name}'" for name in catalogs or []]
    if ping_interval_ms is not None:
        args.append(f"ping_interval_ms => {ping_interval_ms}")
    if auth_function:
        args.append(f"auth_function => '{auth_function}'")
    return f"CALL n6k_serve_fd({', '.join(args)})"


async def _pump(
    ws: WebSocketLike,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    serve: "asyncio.Task[Any]",
) -> None:
    """Shuttle frames until either side stops. WS message <-> length-prefixed record.

    `serve` is watched alongside the two directions so a CALL that fails outright —
    a rejected fd, an extension that did not load — ends the connection with that
    error instead of leaving both directions blocked on a peer that will never speak.
    """

    async def ws_to_sock() -> None:
        while True:
            try:
                message = await ws.receive_bytes()
            except Exception:
                # A normal client close raises here (starlette turns the disconnect into
                # WebSocketDisconnect). That is an ordinary end of stream, not a failure —
                # letting it propagate leaves an unretrieved task exception behind.
                return
            writer.write(struct.pack(">I", len(message)) + message)
            await writer.drain()

    async def sock_to_ws() -> None:
        while True:
            try:
                header = await reader.readexactly(4)
            except asyncio.IncompleteReadError:
                return  # the extension closed
            (n,) = struct.unpack(">I", header)
            if n > MAX_FRAME_BYTES:
                return
            await ws.send_bytes(await reader.readexactly(n))

    mine = {asyncio.create_task(ws_to_sock()), asyncio.create_task(sock_to_ws())}
    done, pending = await asyncio.wait(mine | {serve}, return_when=asyncio.FIRST_COMPLETED)
    for task in pending & mine:
        task.cancel()
    await asyncio.gather(*(pending & mine), return_exceptions=True)
    # Re-raise a CALL that died on its own; a finished pump direction is just EOF.
    if serve in done:
        serve.result()


async def _serve_exited_within_timeout(serve: "Optional[asyncio.Task[Any]]") -> bool:
    """Wait up to `SERVE_JOIN_TIMEOUT_S` for the serving CALL to unwind. A CALL that
    raised on the way out still counts as exited — the real error is already being
    reported elsewhere."""
    if serve is None:
        return True
    try:
        await asyncio.wait_for(asyncio.shield(serve), timeout=SERVE_JOIN_TIMEOUT_S)
    except asyncio.TimeoutError:
        return False
    except Exception:  # noqa: BLE001 - teardown must not raise over the real error
        pass
    return True


async def serve_and_close_connection(
    ws: WebSocketLike,
    con: duckdb.DuckDBPyConnection,
    catalogs: Optional[list[str]] = None,
    *,
    ping_interval_ms: Optional[int] = None,
    auth_function: Optional[str] = None,
) -> None:
    """Serve `ws` from `con` until either side drops, then close `con`.

    Everything `con` deliberately attached is served unless `catalogs` narrows it —
    the extension decides which those are (`build_serve_sql`) — and if `auth_function`
    is named that function must already be registered on `con`.

    Ownership passes here: the serving CALL holds `con` for the connection's life,
    so nothing else may touch it. A caller that needs to reach the data meanwhile
    must take a `con.cursor()` — an independent connection on the same database —
    *before* calling this.
    """
    host_sock, ext_sock = socket.socketpair()
    serve: Optional[asyncio.Task[Any]] = None
    writer: Optional[asyncio.StreamWriter] = None
    transport_owns_host_sock = False
    try:
        # The extension dup()s the fd, but not until the CALL reaches Adopt on the worker
        # thread — so ext_sock stays open for the whole session rather than being closed
        # straight after the call is issued.
        sql = build_serve_sql(ext_sock.fileno(), catalogs, ping_interval_ms, auth_function)
        serve = asyncio.create_task(asyncio.to_thread(con.execute, sql))

        reader, writer = await asyncio.open_connection(sock=host_sock)
        transport_owns_host_sock = True
        await _pump(ws, reader, writer, serve)
    finally:
        # Closing our end EOFs the extension's, which ends its reader loop and returns the
        # CALL. `close()` only *starts* that — the fd is still open when it returns — so the
        # wait is required: without it the serving thread sees no EOF and every teardown
        # burns the full join timeout instead of finishing in microseconds.
        if writer is not None:
            if not writer.is_closing():
                writer.close()
            try:
                await writer.wait_closed()
            except Exception:  # noqa: BLE001 - a broken transport is already closed enough
                pass
        if not transport_owns_host_sock:
            host_sock.close()
        exited = await _serve_exited_within_timeout(serve)
        ext_sock.close()
        # Close the WebSocket properly rather than letting the handler return and the socket
        # drop. The reactor ends a connection itself in some cases — a rejected credential,
        # for one — and without this the client sees a truncated stream with no close frame
        # instead of a clean shutdown, which is indistinguishable from a network failure.
        try:
            await ws.close()
        except Exception:  # noqa: BLE001 - already closed by the peer, or mid-close
            pass
        # A thread that outlived the timeout still owns `con`; closing it underneath would race.
        if exited:
            con.close()
