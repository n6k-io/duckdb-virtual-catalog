"""WS auth handshake (FT_HELLO) against a token-protected mount.

Browsers cannot set headers on a WS upgrade, so rather than leak the token in
the URL (`?token=`, which lands in access logs) they send it in an FT_HELLO
frame once the socket is open. These tests drive that path directly with the
`websockets` client — no DuckDB extension needed — and also assert the retained
header path and the deprecated `?token=` fallback still authenticate.

Targets the always-on `/auth` mount on the running :8099 test server (static
bearer token == AUTH_TOKEN); no server is spawned.

Run:
    uv run pytest integration_tests/test_ws_handshake_auth.py -v
"""

import urllib.error
import urllib.request

import pytest
import websockets

from n6k_protocol.engine import pack_frame, unpack_frame
from n6k_protocol.protocol import (  # noqa: E402
    FT_ERR,
    FT_HELLO,
    FT_HELLO_ACK,
    FT_HELLO_ERR,
    FT_REQ,
    N6K_PROTOCOL_VERSION,
    OP_CATALOG_LIST,
)

SERVER_PORT = 8099
SERVER_URL = f"http://localhost:{SERVER_PORT}"
# The static token the /auth prefix requires (test_server/app.py AUTH_TOKEN).
AUTH_TOKEN = "n6k-test-token"
# WS endpoint of the token-gated mount.
WS_URL = f"ws://127.0.0.1:{SERVER_PORT}/auth/ws?catalog=db"


@pytest.fixture(autouse=True)
def check_server():
    try:
        urllib.request.urlopen(f"{SERVER_URL}/debug/counts", timeout=2)
    except (urllib.error.URLError, ConnectionRefusedError, OSError):
        pytest.skip("Test server not running on port 8099")


def _hello_frame(token: str) -> bytes:
    return pack_frame({"t": FT_HELLO, "token": token})


def _close_code(exc: websockets.exceptions.ConnectionClosed) -> int | None:
    return exc.rcvd.code if exc.rcvd is not None else None


async def _recv_hello_ack(ws) -> dict:
    raw = await ws.recv()
    assert not isinstance(raw, str), f"expected binary frame, got text: {raw!r}"
    header, _ = unpack_frame(raw)
    assert header["t"] == FT_HELLO_ACK, f"expected HELLO_ACK, got {header['t']}"
    assert "id" not in header  # connection-scoped
    return header


async def _recv_hello_err(ws) -> dict:
    """A connection-scoped rejection sends a HELLO_ERR frame before the close,
    so the driver surfaces the reason instead of a bare "socket closed"."""
    raw = await ws.recv()
    assert not isinstance(raw, str), f"expected binary frame, got text: {raw!r}"
    header, _ = unpack_frame(raw)
    assert header["t"] == FT_HELLO_ERR, f"expected HELLO_ERR, got {header['t']}"
    assert "id" not in header  # connection-scoped
    assert header["exception_message"]
    return header


@pytest.mark.asyncio
async def test_handshake_valid_token_authenticates():
    async with websockets.connect(WS_URL) as ws:
        await ws.send(_hello_frame(AUTH_TOKEN))
        hello = await _recv_hello_ack(ws)
        assert hello["protocol_version"] == N6K_PROTOCOL_VERSION


@pytest.mark.asyncio
async def test_handshake_bad_token_is_rejected():
    """A bad credential gets HELLO_ERR and then the connection ends.

    The close code is no longer 4401. Auth moved into the reactor — it reads the
    HELLO and calls the host's verifier — so the process that owns the WebSocket no
    longer knows *why* the session was refused, only that serving finished. The
    reason still reaches the client, and more richly, in the HELLO_ERR frame that
    `_recv_hello_err` just read.
    """
    async with websockets.connect(WS_URL) as ws:
        await ws.send(_hello_frame("wrong-token"))
        await _recv_hello_err(ws)
        with pytest.raises(websockets.exceptions.ConnectionClosed):
            await ws.recv()


@pytest.mark.asyncio
async def test_non_hello_first_frame_is_rejected():
    """A client that skips the handshake and sends a REQ first must be refused.

    The refusal is a request-scoped FT_ERR carrying that request's id, not a
    connection-scoped HELLO_ERR. That is the better answer to a REQ: HELLO_ERR has no
    `id`, so a client waiting on request 1 would never see its reply resolve. What
    matters here is that no session exists to serve it — which the error says.
    """
    async with websockets.connect(WS_URL) as ws:
        await ws.send(pack_frame({"t": FT_REQ, "id": 1, "op": OP_CATALOG_LIST}))
        header, _ = unpack_frame(await ws.recv())
        assert header["t"] == FT_ERR, f"expected FT_ERR, got {header['t']}"
        assert header.get("id") == 1
        assert "hello" in header["exception_message"].lower()


@pytest.mark.asyncio
async def test_header_auth_still_works():
    # Native clients keep authenticating via the Authorization header — the
    # server sends HELLO_ACK without waiting for a handshake frame.
    async with websockets.connect(
        WS_URL,
        additional_headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
    ) as ws:
        hello = await _recv_hello_ack(ws)
        assert hello["protocol_version"] == N6K_PROTOCOL_VERSION


@pytest.mark.asyncio
async def test_deprecated_query_token_still_works():
    # The ?token= query param remains accepted (deprecated) for older clients.
    async with websockets.connect(f"{WS_URL}&token={AUTH_TOKEN}") as ws:
        hello = await _recv_hello_ack(ws)
        assert hello["protocol_version"] == N6K_PROTOCOL_VERSION
