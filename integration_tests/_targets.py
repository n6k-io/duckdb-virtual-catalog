"""Server targets for the wire-conformance tests, and the raw client that drives them.

Every n6k server implementation must answer the same frames the same way. These tests
prove that by speaking the protocol directly — `websockets` + `pack_frame`/`unpack_frame`,
no DuckDB extension and no client-side code — so they are implementation-neutral by
construction and can be pointed at anything that serves the wire.

`TARGETS` is the list of those implementations, each mounted at a different path on the
one test server (`uv run test-server`). Adding a new server means adding one `Target`
here; every test parameterized over `TARGETS` then covers it. That is the point of this
module: the alternative is what the suite grew into first, where each file hard-coded
its own mount paths, its own port probe, and its own copy of the request client.

`Target.supports` records where an implementation is deliberately, permanently narrower
than the protocol — not where it is merely unfinished. A test skips on a missing
capability rather than being deleted, so the gap stays visible in the test report.
"""

import io
import os
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Optional

import pyarrow as pa
import pyarrow.ipc as ipc
import websockets

from n6k_protocol.engine import pack_frame, unpack_frame
from n6k_protocol.protocol import (
    FT_ERR,
    FT_HELLO_ACK,
    FT_READY,
    FT_REQ,
    FT_RESP_CHUNK,
    FT_RESP_END,
    FT_RESP_SCHEMA,
)

SERVER_HOST = os.environ.get("N6K_TEST_HOST", "127.0.0.1")
SERVER_PORT = int(os.environ.get("N6K_TEST_PORT", "8099"))
SERVER_URL = f"http://{SERVER_HOST}:{SERVER_PORT}"

# The catalog every target seeds from fixtures.sql, and therefore the one the
# cross-implementation comparisons address.
FIXTURE_CATALOG = "db"

# Capability names used in `Target.supports`.
CAP_RPC = "rpc"
CAP_AUTH = "auth"
CAP_DEFERRED_SESSION = "deferred_session"


@dataclass(frozen=True)
class Target:
    """One server implementation, addressed by the path it is mounted at."""

    id: str
    mount: str
    implementation: str
    supports: frozenset[str] = field(default_factory=frozenset)

    def ws_url(self, catalog: Optional[str] = FIXTURE_CATALOG, **query: str) -> str:
        url = f"ws://{SERVER_HOST}:{SERVER_PORT}{self.mount}/ws"
        params = dict(query)
        if catalog is not None:
            params.setdefault("catalog", catalog)
        if params:
            url += "?" + "&".join(f"{k}={v}" for k, v in params.items())
        return url

    def __str__(self) -> str:
        return self.id


# The root mount, reached through the shipped `register()` adapter — and, since the Python
# engine that used to back a second one is gone, the only implementation there is. `TARGETS`
# is a list of one on purpose: the parametrization is what makes adding the next server a
# one-line change here rather than an edit to every test that must then cover it.
REGISTER = Target(
    id="register",
    mount="",
    implementation="server_fastapi.register(connect=...) -> src/n6k_server",
    supports=frozenset({CAP_RPC, CAP_AUTH}),
)

TARGETS = [REGISTER]

# Not a target: the same reactor behind the same pump, mounted separately only because the
# conformance suite needs per-connection knobs the shipped adapter deliberately has no room
# for (`?catalogs=`, `?ping_interval_ms=`, `?require_token=`). Tests that exercise those use
# it directly; nothing about the wire differs, so parametrizing over it would prove nothing.
TUNED = Target(
    id="tuned",
    mount="/tuned",
    implementation="test_server.app:_tuned_ws -> src/n6k_server",
    supports=frozenset({CAP_RPC, CAP_AUTH}),
)


def server_is_up(timeout: float = 2.0) -> bool:
    """True if the test server answers on its debug endpoint."""
    try:
        urllib.request.urlopen(f"{SERVER_URL}/debug/counts", timeout=timeout)
    except (urllib.error.URLError, ConnectionRefusedError, OSError):
        return False
    return True


class WireClient:
    """A raw n6k protocol client: one request id space, frames in and out.

    Wraps an open WebSocket. Request ids are allocated monotonically from 1, matching
    the client contract (id 0 is reserved for the multiplexed session-close CANCEL).
    """

    def __init__(self, ws: Any) -> None:
        self.ws = ws
        self._next_id = 1

    async def send(self, op: int, body: bytes = b"", **args: Any) -> int:
        """Send one REQ. `body` is the raw payload appended after the header — Arrow IPC
        for the ops that push rows (INSERT, RPC_TABLE), empty for everything else."""
        req_id = self._next_id
        self._next_id += 1
        await self.ws.send(pack_frame({"t": FT_REQ, "id": req_id, "op": op, **args}, body))
        return req_id

    async def drain(self, req_id: int) -> list[tuple[dict[str, Any], bytes]]:
        """Collect frames until the terminal one for `req_id` (RESP_END or ERR)."""
        frames: list[tuple[dict[str, Any], bytes]] = []
        while True:
            header, body = unpack_frame(await self.ws.recv())
            frames.append((header, bytes(body)))
            if header.get("id") == req_id and header["t"] in (FT_RESP_END, FT_ERR):
                return frames

    async def request(self, op: int, body: bytes = b"", **args: Any) -> list[tuple[dict[str, Any], bytes]]:
        return await self.drain(await self.send(op, body, **args))

    async def table(self, op: int, body: bytes = b"", **args: Any) -> pa.Table:
        """Run a streaming op and reassemble its Arrow IPC stream.

        Concatenating the RESP_SCHEMA body with every RESP_CHUNK body yields a valid
        IPC stream regardless of where the server put its message boundaries — which
        is exactly the framing latitude the protocol grants (see the Arrow message
        framing section of docs/n6k-network-protocol.md).
        """
        frames = await self.request(op, body, **args)
        errs = [h for h, _ in frames if h["t"] == FT_ERR]
        assert not errs, f"unexpected ERR frame: {errs}"
        schema = [b for h, b in frames if h["t"] == FT_RESP_SCHEMA][0]
        chunks = [b for h, b in frames if h["t"] == FT_RESP_CHUNK]
        return ipc.open_stream(io.BytesIO(schema + b"".join(chunks))).read_all()

    async def error(self, op: int, body: bytes = b"", **args: Any) -> dict[str, Any]:
        """Run an op expected to fail and return the ERR header."""
        frames = await self.request(op, body, **args)
        errs = [h for h, _ in frames if h["t"] == FT_ERR]
        assert errs, f"expected an ERR frame, got {[h['t'] for h, _ in frames]}"
        return errs[0]


class connect:
    """Async context manager yielding `(WireClient, hello_ack_header)` for a target.

    Absorbs the deferred-build handshake: a server that acks with `session_pending`
    sends FT_READY once the session is live, and the first request must wait for it.
    A server that builds eagerly sends neither, so both shapes arrive here as a
    ready-to-use client.
    """

    def __init__(self, target: Target, catalog: Optional[str] = FIXTURE_CATALOG, **query: str) -> None:
        self._url = target.ws_url(catalog, **query)
        self._ws: Any = None

    async def __aenter__(self) -> tuple[WireClient, dict[str, Any]]:
        self._ws = await websockets.connect(self._url)
        header, _ = unpack_frame(await self._ws.recv())
        assert header["t"] == FT_HELLO_ACK, header
        hello = dict(header)
        if header.get("session_pending"):
            ready, _ = unpack_frame(await self._ws.recv())
            assert ready["t"] == FT_READY, ready
            hello.setdefault("capabilities", ready.get("capabilities", []))
        return WireClient(self._ws), hello

    async def __aexit__(self, *exc: Any) -> None:
        await self._ws.close()
