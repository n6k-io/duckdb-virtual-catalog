"""The native `wsFd` ATTACH option: ride a socket the host already connected and
handshook, instead of letting the extension dial the server itself.

The host (here, this test) owns the WebSocket end-to-end:
  1. socketpair() -> (py_sock, ext_sock)
  2. open the real WebSocket to the server (a *bare* /ws — no ?catalog=) and let
     it complete the upgrade. The catalog is not in the URL; the extension's
     FdTransport sends an FT_HELLO frame carrying catalog=db through the pump, and
     the server resolves the catalog from it before HELLO_ACK.
  3. two daemon threads pump *length-framed* n6k frames between ws <-> py_sock
     (4-byte big-endian length prefix per frame — the same wire convention as
     src/n6k_server/uds_transport.cpp)
  4. ATTACH '' AS db (TYPE n6k, wsFd <ext_sock.fileno()>)

This is the headline of catalog-via-HELLO: a catalog-agnostic host dials a bare
/ws and the extension declares the catalog (the ATTACH alias `db`) in the
handshake frame.

The extension dup()s the fd, so we close our ext_sock right after handing it off;
its dup is then the sole owner of that socket endpoint, and DETACH makes py_sock
observe EOF.

Runs against the always-on test server on :8099 (open root mount, catalog `db`
seeded with main.users == 3 rows). We never spawn a server here.

Run:
    uv run pytest integration_tests/test_wsfd_attach.py -v
"""

import os
import socket
import struct
import threading
import urllib.error
import urllib.request

import duckdb
import pytest
from websockets.sync.client import connect as ws_connect

from _paths import EXT_PATH

SERVER_PORT = 8099
SERVER_URL = f"http://localhost:{SERVER_PORT}"
# `?catalog=` is required now. It used to be omissible here because the catalog could
# ride the extension's FT_HELLO frame, but the host in front of the server no longer
# reads frames — it has to ATTACH the catalog before handing the socket over, so it
# needs the name from the URL. The extension still sends it in HELLO as well, and both
# real clients already put it in the URL too.
WS_URL = f"ws://localhost:{SERVER_PORT}/ws?catalog=db"


@pytest.fixture(autouse=True)
def skip_if_no_extension():
    if not os.path.exists(EXT_PATH):
        pytest.skip(f"Extension not built: {EXT_PATH}")


@pytest.fixture(autouse=True)
def check_server():
    try:
        urllib.request.urlopen(f"{SERVER_URL}/debug/counts", timeout=2)
    except (urllib.error.URLError, ConnectionRefusedError, OSError):
        pytest.skip("Test server not running on port 8099")


def _connect():
    c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    c.load_extension("httpfs")
    c.load_extension(EXT_PATH)
    return c


def _recv_exactly(sock, n):
    """Read exactly n bytes; return None on EOF/error (peer closed)."""
    chunks = []
    got = 0
    while got < n:
        try:
            b = sock.recv(n - got)
        except OSError:
            return None
        if not b:
            return None
        chunks.append(b)
        got += len(b)
    return b"".join(chunks)


def _start_pump(ws, sock):
    """Pump length-framed n6k frames between the WebSocket and the raw socket.

    ws  -> sock: each binary WS message becomes [4-byte BE len][message].
    sock -> ws : read [4-byte BE len] then that many bytes, send as one WS message.

    Returns the (ws->sock, sock->ws) threads so a test can join on the second one
    to observe the extension closing its fd.
    """

    def ws_to_sock():
        try:
            for msg in ws:
                if isinstance(msg, str):
                    msg = msg.encode()
                sock.sendall(struct.pack(">I", len(msg)) + msg)
        except Exception:
            pass
        finally:
            try:
                sock.shutdown(socket.SHUT_WR)
            except OSError:
                pass

    def sock_to_ws():
        try:
            while True:
                header = _recv_exactly(sock, 4)
                if header is None:
                    break
                (n,) = struct.unpack(">I", header)
                body = _recv_exactly(sock, n)
                if body is None:
                    break
                ws.send(body)
        except Exception:
            pass
        finally:
            try:
                ws.close()
            except Exception:
                pass

    t_out = threading.Thread(target=ws_to_sock, daemon=True)
    t_in = threading.Thread(target=sock_to_ws, daemon=True)
    t_out.start()
    t_in.start()
    return t_out, t_in


def _attach_over_wsfd(c, alias="db"):
    """Open the WS, start the pump, and ATTACH over the resulting fd.

    Returns (py_sock, ws, sock_to_ws_thread). Closes ext_sock once the extension
    has dup()'d it.
    """
    py_sock, ext_sock = socket.socketpair()
    ws = ws_connect(WS_URL)
    _t_out, t_in = _start_pump(ws, py_sock)
    c.execute(f"ATTACH '' AS {alias} (TYPE n6k, wsFd {ext_sock.fileno()})")
    # The extension dup()'d the fd during ATTACH; drop our copy so its dup solely
    # owns the endpoint and DETACH will make py_sock observe EOF.
    ext_sock.close()
    return py_sock, ws, t_in


def test_wsfd_attaches_and_queries():
    c = _connect()
    py_sock, ws, _t_in = _attach_over_wsfd(c)
    try:
        assert c.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3
        # A second statement proves the fd transport multiplexes more than the
        # attach-time catalog probe.
        names = [r[0] for r in c.execute("SELECT name FROM db.main.users ORDER BY name").fetchall()]
        assert len(names) == 3
    finally:
        c.close()
        ws.close()
        py_sock.close()


def test_wsfd_detach_closes_socket():
    c = _connect()
    py_sock, ws, t_in = _attach_over_wsfd(c)
    try:
        # Live before detach.
        assert c.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3
        c.execute("DETACH db")
        # DETACH drops the WsClient, which closes the extension's dup of the fd.
        # That was the last owner of the endpoint, so the pump's reader on py_sock
        # hits EOF and the sock->ws thread exits — proving the ownership semantics.
        t_in.join(timeout=5)
        assert not t_in.is_alive(), "extension did not close its fd on DETACH"
    finally:
        c.close()
        ws.close()
        py_sock.close()


def test_wsfd_mutually_exclusive_with_url():
    c = _connect()
    py_sock, ext_sock = socket.socketpair()
    try:
        with pytest.raises(duckdb.Error, match="mutually exclusive"):
            c.execute(f"ATTACH 'n6k://localhost:{SERVER_PORT}' AS db (TYPE n6k, wsFd {ext_sock.fileno()})")
    finally:
        ext_sock.close()
        py_sock.close()
        c.close()
