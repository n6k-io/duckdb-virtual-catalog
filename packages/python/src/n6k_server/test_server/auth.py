"""Auth for the test server.

Two modes:
- Static bearer token via `set_expected_token(token)`.
- JWT verification via `set_jwt_verifier(fn)` — `fn(token)`
  returns True for a valid (signature + exp + aud) data-service JWT.

Both accept the credential from the `Authorization: Bearer` header or, for WS
upgrades where browsers cannot set headers, the `?token=` query param. `None`
(neither configured) disables auth.
"""

import logging
from typing import Callable, Optional

from fastapi import Request, WebSocket

logger = logging.getLogger("n6k_server.test_server.auth")

_expected_token: Optional[str] = None
_jwt_verifier: Optional[Callable[[str], bool]] = None


def set_expected_token(token: Optional[str]) -> None:
    global _expected_token
    _expected_token = token


def set_jwt_verifier(verifier: Optional[Callable[[str], bool]]) -> None:
    global _jwt_verifier
    _jwt_verifier = verifier


def is_auth_enabled() -> bool:
    return _expected_token is not None or _jwt_verifier is not None


def _bearer(header_value: Optional[str]) -> Optional[str]:
    if header_value and header_value.startswith("Bearer "):
        return header_value[len("Bearer ") :]
    return None


async def ws_present_token(ws: WebSocket) -> Optional[str]:
    """The credential a WS upgrade presents at the transport level.

    Header or `?token=` only. The third source — the FT_HELLO frame, which is where
    a browser must put its token since it cannot set a header on a WS upgrade — is
    no longer read here: reading it means parsing a frame, and frames belong to the
    C++ reactor. That case is handled by the reactor's `auth_function`, which calls
    back into Python with the token it read (see `app._register_ws_auth`).
    """
    return _bearer(ws.headers.get("authorization")) or ws.query_params.get("token")


def http_present_token(request: Request) -> Optional[str]:
    """The credential an HTTP request presents (Bearer header or `?token=`)."""
    return _bearer(request.headers.get("authorization")) or request.query_params.get("token")


def is_authorized_http(request: Request) -> bool:
    if _jwt_verifier is not None:
        token = http_present_token(request)
        return bool(token and _jwt_verifier(token))
    if _expected_token is None:
        return True
    return request.headers.get("authorization") == f"Bearer {_expected_token}"
