"""`register()` mounts `<prefix>/ws` on a FastAPI app via APIRouter.

Covers prefix validation and the mounted path. Nothing else is decided here: the
extension picks the served catalogs (`n6k::ResolveServedCatalogs`, asserted by
`test/sql/n6k_server.test`) and finds the `n6k_authorize` verifier itself
(`n6k_serve_function.cpp`, exercised over the wire by
`integration_tests/test_ws_handshake_auth.py`).
"""

from typing import Any

import pytest
from fastapi import FastAPI

from n6k_server.server_fastapi.register import register


def _connect(ws: Any, **_: Any) -> Any:
    """Never called: these tests assert on registration, not on serving."""
    raise AssertionError("connect should not run during registration")


def _paths(app: FastAPI) -> list[str]:
    return [r.path for r in app.routes if hasattr(r, "path")]


def test_prefix_must_not_end_with_slash():
    app = FastAPI()
    with pytest.raises(ValueError):
        register(app, prefix="/foo/", connect=_connect)


def test_non_empty_prefix_must_start_with_slash():
    app = FastAPI()
    with pytest.raises(ValueError):
        register(app, prefix="foo", connect=_connect)


def test_empty_prefix_mounts_at_root():
    app = FastAPI()
    register(app, prefix="", connect=_connect)
    assert "/ws" in _paths(app)


def test_prefix_with_path_param():
    app = FastAPI()
    register(app, prefix="/mydb/{catalog}", connect=_connect)
    assert "/mydb/{catalog}/ws" in _paths(app)


def test_connect_is_required():
    app = FastAPI()
    with pytest.raises(TypeError):
        register(app, prefix="/nope")


def test_two_registrations_are_independent():
    app = FastAPI()
    register(app, prefix="/a", connect=_connect)
    register(app, prefix="/b", connect=_connect)
    paths = _paths(app)
    assert "/a/ws" in paths
    assert "/b/ws" in paths
