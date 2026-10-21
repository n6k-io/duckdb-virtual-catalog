"""Upstreams a bridge can serve from. A test that requests `backend` (or `upstream`) runs once per
entry in BACKENDS; `@pytest.mark.backends(name, ...)` narrows that."""

from backends.base import (
    BRIDGE_EXTENSION,
    PROVIDER_EXTENSION,
    Backend,
    Source,
    new_connection,
    unique_id,
)
from backends.native import NativeBackend
from backends.postgres import PostgresBackend
from backends.sqlite import SQLiteBackend

BACKENDS = [NativeBackend(), SQLiteBackend(), PostgresBackend()]

__all__ = [
    "BACKENDS",
    "BRIDGE_EXTENSION",
    "PROVIDER_EXTENSION",
    "Backend",
    "Source",
    "new_connection",
    "unique_id",
]
