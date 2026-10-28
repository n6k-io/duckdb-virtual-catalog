"""Install and load the ``vcatbridge`` and ``virtual_catalog_provider`` extensions.

Local builds resolve by name from ``$VIRTUAL_CATALOG_EXT_DIR`` (this repo's
``build/release/extension``); unset, they install from the GCS bucket. Connections must allow
unsigned extensions.
"""

import logging
import os
from typing import Optional

import duckdb

log = logging.getLogger(__name__)

EXTENSION_REPO = "https://storage.googleapis.com/n6k-duckdb-release"
VIRTUAL_CATALOG_EXT_DIR_ENV = "VIRTUAL_CATALOG_EXT_DIR"

VCATBRIDGE = "vcatbridge"
VIRTUAL_CATALOG_PROVIDER = "virtual_catalog_provider"


def local_extension_path(name: str, dir_env: str = VIRTUAL_CATALOG_EXT_DIR_ENV) -> Optional[str]:
    base = os.environ.get(dir_env)
    if not base:
        return None
    path = os.path.join(base, name, f"{name}.duckdb_extension")
    return path if os.path.exists(path) else None


def _load_local_or_install(
    conn: duckdb.DuckDBPyConnection, name: str, dir_env: str = VIRTUAL_CATALOG_EXT_DIR_ENV
) -> None:
    local = local_extension_path(name, dir_env)
    if local:
        log.debug("loading duckdb extension %r from local build %s", name, local)
        conn.load_extension(local)
    else:
        log.debug("installing duckdb extension %r from %s", name, EXTENSION_REPO)
        conn.install_extension(name, repository_url=EXTENSION_REPO)
        conn.load_extension(name)


def load_vcatbridge(conn: duckdb.DuckDBPyConnection) -> None:
    _load_local_or_install(conn, VCATBRIDGE)


def load_virtual_catalog_provider(conn: duckdb.DuckDBPyConnection) -> None:
    _load_local_or_install(conn, VIRTUAL_CATALOG_PROVIDER)
