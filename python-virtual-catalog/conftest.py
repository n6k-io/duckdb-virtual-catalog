"""Rootdir conftest: CLI options and extension resolution for tests under src/*/__tests__/."""

import os
from collections.abc import Iterator
from urllib.parse import urlparse

import duckdb
import pytest
from _pytest.config import Config
from _pytest.config.argparsing import Parser
from _pytest.fixtures import FixtureRequest

from virtual_catalog.extension import VIRTUAL_CATALOG_EXT_DIR_ENV

HERE = os.path.dirname(__file__)
EXT_DIR = os.path.realpath(os.path.join(HERE, "..", "build", "release", "extension"))


def pytest_addoption(parser: Parser) -> None:
    parser.addoption(
        "--from-bucket",
        action="store_true",
        default=False,
        help="Load extension from GCS bucket instead of local build",
    )
    parser.addoption(
        "--external",
        default=None,
        help="External DB URL for integration tests (e.g. 'postgres://sean@localhost/n6k_test')",
    )


def pytest_configure(config: Config) -> None:
    config.addinivalue_line("markers", "external: requires --external flag with a database URL")


@pytest.fixture(autouse=True)
def _extension_source(request: FixtureRequest) -> Iterator[None]:
    saved = os.environ.get(VIRTUAL_CATALOG_EXT_DIR_ENV)
    if request.config.getoption("--from-bucket"):
        os.environ.pop(VIRTUAL_CATALOG_EXT_DIR_ENV, None)
    else:
        if not os.path.isdir(EXT_DIR):
            pytest.skip(f"Extensions not built: {EXT_DIR}")
        os.environ[VIRTUAL_CATALOG_EXT_DIR_ENV] = EXT_DIR
    yield
    if saved is None:
        os.environ.pop(VIRTUAL_CATALOG_EXT_DIR_ENV, None)
    else:
        os.environ[VIRTUAL_CATALOG_EXT_DIR_ENV] = saved


def _parse_external_url(url):
    """Parse an external DB URL into (db_type, attach_string, schema).

    Supports:
        postgres://user@host/dbname
        mysql://user@host/dbname
    """
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    if scheme in ("postgres", "postgresql"):
        db_type = "postgres"
        parts = []
        if parsed.hostname:
            parts.append(f"host={parsed.hostname}")
        if parsed.port:
            parts.append(f"port={parsed.port}")
        if parsed.path and parsed.path.strip("/"):
            parts.append(f"dbname={parsed.path.strip('/')}")
        if parsed.username:
            parts.append(f"user={parsed.username}")
        if parsed.password:
            parts.append(f"password={parsed.password}")
        attach_string = " ".join(parts)
        schema = "public"
    elif scheme == "mysql":
        db_type = "mysql"
        parts = []
        if parsed.hostname:
            parts.append(f"host={parsed.hostname}")
        if parsed.port:
            parts.append(f"port={parsed.port}")
        db_name = parsed.path.strip("/") if parsed.path else "test"
        parts.append(f"database={db_name}")
        if parsed.username:
            parts.append(f"user={parsed.username}")
        if parsed.password:
            parts.append(f"password={parsed.password}")
        attach_string = " ".join(parts)
        schema = db_name
    else:
        raise ValueError(f"Unsupported external DB scheme: {scheme}")
    return db_type, attach_string, schema


@pytest.fixture
def external_db(request):
    """Fixture providing a DuckDB connection with an external DB attached.

    Yields a dict with: type, attach_string, catalog, schema, source.
    Cleans up any tables created during the test.
    """
    url = request.config.getoption("--external")
    if not url:
        pytest.skip("--external not provided")

    db_type, attach_string, schema = _parse_external_url(url)
    catalog = "ext"

    source = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    source.execute(f"ATTACH '{attach_string}' AS {catalog} (TYPE {db_type})")

    tables_created = []

    def create_table(name, ddl):
        """Create a table on the external DB and track it for cleanup."""
        qualified = f'"{catalog}"."{schema}"."{name}"'
        source.execute(f"DROP TABLE IF EXISTS {qualified}")
        source.execute(f"CREATE TABLE {qualified}({ddl})")
        tables_created.append(qualified)
        return qualified

    info = {
        "type": db_type,
        "attach_string": attach_string,
        "catalog": catalog,
        "schema": schema,
        "source": source,
        "create_table": create_table,
    }

    yield info

    for qualified in tables_created:
        try:
            source.execute(f"DROP TABLE IF EXISTS {qualified}")
        except Exception:
            pass
    source.close()
