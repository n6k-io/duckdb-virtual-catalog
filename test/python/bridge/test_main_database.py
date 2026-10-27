"""The bridge opened as the target's main database: `duckdb.connect('virtual_catalog_bridge:<id>?token=<token>')`."""

import duckdb
import pytest

from conftest import BRIDGE_EXTENSION, unique_id


@pytest.fixture
def extension_directory(tmp_path):
    if not BRIDGE_EXTENSION.exists():
        pytest.skip(f"extension not built: {BRIDGE_EXTENSION} -- run `make release`")
    directory = str(tmp_path / "extensions")
    installer = duckdb.connect(config=config(directory))
    installer.execute(f"INSTALL '{BRIDGE_EXTENSION}'")
    installer.close()
    return directory


def config(extension_directory):
    return {"allow_unsigned_extensions": "true", "extension_directory": extension_directory}


@pytest.fixture
def source(extension_directory):
    """Loaded from the installed copy: the target loads that one, and both must share a copy."""
    con = duckdb.connect(config=config(extension_directory))
    con.execute("LOAD virtual_catalog_bridge")
    yield con
    con.close()


def open_main(extension_directory, path, temp_directory):
    return duckdb.connect(
        f"virtual_catalog_bridge:{path}",
        config={**config(extension_directory), "temp_directory": temp_directory},
    )


def register(source):
    source.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, tenant_id INTEGER, name VARCHAR)")
    source.execute("INSERT INTO users VALUES (1, 42, 'ada'), (2, 42, 'grace'), (3, 7, 'linus')")
    bridge_id = unique_id()
    token = source.execute("SELECT bridge_register_source($1, 'memory')", [bridge_id]).fetchone()[0]
    source.execute("SELECT bridge_set_policy($1, 'main.users', 'select', 'tenant_id = 42')", [bridge_id])
    return bridge_id, token


def test_bridge_as_main_database(source, extension_directory, tmp_path):
    bridge_id, token = register(source)
    target = open_main(extension_directory, f"{bridge_id}?token={token}", str(tmp_path / "spill"))

    assert target.execute("SELECT current_database()").fetchone()[0] == bridge_id
    assert target.execute("SELECT id FROM users ORDER BY id").fetchall() == [(1,), (2,)]
    assert target.execute("SELECT count(*) FROM main.users").fetchone()[0] == 2


def test_token_not_exposed(source, extension_directory, tmp_path):
    bridge_id, token = register(source)
    target = open_main(extension_directory, f"{bridge_id}?token={token}", str(tmp_path / "spill"))

    assert (
        target.execute("SELECT count(*) FROM duckdb_databases() WHERE path LIKE '%' || $1 || '%'", [token]).fetchone()[
            0
        ]
        == 0
    )
    assert (
        target.execute("SELECT count(*) FROM duckdb_settings() WHERE value LIKE '%' || $1 || '%'", [token]).fetchone()[
            0
        ]
        == 0
    )


def test_bad_token(source, extension_directory, tmp_path):
    bridge_id, _ = register(source)
    with pytest.raises(duckdb.Error, match="invalid setup token"):
        open_main(extension_directory, f"{bridge_id}?token=wrong", str(tmp_path / "spill"))


def test_missing_token(source, extension_directory, tmp_path):
    bridge_id, _ = register(source)
    with pytest.raises(duckdb.Error, match=r"the path must be '<id>\?token=<token>'"):
        open_main(extension_directory, bridge_id, str(tmp_path / "spill"))


def test_default_temp_directory_refused(source, extension_directory):
    bridge_id, token = register(source)
    with pytest.raises(duckdb.Error, match="pass 'temp_directory' in the connect config"):
        duckdb.connect(f"virtual_catalog_bridge:{bridge_id}?token={token}", config=config(extension_directory))
