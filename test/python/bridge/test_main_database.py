"""The bridge as the target's main database: `duckdb.connect('vcatbridge:')`, then
`vcatbridge_init(bridge_id, token)`."""

import duckdb
import pytest

from conftest import BRIDGE_EXTENSION, unique_id

NOT_INITIALIZED = "not initialized"


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
    con.execute("LOAD vcatbridge")
    yield con
    con.close()


@pytest.fixture
def target(extension_directory):
    con = duckdb.connect("vcatbridge:", config=config(extension_directory))
    yield con
    con.close()


def register(source):
    source.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, tenant_id INTEGER, name VARCHAR)")
    source.execute("INSERT INTO users VALUES (1, 42, 'ada'), (2, 42, 'grace'), (3, 7, 'linus')")
    bridge_id = unique_id()
    token = source.execute("SELECT vcatbridge_register_source($1, 'memory')", [bridge_id]).fetchone()[0]
    source.execute("SELECT vcatbridge_set_policy($1, 'main.users', 'select', 'tenant_id = 42')", [bridge_id])
    return bridge_id, token


def init(target, bridge_id, token):
    target.execute("SELECT vcatbridge_init($1, $2)", [bridge_id, token])


def test_init_then_query(source, target):
    bridge_id, token = register(source)
    init(target, bridge_id, token)

    assert target.execute("SELECT current_database()").fetchone()[0] == "memory"
    assert target.execute("SELECT id FROM users ORDER BY id").fetchall() == [(1,), (2,)]
    assert target.execute("SELECT count(*) FROM main.users").fetchone()[0] == 2


def test_new_connection_sees_bridge(source, target):
    bridge_id, token = register(source)
    init(target, bridge_id, token)

    other = target.cursor()
    assert other.execute("SELECT id FROM users ORDER BY id").fetchall() == [(1,), (2,)]
    other.close()


@pytest.mark.parametrize(
    "sql",
    ["CREATE TABLE memory.main.users (id INTEGER)", "CREATE SCHEMA memory.s"],
)
def test_forbidden_before_init(target, sql):
    with pytest.raises(duckdb.Error, match=NOT_INITIALIZED):
        target.execute(sql)


@pytest.mark.parametrize(
    "sql",
    ["SELECT * FROM users", "SELECT * FROM memory.main.users", "CREATE TABLE users (id INTEGER)", "USE memory.main"],
)
def test_lookup_fails_before_init(target, sql):
    with pytest.raises(duckdb.Error):
        target.execute(sql)


def test_enumeration_before_init(target):
    assert target.execute("SELECT count(*) FROM duckdb_tables()").fetchone()[0] == 0
    assert target.execute("SELECT 1").fetchone()[0] == 1


def test_native_table_cannot_shadow(source, target):
    bridge_id, token = register(source)
    with pytest.raises(duckdb.Error):
        target.execute("CREATE TABLE users (id INTEGER, tenant_id INTEGER, name VARCHAR)")
    init(target, bridge_id, token)
    assert target.execute("SELECT id FROM users ORDER BY id").fetchall() == [(1,), (2,)]


def test_second_init_refused(source, target):
    bridge_id, token = register(source)
    init(target, bridge_id, token)
    with pytest.raises(duckdb.Error, match="already initialized"):
        init(target, bridge_id, token)


def test_bad_token(source, target):
    bridge_id, token = register(source)
    with pytest.raises(duckdb.Error, match="invalid setup token"):
        init(target, bridge_id, "wrong")
    init(target, bridge_id, token)
    assert target.execute("SELECT count(*) FROM users").fetchone()[0] == 2


def test_bad_tokens_drop_registration(source, target):
    bridge_id, token = register(source)
    for _ in range(5):
        with pytest.raises(duckdb.Error, match="invalid setup token"):
            init(target, bridge_id, "wrong")
    with pytest.raises(duckdb.Error, match="no registered source"):
        init(target, bridge_id, token)


def test_init_after_lockdown_refused(source, target):
    bridge_id, token = register(source)
    target.execute("SELECT vcatbridge_lockdown('1GB', 2)")
    with pytest.raises(duckdb.Error, match="configuration is locked"):
        init(target, bridge_id, token)


def test_init_on_plain_main_refused(source, extension_directory):
    bridge_id, token = register(source)
    con = duckdb.connect(config=config(extension_directory))
    con.execute("LOAD vcatbridge")
    with pytest.raises(duckdb.Error, match="main database is not a bridge"):
        init(con, bridge_id, token)
    con.close()


def test_path_refused(extension_directory):
    with pytest.raises(duckdb.Error, match="with no path"):
        duckdb.connect("vcatbridge:b1", config=config(extension_directory))


def test_token_not_exposed(source, target):
    bridge_id, token = register(source)
    init(target, bridge_id, token)

    leaked = target.execute(
        "SELECT count(*) FROM duckdb_databases() WHERE path LIKE '%' || $1 || '%' OR database_name LIKE '%' || $1 || '%'",
        [token],
    ).fetchone()[0]
    assert leaked == 0
    leaked = target.execute(
        "SELECT count(*) FROM duckdb_settings() WHERE value LIKE '%' || $1 || '%'", [token]
    ).fetchone()[0]
    assert leaked == 0
