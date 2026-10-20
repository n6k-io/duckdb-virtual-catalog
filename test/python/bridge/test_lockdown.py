"""virtual_catalog_lockdown against the bridge: a locked instance can still be a target, a source, a
middle hop, or both ends of a same-instance bridge."""

import duckdb
import pytest

from conftest import READ, READWRITE, bridge, new_connection, unbridge


def lockdown(con, memory_limit="512MB", threads=2):
    assert con.execute("SELECT virtual_catalog_lockdown(?, ?)", [memory_limit, threads]).fetchone() == ("ok",)


@pytest.fixture
def pair(source, target):
    source.execute("CREATE TABLE users(id INTEGER PRIMARY KEY, name VARCHAR)")
    source.execute("INSERT INTO users VALUES (1, 'ada'), (2, 'grace')")
    return source, target


def test_settings_are_applied(source, target):
    source.execute("SET memory_limit = '256MB'")
    lockdown(target, memory_limit="256MB", threads=3)
    query = (
        "SELECT name, value FROM duckdb_settings() WHERE name IN "
        "('memory_limit', 'threads', 'enable_external_access', 'lock_configuration', 'temp_directory')"
    )
    settings = dict(target.execute(query).fetchall())
    assert settings["memory_limit"] == dict(source.execute(query).fetchall())["memory_limit"]
    assert settings["threads"] == "3"
    assert settings["enable_external_access"] == "false"
    assert settings["lock_configuration"] == "true"
    assert settings["temp_directory"] == ""


def test_attach_after_lockdown(pair):
    source, target = pair
    lockdown(target)
    bridge(source, target, {"users": READ})
    assert target.execute("SELECT id, name FROM app.main.users ORDER BY id").fetchall() == [(1, "ada"), (2, "grace")]


def test_writes_through_a_locked_target(pair):
    source, target = pair
    bridge(source, target, {"users": READWRITE})
    lockdown(target)
    target.execute("INSERT INTO app.main.users VALUES (3, 'alan')")
    target.execute("UPDATE app.main.users SET name = 'ADA' WHERE id = 1")
    target.execute("DELETE FROM app.main.users WHERE id = 2")
    assert source.execute("SELECT id, name FROM users ORDER BY id").fetchall() == [(1, "ADA"), (3, "alan")]


def test_transaction_reads_on_a_locked_target(pair):
    source, target = pair
    bridge(source, target, {"users": READ})
    lockdown(target)
    target.execute("BEGIN")
    assert target.execute("SELECT count(*) FROM app.main.users").fetchone() == (2,)
    assert target.execute("SELECT max(id) FROM app.main.users").fetchone() == (2,)
    target.execute("COMMIT")


def test_detach_after_lockdown(pair):
    source, target = pair
    bridge(source, target, {"users": READ})
    lockdown(target)
    unbridge(target)
    with pytest.raises(duckdb.BinderException, match='Catalog "app" does not exist'):
        target.execute("SELECT * FROM app.main.users")


def test_locked_source_still_grants(pair):
    source, target = pair
    lockdown(source)
    bridge_id = bridge(source, target, {"users": READ})
    source.execute("SELECT bridge_set_context(?, 'tenant', 1)", [bridge_id])
    source.execute("CREATE TABLE logs(id INTEGER PRIMARY KEY)")
    source.execute("SELECT bridge_policy(?, 'main.logs', 'select', 'id = $tenant')", [bridge_id])
    source.execute("INSERT INTO logs VALUES (1), (2)")
    assert target.execute("SELECT id FROM app.main.logs").fetchall() == [(1,)]


def test_locked_middle_hop():
    a, b, c = new_connection(), new_connection(), new_connection()
    try:
        a.execute("CREATE TABLE users(id INTEGER PRIMARY KEY, name VARCHAR)")
        a.execute("INSERT INTO users VALUES (1, 'ada'), (2, 'grace')")
        lockdown(b)
        bridge(a, b, {"users": READWRITE})
        bridge(b, c, {"users": READWRITE}, source_catalog="app")
        c.execute("INSERT INTO app.main.users VALUES (3, 'alan')")
        assert c.execute("SELECT id FROM app.main.users ORDER BY id").fetchall() == [(1,), (2,), (3,)]
        assert a.execute("SELECT count(*) FROM users").fetchone() == (3,)
    finally:
        for con in (c, b, a):
            con.close()


def test_same_instance(source):
    source.execute("CREATE TABLE users(id INTEGER PRIMARY KEY)")
    source.execute("INSERT INTO users VALUES (1), (2)")
    lockdown(source)
    bridge(source, source, {"users": READ})
    assert source.execute("SELECT id FROM app.main.users ORDER BY id").fetchall() == [(1,), (2,)]


def test_second_call_is_refused(target):
    lockdown(target)
    with pytest.raises(duckdb.PermissionException, match="already locked"):
        lockdown(target)


@pytest.mark.parametrize("args", [(None, 2), ("512MB", None)], ids=["memory_limit", "threads"])
def test_null_argument_is_refused(target, args):
    with pytest.raises(duckdb.InvalidInputException, match="must not be NULL"):
        target.execute("SELECT virtual_catalog_lockdown(?, ?)", list(args))
    assert target.execute("SELECT current_setting('lock_configuration')").fetchone() == (False,)


def test_once_per_row_is_refused(target):
    with pytest.raises(duckdb.InvalidInputException, match="once"):
        target.execute("SELECT virtual_catalog_lockdown('512MB', 2) FROM range(3)")
    assert target.execute("SELECT current_setting('lock_configuration')").fetchone() == (False,)


@pytest.mark.parametrize("memory_limit", ["-1", "none", "null", "NONE"])
def test_unlimited_memory_limit_is_refused(target, memory_limit):
    with pytest.raises(duckdb.InvalidInputException, match="memory_limit"):
        target.execute("SELECT virtual_catalog_lockdown(?, 2)", [memory_limit])
    assert target.execute("SELECT current_setting('lock_configuration')").fetchone() == (False,)


def test_drop_functions(source, target):
    target.execute("SELECT virtual_catalog_lockdown('512MB', 2, ['read_text', 'md5', 'version'])")
    for sql in ["SELECT * FROM read_text('x')", "SELECT md5('x')", "SELECT version()", "PRAGMA version"]:
        with pytest.raises(duckdb.CatalogException):
            target.execute(sql)
    assert source.execute("SELECT md5('x')").fetchone() == ("9dd4e461268c8034f5c8564e155c67a6",)


def test_drop_functions_unknown_name_is_refused(target):
    with pytest.raises(duckdb.InvalidInputException, match='no system function named "no_such_fn"'):
        target.execute("SELECT virtual_catalog_lockdown('512MB', 2, ['md5', 'no_such_fn'])")
    assert target.execute("SELECT current_setting('lock_configuration')").fetchone() == (False,)
    assert target.execute("SELECT md5('x')").fetchone() == ("9dd4e461268c8034f5c8564e155c67a6",)


@pytest.mark.parametrize("names", [None, ["md5", None]], ids=["list", "element"])
def test_drop_functions_null_is_refused(target, names):
    with pytest.raises(duckdb.InvalidInputException, match="drop_functions must not"):
        target.execute("SELECT virtual_catalog_lockdown('512MB', 2, ?::VARCHAR[])", [names])
    assert target.execute("SELECT current_setting('lock_configuration')").fetchone() == (False,)


def test_transaction_is_refused(target):
    target.execute("BEGIN")
    with pytest.raises(duckdb.PermissionException, match="inside a transaction"):
        target.execute("SELECT virtual_catalog_lockdown('512MB', 2, ['md5'])")
    target.execute("ROLLBACK")
    assert target.execute("SELECT current_setting('lock_configuration')").fetchone() == (False,)
    assert target.execute("SELECT md5('x')").fetchone() == ("9dd4e461268c8034f5c8564e155c67a6",)
