"""The source catalog is pinned by identity at registration, not looked up by name on each use."""

import pytest

from conftest import READ, READWRITE, bridge, new_connection, unique_id


@pytest.fixture
def source():
    con = new_connection()
    con.execute("ATTACH ':memory:' AS src")
    con.execute("CREATE TABLE src.main.users(id INTEGER PRIMARY KEY, name VARCHAR)")
    con.execute("INSERT INTO src.main.users VALUES (1, 'a'), (2, 'b')")
    yield con
    con.close()


def test_bridge_follows_a_renamed_source(source, target):
    bridge(source, target, {"users": READWRITE}, source_catalog="src")
    source.execute("ALTER DATABASE src SET ALIAS TO src2")
    assert target.execute("SELECT count(*) FROM app.main.users").fetchone() == (2,)
    target.execute("INSERT INTO app.main.users VALUES (3, 'c')")
    assert source.execute("SELECT count(*) FROM src2.main.users").fetchone() == (3,)


def test_detached_source_fails_with_a_clear_error(source, target):
    bridge(source, target, {"users": READ}, source_catalog="src")
    source.execute("DETACH src")
    with pytest.raises(Exception, match="source catalog 'src' was detached"):
        target.execute("SELECT count(*) FROM app.main.users").fetchall()


def test_a_database_attached_under_the_same_name_is_not_adopted(source, target):
    bridge(source, target, {"users": READ}, source_catalog="src")
    source.execute("DETACH src")
    source.execute("ATTACH ':memory:' AS src")
    source.execute("CREATE TABLE src.main.users(id INTEGER PRIMARY KEY, name VARCHAR)")
    with pytest.raises(Exception, match="source catalog 'src' was detached"):
        target.execute("SELECT count(*) FROM app.main.users").fetchall()


@pytest.mark.parametrize("catalog", ["temp", "system"])
def test_register_source_refuses_temporary_and_system_catalogs(source, catalog):
    with pytest.raises(Exception, match="temporary or system catalog"):
        source.execute("SELECT vcatbridge_register_source(?, ?)", [unique_id(), catalog])
