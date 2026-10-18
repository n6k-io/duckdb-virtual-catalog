"""Grants changed after ATTACH, across separate DatabaseInstances."""

import duckdb
import pytest

from conftest import READ, bridge, new_connection


@pytest.fixture
def tables(source):
    source.execute("CREATE TABLE users(id INTEGER PRIMARY KEY, name VARCHAR)")
    source.execute("INSERT INTO users VALUES (1, 'ada'), (2, 'grace')")
    source.execute("CREATE TABLE orders(id INTEGER PRIMARY KEY, amount INTEGER)")
    source.execute("INSERT INTO orders VALUES (10, 5)")
    source.execute("CREATE TABLE nopk(x INTEGER, y VARCHAR)")
    source.execute("INSERT INTO nopk VALUES (1, 'a'), (2, 'b')")


def test_a_policy_after_attach_reaches_the_target(source, target, tables):
    bridge_id = bridge(source, target, {"users": READ})
    source.execute("SELECT bridge_policy(?, 'main.orders', 'select', 'true')", [bridge_id])
    assert target.execute("SELECT id, amount FROM app.main.orders").fetchall() == [(10, 5)]


def test_the_target_cannot_grant_after_attach(source, target, tables):
    bridge_id = bridge(source, target, {"users": READ})
    with pytest.raises(duckdb.PermissionException, match="registered by a different source connection"):
        target.execute("SELECT bridge_policy(?, 'main.orders', 'select', 'true')", [bridge_id])
    with pytest.raises(duckdb.PermissionException, match="registered by a different source connection"):
        target.execute("SELECT bridge_primary_key(?, 'main.users', ['id'])", [bridge_id])
    with pytest.raises(duckdb.PermissionException, match="registered by a different source connection"):
        target.execute("SELECT bridge_finalize_policy(?)", [bridge_id])
    with pytest.raises(duckdb.CatalogException):
        target.execute("SELECT * FROM app.main.orders")


def test_a_key_after_attach_enables_a_keyed_write(source, target, tables):
    bridge_id = bridge(source, target, {"nopk": READ})
    source.execute("SELECT bridge_primary_key(?, 'main.nopk', ['x'])", [bridge_id])
    source.execute(
        "SELECT bridge_primary_key_check(?, 'main.nopk', 'SELECT x FROM nopk GROUP BY x HAVING count(*) > 1')",
        [bridge_id],
    )
    source.execute("SELECT bridge_policy(?, 'main.nopk', 'update', 'true')", [bridge_id])
    target.execute("UPDATE app.main.nopk SET y = 'z' WHERE x = 1")
    assert source.execute("SELECT x, y FROM nopk ORDER BY x").fetchall() == [(1, "z"), (2, "b")]


def test_finalize_after_attach_locks_the_grants(source, target, tables):
    bridge_id = bridge(source, target, {"users": READ})
    source.execute("SELECT bridge_finalize_policy(?)", [bridge_id])
    with pytest.raises(duckdb.PermissionException, match="are finalized"):
        source.execute("SELECT bridge_policy(?, 'main.orders', 'select', 'true')", [bridge_id])
    assert target.execute("SELECT count(*) FROM app.main.users").fetchone() == (2,)


def test_a_policy_on_the_middle_hop_reaches_the_outer_hop():
    a, b, c = new_connection(), new_connection(), new_connection()
    try:
        a.execute("CREATE TABLE users(id INTEGER PRIMARY KEY, name VARCHAR)")
        a.execute("INSERT INTO users VALUES (1, 'ada')")
        a.execute("CREATE TABLE orders(id INTEGER PRIMARY KEY, amount INTEGER)")
        a.execute("INSERT INTO orders VALUES (10, 5)")
        inner = bridge(a, b, {"users": READ})
        outer = bridge(b, c, {"users": READ}, source_catalog="app")

        a.execute("SELECT bridge_policy(?, 'main.orders', 'select', 'true')", [inner])
        b.execute("SELECT bridge_policy(?, 'main.orders', 'select', 'true')", [outer])
        assert c.execute("SELECT id, amount FROM app.main.orders").fetchall() == [(10, 5)]
    finally:
        for con in (c, b, a):
            con.close()
