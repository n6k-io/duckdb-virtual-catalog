"""Grants changed after ATTACH, across separate DatabaseInstances."""

import json
from concurrent.futures import ThreadPoolExecutor

import duckdb
import pytest

from conftest import READ, bridge, new_connection, unique_id


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
    source.execute("SELECT bridge_set_policy(?, 'main.orders', 'select', 'true')", [bridge_id])
    assert target.execute("SELECT id, amount FROM app.main.orders").fetchall() == [(10, 5)]


def test_the_target_cannot_grant_after_attach(source, target, tables):
    bridge_id = bridge(source, target, {"users": READ})
    with pytest.raises(duckdb.PermissionException, match="registered by a different source connection"):
        target.execute("SELECT bridge_set_policy(?, 'main.orders', 'select', 'true')", [bridge_id])
    with pytest.raises(duckdb.PermissionException, match="registered by a different source connection"):
        target.execute("SELECT bridge_set_policy(?, 'main.users', 'primary_key', ['id'])", [bridge_id])
    with pytest.raises(duckdb.PermissionException, match="registered by a different source connection"):
        target.execute("SELECT bridge_finalize_policy(?)", [bridge_id])
    with pytest.raises(duckdb.CatalogException):
        target.execute("SELECT * FROM app.main.orders")


def test_a_key_after_attach_enables_a_keyed_write(source, target, tables):
    bridge_id = bridge(source, target, {"nopk": READ})
    source.execute(
        "SELECT bridge_set_policy(?, 'main.nopk', 'primary_key', {'columns': ['x'], 'verify_unique': ?})",
        [bridge_id, "SELECT x FROM nopk GROUP BY x HAVING count(*) > 1"],
    )
    source.execute("SELECT bridge_set_policy(?, 'main.nopk', 'update', 'true')", [bridge_id])
    target.execute("UPDATE app.main.nopk SET y = 'z' WHERE x = 1")
    assert source.execute("SELECT x, y FROM nopk ORDER BY x").fetchall() == [(1, "z"), (2, "b")]


def test_finalize_after_attach_locks_the_grants(source, target, tables):
    bridge_id = bridge(source, target, {"users": READ})
    source.execute("SELECT bridge_finalize_policy(?)", [bridge_id])
    with pytest.raises(duckdb.PermissionException, match="are finalized"):
        source.execute("SELECT bridge_set_policy(?, 'main.orders', 'select', 'true')", [bridge_id])
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

        a.execute("SELECT bridge_set_policy(?, 'main.orders', 'select', 'true')", [inner])
        b.execute("SELECT bridge_set_policy(?, 'main.orders', 'select', 'true')", [outer])
        assert c.execute("SELECT id, amount FROM app.main.orders").fetchall() == [(10, 5)]
    finally:
        for con in (c, b, a):
            con.close()


def test_configure_replaces_the_policy_for_the_next_statement(source, target, tables):
    bridge_id = bridge(source, target, {"users": READ, "orders": READ})
    policy = {"policy_version": 1, "tables": {"main.orders": {"select": "amount > 100"}}}
    source.execute("SELECT bridge_configure_policy(?, ?)", [bridge_id, json.dumps(policy)])
    assert target.execute("SELECT count(*) FROM app.main.orders").fetchone() == (0,)
    with pytest.raises(duckdb.CatalogException):
        target.execute("SELECT * FROM app.main.users")


def test_configure_waits_for_uncommitted_target_ddl(source, target, tables):
    bridge_id = bridge(source, target, {"users": READ})
    source.execute("SELECT bridge_set_policy(?, 'main', 'create', true)", [bridge_id])
    policy = json.dumps({"policy_version": 1, "tables": {"main.users": {"select": True}}})

    target.execute("BEGIN")
    target.execute("CREATE TABLE app.main.made(id INTEGER)")
    with pytest.raises(duckdb.PermissionException, match="uncommitted DDL"):
        source.execute("SELECT bridge_configure_policy(?, ?)", [bridge_id, policy])
    with pytest.raises(duckdb.PermissionException, match="uncommitted DDL"):
        source.execute("SELECT bridge_set_policy(?, 'main.orders', 'select', true)", [bridge_id])
    target.execute("ROLLBACK")

    source.execute("SELECT bridge_configure_policy(?, ?)", [bridge_id, policy])
    assert target.execute("SELECT count(*) FROM app.main.users").fetchone() == (2,)


def test_concurrent_settings_are_all_kept(source, target):
    names = [f"t{i}" for i in range(16)]
    for name in names:
        source.execute(f"CREATE TABLE {name}(id INTEGER PRIMARY KEY)")
    bridge_id = unique_id()
    token = source.execute("SELECT bridge_register_source(?, 'memory')", [bridge_id]).fetchone()[0]

    def grant(name):
        cursor = source.cursor()
        cursor.execute("SELECT bridge_set_policy(?, ?, 'select', true)", [bridge_id, f"main.{name}"])

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(grant, names))
    target.execute(f"ATTACH '' AS app (TYPE vcatbridge, ID '{bridge_id}', TOKEN '{token}')")
    granted = target.execute("SELECT name FROM bridge_table_permissions('app') ORDER BY name").fetchall()
    assert sorted(row[0] for row in granted) == sorted(names)


@pytest.mark.parametrize("edit", ["'remove', 'main.users'", "'set', []::VARCHAR[]"])
def test_foreign_key_edit_on_ungranted_table_leaves_attach_working(source, target, edit):
    source.execute("CREATE TABLE users(id INTEGER PRIMARY KEY)")
    source.execute("CREATE TABLE orders(id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id))")
    bridge_id = unique_id()
    token = source.execute("SELECT bridge_register_source(?, 'memory')", [bridge_id]).fetchone()[0]
    source.execute("SELECT bridge_set_policy(?, 'main.users', 'select', true)", [bridge_id])
    source.execute(f"SELECT bridge_set_policy(?, 'main.orders', 'foreign_keys', {edit})", [bridge_id])

    target.execute(f"ATTACH '' AS app (TYPE vcatbridge, ID '{bridge_id}', TOKEN '{token}')")
    granted = target.execute("SELECT name FROM bridge_table_permissions('app')").fetchall()
    assert granted == [("users",)]
