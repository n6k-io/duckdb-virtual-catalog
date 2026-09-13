"""The bridge across two DatabaseInstances, which is the shape a bridge is actually used in.

sqllogictest runs source and target on one instance, so everything up to here has been tested
with the source and the target sharing a DatabaseInstance. These are the tests that separate them.
"""

import pytest

from conftest import READ, READWRITE, bridge, unbridge


def seed(source):
    source.execute("CREATE TABLE orders(id INTEGER PRIMARY KEY, name VARCHAR, amt INTEGER, tenant INTEGER)")
    source.execute("INSERT INTO orders VALUES (1,'ada',50,7),(2,'grace',150,7),(3,'alan',250,9),(4,'edsger',350,7)")


def test_read_across_instances(source, target):
    seed(source)
    bridge(source, target, {"orders": READ})

    assert target.execute("SELECT count(*) FROM app.main.orders").fetchone()[0] == 4
    assert target.execute("SELECT name FROM app.main.orders WHERE amt > 100 ORDER BY id").fetchall() == [
        ("grace",),
        ("alan",),
        ("edsger",),
    ]


def test_pushdown_across_instances(source, target):
    seed(source)
    bridge(source, target, {"orders": READ})

    # An aggregate the old absorber refuses outright, crossing to another instance.
    assert target.execute(
        "SELECT tenant, sum(amt) FROM app.main.orders GROUP BY tenant ORDER BY tenant"
    ).fetchall() == [(7, 550), (9, 250)]

    plan = target.execute("EXPLAIN SELECT tenant, sum(amt) FROM app.main.orders GROUP BY tenant").fetchall()
    assert "AGGREGATE" in plan[0][1]


def test_policy_across_instances(source, target):
    seed(source)
    bridge(source, target, {"orders": {"select": "tenant = 7"}})

    assert target.execute("SELECT count(*) FROM app.main.orders").fetchone()[0] == 3
    assert target.execute("SELECT count(*) FROM app.main.orders WHERE id = 3").fetchone()[0] == 0
    # The source's own table is untouched by the bridge's policy.
    assert source.execute("SELECT count(*) FROM orders").fetchone()[0] == 4


def test_write_across_instances(source, target):
    seed(source)
    bridge(source, target, {"orders": READWRITE})

    target.execute("INSERT INTO app.main.orders VALUES (5,'hoare',500,7)")
    assert source.execute("SELECT count(*) FROM orders").fetchone()[0] == 5

    target.execute("UPDATE app.main.orders SET amt = 0 WHERE id = 1")
    assert source.execute("SELECT amt FROM orders WHERE id = 1").fetchone()[0] == 0

    target.execute("DELETE FROM app.main.orders WHERE id = 5")
    assert source.execute("SELECT count(*) FROM orders").fetchone()[0] == 4


def test_rollback_across_instances(source, target):
    seed(source)
    bridge(source, target, {"orders": READWRITE})

    target.execute("BEGIN TRANSACTION")
    target.execute("INSERT INTO app.main.orders VALUES (6,'dijkstra',600,7)")
    assert target.execute("SELECT count(*) FROM app.main.orders").fetchone()[0] == 5
    target.execute("ROLLBACK")

    assert source.execute("SELECT count(*) FROM orders").fetchone()[0] == 4
    assert target.execute("SELECT count(*) FROM app.main.orders").fetchone()[0] == 4


def test_ungranted_table_is_absent(source, target):
    seed(source)
    source.execute("CREATE TABLE secrets(id INTEGER, payload VARCHAR)")
    source.execute("INSERT INTO secrets VALUES (1, 'nuclear codes')")
    bridge(source, target, {"orders": READ})

    with pytest.raises(duckdb_error()):
        target.execute("SELECT * FROM app.main.secrets").fetchall()


def test_ungranted_verb_is_refused(source, target):
    seed(source)
    bridge(source, target, {"orders": READ})

    with pytest.raises(duckdb_error()):
        target.execute("INSERT INTO app.main.orders VALUES (9,'x',1,1)")
    assert source.execute("SELECT count(*) FROM orders").fetchone()[0] == 4


def test_detach_releases_the_catalog(source, target):
    seed(source)
    bridge(source, target, {"orders": READ})
    assert target.execute("SELECT count(*) FROM app.main.orders").fetchone()[0] == 4

    unbridge(target)
    with pytest.raises(duckdb_error()):
        target.execute("SELECT count(*) FROM app.main.orders").fetchall()


def duckdb_error():
    import duckdb

    return duckdb.Error
