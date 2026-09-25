"""A source catalog that is not DuckDB's own storage.

The scan of a sqlite (or postgres) table cannot be serialized, so it cannot be built at bind time
and copied to the connection that runs it. The bridge names the table instead and binds it where
the plan runs. These tests pin that reads, policies and every write verb work over such a catalog.
"""

import duckdb
import pytest

from conftest import bridge, new_connection


@pytest.fixture
def sqlite_source(tmp_path):
    con = new_connection()
    try:
        con.execute("INSTALL sqlite; LOAD sqlite")
    except duckdb.Error as error:
        con.close()
        pytest.skip(f"sqlite extension unavailable: {error}")
    con.execute(f"ATTACH '{tmp_path / 'src.sqlite'}' AS lite (TYPE sqlite)")
    con.execute("CREATE TABLE lite.users(id INTEGER PRIMARY KEY, name VARCHAR, score INTEGER)")
    con.execute("INSERT INTO lite.users VALUES (1, 'ana', 10), (2, 'bo', 20), (3, 'cy', 30)")
    yield con
    con.close()


def users(source):
    return source.execute("SELECT * FROM lite.users ORDER BY id").fetchall()


def test_read(sqlite_source, target):
    bridge(sqlite_source, target, {"users": ["select"]}, source_catalog="lite")
    assert target.execute("SELECT * FROM app.main.users ORDER BY id").fetchall() == users(sqlite_source)


def test_read_with_a_row_policy(sqlite_source, target):
    bridge(sqlite_source, target, {"users": {"select": "score > 10"}}, source_catalog="lite")
    assert target.execute("SELECT id FROM app.main.users ORDER BY id").fetchall() == [(2,), (3,)]


def test_pushed_filter_runs_on_the_source(sqlite_source, target):
    bridge(sqlite_source, target, {"users": ["select"]}, source_catalog="lite")
    assert target.execute("SELECT name FROM app.main.users WHERE score >= 20 ORDER BY id").fetchall() == [
        ("bo",),
        ("cy",),
    ]


def test_insert(sqlite_source, target):
    bridge(sqlite_source, target, {"users": ["select", "insert"]}, source_catalog="lite")
    target.execute("INSERT INTO app.main.users VALUES (4, 'di', 40)")
    assert users(sqlite_source)[-1] == (4, "di", 40)


def test_update(sqlite_source, target):
    bridge(sqlite_source, target, {"users": ["select", "update"]}, source_catalog="lite")
    target.execute("UPDATE app.main.users SET score = 99 WHERE id = 2")
    assert users(sqlite_source) == [(1, "ana", 10), (2, "bo", 99), (3, "cy", 30)]


def test_delete(sqlite_source, target):
    bridge(sqlite_source, target, {"users": ["select", "delete"]}, source_catalog="lite")
    target.execute("DELETE FROM app.main.users WHERE id = 1")
    assert users(sqlite_source) == [(2, "bo", 20), (3, "cy", 30)]


def test_whole_write_fed_by_the_same_source(sqlite_source, target):
    """Rows come from a read of the same source, so the write carries a plan, not rows."""
    bridge(sqlite_source, target, {"users": ["select", "insert"]}, source_catalog="lite")
    target.execute("INSERT INTO app.main.users SELECT id + 10, name, score FROM app.main.users")
    assert sqlite_source.execute("SELECT count(*) FROM lite.users").fetchone() == (6,)
