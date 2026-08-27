"""Writes issued on the target must land on the source, through the rowid path."""

import pytest

from conftest import bridge

SETUP = """
CREATE TABLE users(id INTEGER PRIMARY KEY, name VARCHAR, score INTEGER);
INSERT INTO users VALUES (1, 'ana', 10), (2, 'bo', 20), (3, 'cy', 30);
CREATE TABLE logs(id INTEGER PRIMARY KEY, msg VARCHAR);
INSERT INTO logs VALUES (1, 'boot');
"""


@pytest.fixture
def bridged(source, target):
    source.execute(SETUP)
    bridge(source, target, {"users": "readwrite", "logs": "read"})
    return source, target


def source_users(source):
    return source.execute("SELECT * FROM users ORDER BY id").fetchall()


def test_insert_lands_on_the_source(bridged):
    source, target = bridged
    target.execute("INSERT INTO app.main.users VALUES (4, 'di', 40)")
    assert source_users(source) == [(1, 'ana', 10), (2, 'bo', 20), (3, 'cy', 30), (4, 'di', 40)]


def test_insert_many_rows(bridged):
    source, target = bridged
    target.execute("INSERT INTO app.main.users SELECT i + 100, 'gen', i FROM range(1, 2500) t(i)")
    assert source.execute("SELECT count(*) FROM users").fetchone() == (2502,)


def test_insert_select_from_the_bridge_itself(bridged):
    source, target = bridged
    target.execute("INSERT INTO app.main.users SELECT id + 10, name, score FROM app.main.users")
    assert source.execute("SELECT count(*) FROM users").fetchone() == (6,)


def test_update_lands_on_the_source(bridged):
    source, target = bridged
    target.execute("UPDATE app.main.users SET name = 'ANA' WHERE id = 1")
    assert source_users(source) == [(1, 'ANA', 10), (2, 'bo', 20), (3, 'cy', 30)]


def test_update_touching_several_rows(bridged):
    source, target = bridged
    target.execute("UPDATE app.main.users SET score = score + 1 WHERE id >= 2")
    assert source_users(source) == [(1, 'ana', 10), (2, 'bo', 21), (3, 'cy', 31)]


def test_update_matching_nothing_changes_nothing(bridged):
    source, target = bridged
    target.execute("UPDATE app.main.users SET name = 'x' WHERE id = 999")
    assert source_users(source) == [(1, 'ana', 10), (2, 'bo', 20), (3, 'cy', 30)]


def test_delete_lands_on_the_source(bridged):
    source, target = bridged
    target.execute("DELETE FROM app.main.users WHERE id = 2")
    assert source_users(source) == [(1, 'ana', 10), (3, 'cy', 30)]


def test_delete_all(bridged):
    source, target = bridged
    target.execute("DELETE FROM app.main.users")
    assert source_users(source) == []


def test_writes_are_visible_back_through_the_bridge(bridged):
    _, target = bridged
    target.execute("INSERT INTO app.main.users VALUES (9, 'nine', 90)")
    target.execute("UPDATE app.main.users SET score = 91 WHERE id = 9")
    assert target.execute("SELECT * FROM app.main.users WHERE id = 9").fetchall() == [(9, 'nine', 91)]


def test_read_only_tables_reject_every_write_path(bridged):
    _, target = bridged
    # READ tables are exposed as views, so the binder refuses -- the message names neither the
    # bridge nor a permission.
    with pytest.raises(Exception, match="not an table"):
        target.execute("INSERT INTO app.main.logs VALUES (2, 'nope')")
    with pytest.raises(Exception, match="Can only update base table"):
        target.execute("UPDATE app.main.logs SET msg = 'nope'")
    with pytest.raises(Exception, match="Can only delete from base table"):
        target.execute("DELETE FROM app.main.logs")


def test_source_constraints_still_apply(bridged):
    source, target = bridged
    with pytest.raises(Exception):
        target.execute("INSERT INTO app.main.users VALUES (1, 'duplicate', 0)")
    assert source.execute("SELECT count(*) FROM users").fetchone() == (3,)


def test_composite_primary_key_update_and_delete(source, target):
    source.execute("""
        CREATE TABLE events(tenant INTEGER, eid INTEGER, note VARCHAR, PRIMARY KEY(tenant, eid));
        INSERT INTO events VALUES (1, 1, 'a'), (1, 2, 'b'), (2, 1, 'c');
    """)
    bridge(source, target, {"events": "readwrite"})
    target.execute("UPDATE app.main.events SET note = 'B' WHERE tenant = 1 AND eid = 2")
    target.execute("DELETE FROM app.main.events WHERE tenant = 2")
    assert source.execute("SELECT * FROM events ORDER BY tenant, eid").fetchall() == \
        [(1, 1, 'a'), (1, 2, 'B')]


def test_primary_key_override_enables_writes_on_a_keyless_table(source, target):
    source.execute("CREATE TABLE nopk(x INTEGER, y VARCHAR); INSERT INTO nopk VALUES (1, 'a'), (2, 'b')")
    bridge(source, target, {"nopk": "readwrite"}, pk_overrides={"nopk": "x"})
    target.execute("UPDATE app.main.nopk SET y = 'A' WHERE x = 1")
    target.execute("DELETE FROM app.main.nopk WHERE x = 2")
    assert source.execute("SELECT * FROM nopk ORDER BY x").fetchall() == [(1, 'A')]


def test_target_rollback_does_not_undo_the_source_write(bridged):
    """Documented as stateless: the write commits on the source independently of the target's
    transaction, so a target ROLLBACK does not take it back."""
    source, target = bridged
    target.execute("BEGIN")
    target.execute("INSERT INTO app.main.users VALUES (7, 'seven', 70)")
    target.execute("ROLLBACK")
    assert source.execute("SELECT count(*) FROM users WHERE id = 7").fetchone() == (1,)


def test_returning_is_not_supported(bridged):
    _, target = bridged
    with pytest.raises(Exception):
        target.execute("INSERT INTO app.main.users VALUES (8, 'eight', 80) RETURNING id")
