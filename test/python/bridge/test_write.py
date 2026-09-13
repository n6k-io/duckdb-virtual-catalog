"""Writes issued on the target must land on the source, through the rowid path."""

import duckdb
import pytest

from conftest import READ, READWRITE, bridge

SETUP = """
CREATE TABLE users(id INTEGER PRIMARY KEY, name VARCHAR, score INTEGER);
INSERT INTO users VALUES (1, 'ana', 10), (2, 'bo', 20), (3, 'cy', 30);
CREATE TABLE logs(id INTEGER PRIMARY KEY, msg VARCHAR);
INSERT INTO logs VALUES (1, 'boot');
"""


@pytest.fixture
def bridged(source, target):
    source.execute(SETUP)
    bridge(source, target, {"users": READWRITE, "logs": READ})
    return source, target


def source_users(source):
    return source.execute("SELECT * FROM users ORDER BY id").fetchall()


def test_insert_lands_on_the_source(bridged):
    source, target = bridged
    target.execute("INSERT INTO app.main.users VALUES (4, 'di', 40)")
    assert source_users(source) == [(1, 'ana', 10), (2, 'bo', 20), (3, 'cy', 30), (4, 'di', 40)]


def test_a_prepared_insert_runs_more_than_once(bridged):
    source, target = bridged
    target.execute("PREPARE ins AS INSERT INTO app.main.users VALUES (?, ?, ?)")
    target.execute("EXECUTE ins(21, 'u', 1)")
    target.execute("EXECUTE ins(22, 'v', 2)")
    assert source.execute("SELECT id FROM users WHERE id > 20 ORDER BY id").fetchall() == [(21,), (22,)]


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
    # A read-only table is a table entry like any other, so the refusal comes from the grant check
    # and names the verb and the bridge -- rather than from the binder rejecting a view, which used
    # to report "not an table" and said nothing about permissions.
    for verb, sql in (
        ("insert", "INSERT INTO app.main.logs VALUES (2, 'nope')"),
        ("update", "UPDATE app.main.logs SET msg = 'nope'"),
        ("delete", "DELETE FROM app.main.logs"),
    ):
        with pytest.raises(Exception, match=f"does not have '{verb}' permission"):
            target.execute(sql)


def test_source_constraints_still_apply(bridged):
    source, target = bridged
    with pytest.raises(Exception):
        target.execute("INSERT INTO app.main.users VALUES (1, 'duplicate', 0)")
    assert source.execute("SELECT count(*) FROM users").fetchone() == (3,)


def test_composite_primary_key_update_and_delete(source, target):
    source.execute(
        """
        CREATE TABLE events(tenant INTEGER, eid INTEGER, note VARCHAR, PRIMARY KEY(tenant, eid));
        INSERT INTO events VALUES (1, 1, 'a'), (1, 2, 'b'), (2, 1, 'c');
    """
    )
    bridge(source, target, {"events": READWRITE})
    target.execute("UPDATE app.main.events SET note = 'B' WHERE tenant = 1 AND eid = 2")
    target.execute("DELETE FROM app.main.events WHERE tenant = 2")
    assert source.execute("SELECT * FROM events ORDER BY tenant, eid").fetchall() == [(1, 1, 'a'), (1, 2, 'B')]


def test_primary_key_override_enables_writes_on_a_keyless_table(source, target):
    source.execute("CREATE TABLE nopk(x INTEGER, y VARCHAR); INSERT INTO nopk VALUES (1, 'a'), (2, 'b')")
    bridge(source, target, {"nopk": READWRITE}, pk_overrides={"nopk": "x"})
    target.execute("UPDATE app.main.nopk SET y = 'A' WHERE x = 1")
    target.execute("DELETE FROM app.main.nopk WHERE x = 2")
    assert source.execute("SELECT * FROM nopk ORDER BY x").fetchall() == [(1, 'A')]


def test_target_rollback_undoes_the_source_write(bridged):
    """The write runs on the source connection the target's transaction owns, so aborting the
    target aborts it too."""
    source, target = bridged
    target.execute("BEGIN")
    target.execute("INSERT INTO app.main.users VALUES (7, 'seven', 70)")
    target.execute("ROLLBACK")
    assert source.execute("SELECT count(*) FROM users WHERE id = 7").fetchone() == (0,)


def test_returning_emits_the_rows_that_were_written(bridged):
    source, target = bridged
    assert target.execute("INSERT INTO app.main.users VALUES (8, 'eight', 80) RETURNING id, name").fetchall() == [
        (8, "eight")
    ]
    assert target.execute("UPDATE app.main.users SET score = 81 WHERE id = 8 RETURNING id, score").fetchall() == [
        (8, 81)
    ]
    assert target.execute("DELETE FROM app.main.users WHERE id = 8 RETURNING id").fetchall() == [(8,)]
    assert source.execute("SELECT count(*) FROM users WHERE id = 8").fetchone() == (0,)


def test_update_spanning_several_chunks_writes_every_row(source, target):
    """The sink holds its narrowed chunks until Finalize, so it must own their storage: the child
    refills one output chunk in place, and anything past the first vector would otherwise be lost
    while still being counted as affected."""
    source.execute("CREATE TABLE big(id INTEGER PRIMARY KEY, name VARCHAR)")
    source.execute("INSERT INTO big SELECT i, 'u' || i FROM range(1, 6001) t(i)")
    bridge(source, target, {"big": READWRITE})

    assert target.execute("UPDATE app.main.big SET name = 'P'").fetchall() == [(6000,)]
    assert source.execute("SELECT count(*) FROM big WHERE name = 'P'").fetchone() == (6000,)
