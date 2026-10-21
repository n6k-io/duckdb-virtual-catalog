"""ATTACK  target writes to a table granted select and nothing else
BREACH  the source table changes
PASS    the source is identical afterwards

The source is compared whether or not the statement was refused: a write that errors after already
reaching the source is still a breach.
"""

import duckdb
import pytest

from security.world import HIDDEN, SETUP

WRITES = {
    "insert": "INSERT INTO app.main.users VALUES (9, 1)",
    "update": "UPDATE app.main.users SET tenant = 2 WHERE id = 1",
    "update_hidden": "UPDATE app.main.users SET tenant = 1 WHERE tenant = 2",
    "delete": "DELETE FROM app.main.users WHERE id = 1",
    "delete_all": "DELETE FROM app.main.users",
    "insert_select": "INSERT INTO app.main.users SELECT 9, 1",
    "drop": "DROP TABLE app.main.users",
    "alter": "ALTER TABLE app.main.users ADD COLUMN owned VARCHAR",
}


@pytest.mark.parametrize("sql", WRITES.values(), ids=WRITES.keys())
def test_select_only_grant_refuses_every_write(attacker, sql):
    source, target, _ = attacker
    before = source.execute("SELECT * FROM users ORDER BY id").fetchall()

    try:
        target.execute(sql)
    except Exception:  # noqa: BLE001 -- the source is checked either way
        pass

    after = source.execute("SELECT * FROM users ORDER BY id").fetchall()
    assert after == before, f"source changed\n  before {before}\n  after  {after}"


READING_DELETES = {
    "where": "DELETE FROM app.main.users WHERE tenant = 2",
    "returning": "DELETE FROM app.main.users RETURNING id",
}


@pytest.mark.parametrize("sql", READING_DELETES.values(), ids=READING_DELETES.keys())
def test_granted_delete_reading_rows_cannot_reach_rows_the_policy_hides(attacker_granted_delete, sql):
    """
    ATTACK  a delete that reads its rows, under an unrestricted delete grant
    BREACH  a tenant-2 row disappears
    PASS    tenant 2 intact

    Reading the rows takes the select policy, so it confines the delete.
    """
    source, target = attacker_granted_delete

    try:
        target.execute(sql)
    except Exception:  # noqa: BLE001
        pass

    survivors = {row[0] for row in source.execute("SELECT id FROM users").fetchall()}
    assert HIDDEN <= survivors, f"deleted hidden rows {sorted(HIDDEN - survivors)}"


def test_granted_delete_reading_no_row_reaches_what_it_grants(attacker_granted_delete):
    """A delete reading nothing is governed by the delete policy alone, here `true`."""
    source, target = attacker_granted_delete

    assert target.execute("DELETE FROM app.main.users").fetchall() == [(4,)]
    assert source.execute("SELECT count(*) FROM users").fetchall() == [(0,)]


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE app.main.users SET tenant = 3 WHERE id = 1",
        "UPDATE app.main.users SET tenant = tenant + 1",
        "DELETE FROM app.main.users WHERE id = 1",
        "DELETE FROM app.main.users RETURNING id",
        "SELECT * FROM app.main.users",
    ],
)
def test_write_grant_without_select_refuses_reading_rows(bridged, sql):
    source, target, handshake = bridged
    source.execute(SETUP)
    handshake(source, target, {"users": {"update": "true", "delete": "true"}})
    before = source.execute("SELECT * FROM users ORDER BY id").fetchall()

    with pytest.raises(duckdb.PermissionException, match="does not have 'select' permission"):
        target.execute(sql)
    assert source.execute("SELECT * FROM users ORDER BY id").fetchall() == before


def test_write_grant_without_select_takes_writes_reading_no_row(bridged):
    source, target, handshake = bridged
    source.execute(SETUP)
    handshake(source, target, {"users": {"update": "true", "delete": "true"}})

    assert target.execute("UPDATE app.main.users SET tenant = 9").fetchall() == [(4,)]
    assert target.execute("DELETE FROM app.main.users").fetchall() == [(4,)]
    assert source.execute("SELECT count(*) FROM users").fetchall() == [(0,)]
