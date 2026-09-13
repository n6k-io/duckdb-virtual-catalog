"""ATTACK  target writes to a table granted select and nothing else
BREACH  the source table changes
PASS    the source is identical afterwards

The source is compared whether or not the statement was refused: a write that errors after already
reaching the source is still a breach.
"""

import pytest

from security.world import HIDDEN

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


def test_granted_delete_cannot_reach_rows_the_policy_hides(attacker_granted_delete):
    """
    ATTACK  delete by a predicate matching only rows the select policy hides
    BREACH  a tenant-2 row disappears
    PASS    tenant 2 intact

    DELETE resolves rows through a scan, so the select policy is what confines it even though the
    delete grant itself is unrestricted.
    """
    source, target = attacker_granted_delete

    try:
        target.execute("DELETE FROM app.main.users WHERE tenant = 2")
    except Exception:  # noqa: BLE001
        pass

    survivors = {row[0] for row in source.execute("SELECT id FROM users").fetchall()}
    assert HIDDEN <= survivors, f"deleted hidden rows {sorted(HIDDEN - survivors)}"
