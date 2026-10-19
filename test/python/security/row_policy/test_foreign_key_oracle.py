"""ATTACK  target creates a table whose foreign key references a policy-restricted table (G8)
BREACH  an insert succeeds or fails depending on whether a hidden key exists, or the source owner
        can no longer drop the referenced table
PASS    hidden and absent keys behave the same, and the owner's DROP succeeds
"""

import duckdb
import pytest

from conftest import bridge, new_connection
from security.world import HIDDEN, POLICY, SETUP

ABSENT = 999

FOREIGN_KEYS = [
    "CREATE TABLE probe(uid INTEGER REFERENCES users(id))",
    "CREATE TABLE probe(uid INTEGER, FOREIGN KEY (uid) REFERENCES users(id))",
    "CREATE TABLE probe(uid INTEGER, FOREIGN KEY (uid) REFERENCES main.users(id))",
]


@pytest.fixture
def creator():
    """`users` under POLICY, plus an unrestricted create grant on `main`."""
    source, target = new_connection(), new_connection()
    source.execute(SETUP)
    bridge_id = bridge(source, target, {"users": {"select": POLICY}})
    source.execute("SELECT bridge_policy(?, 'main', 'create', 'true')", [bridge_id])
    target.execute("USE app")
    yield source, target
    source.close()
    target.close()


def outcome(con, sql):
    try:
        con.execute(sql)
        return "ok"
    except Exception as e:  # noqa: BLE001 -- the outcome itself is what is compared
        return f"{type(e).__name__}: {e}"


@pytest.mark.parametrize("ddl", FOREIGN_KEYS, ids=["inline", "table", "qualified"])
def test_foreign_key_is_not_an_existence_oracle(creator, ddl):
    _, target = creator
    outcome(target, ddl)

    hidden = outcome(target, f"INSERT INTO probe VALUES ({min(HIDDEN)})")
    absent = outcome(target, f"INSERT INTO probe VALUES ({ABSENT})")

    assert hidden == absent, f"hidden key changed the outcome\n  hidden {hidden}\n  absent {absent}"


@pytest.mark.parametrize("ddl", FOREIGN_KEYS, ids=["inline", "table", "qualified"])
def test_foreign_key_is_refused(creator, ddl):
    source, target = creator
    with pytest.raises(duckdb.PermissionException, match="foreign key"):
        target.execute(ddl)

    assert source.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name = 'probe'").fetchone() == (0,)


@pytest.mark.parametrize("ddl", FOREIGN_KEYS, ids=["inline", "table", "qualified"])
def test_foreign_key_does_not_pin_source_table(creator, ddl):
    source, target = creator
    outcome(target, ddl)

    source.execute("DROP TABLE users")
