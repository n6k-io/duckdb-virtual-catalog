"""ATTACK  target creates a table whose foreign key references a policy-restricted table (G8)
BREACH  an insert succeeds or fails depending on whether a hidden key exists, or the source owner
        can no longer drop the referenced table
PASS    the DDL is refused and no table is created
"""

import duckdb
import pytest

from conftest import bridge, new_connection
from security.world import POLICY, SETUP, outcome

FOREIGN_KEYS = {
    "inline": "CREATE TABLE probe(uid INTEGER REFERENCES users(id))",
    "table": "CREATE TABLE probe(uid INTEGER, FOREIGN KEY (uid) REFERENCES users(id))",
    "qualified": "CREATE TABLE probe(uid INTEGER, FOREIGN KEY (uid) REFERENCES main.users(id))",
}


@pytest.fixture
def world():
    """Builds a fresh source/target pair per call. Yields make(setup, grants, **bridge_options) -> (source, target, bridge_id)."""
    opened = []

    def make(setup, grants, **options):
        source, target = new_connection(), new_connection()
        opened.extend([source, target])
        source.execute(setup)
        return source, target, bridge(source, target, grants, **options)

    yield make
    for con in opened:
        con.close()


@pytest.mark.parametrize("ddl", FOREIGN_KEYS.values(), ids=FOREIGN_KEYS.keys())
def test_foreign_key_is_refused(world, ddl):
    source, target, bridge_id = world(SETUP, {"users": {"select": POLICY}})
    source.execute("SELECT vcatbridge_set_policy(?, 'main', 'create', 'true')", [bridge_id])
    target.execute("USE app")

    with pytest.raises(duckdb.PermissionException, match="foreign key"):
        target.execute(ddl)

    assert source.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name = 'probe'").fetchone() == (0,)


# --- G9: an owner-declared foreign key to an ungranted parent ------------------------------------

PARENT_SETUP = """
CREATE TABLE secret_orgs(id INTEGER PRIMARY KEY);
INSERT INTO secret_orgs VALUES (7);
CREATE TABLE users(id INTEGER PRIMARY KEY, tenant INTEGER, org INTEGER REFERENCES secret_orgs(id));
INSERT INTO users VALUES (1, 1, 7);
"""

#: Each probe is formatted with a parent key that exists, then one that does not.
PARENT_PROBES = {
    "insert": "INSERT INTO app.main.users VALUES (10, 1, {key})",
    "update": "UPDATE app.main.users SET org = {key} WHERE id = 1",
}

WRITES = {
    "insert": {"select": POLICY, "insert": ("true", POLICY)},
    "update": {"select": POLICY, "update": POLICY},
}


@pytest.mark.parametrize("grant", WRITES.values(), ids=WRITES.keys())
def test_write_over_ungranted_parent_is_refused(world, grant):
    with pytest.raises(duckdb.PermissionException, match="foreign key to 'main.secret_orgs'"):
        world(PARENT_SETUP, {"users": grant})


def test_write_over_policy_parent_is_refused(world):
    with pytest.raises(duckdb.PermissionException, match="foreign key to 'main.secret_orgs'"):
        world(PARENT_SETUP, {"users": WRITES["insert"], "secret_orgs": {"select": "id > 0"}})


def test_write_over_readable_parent_is_allowed(world):
    _, target, _ = world(PARENT_SETUP, {"users": WRITES["insert"], "secret_orgs": ["select"]})

    assert outcome(target, "INSERT INTO app.main.users VALUES (10, 1, 7)") == ("ok", [(1,)])


def test_write_over_self_reference_is_refused(world):
    setup = "CREATE TABLE users(id INTEGER PRIMARY KEY, tenant INTEGER, boss INTEGER REFERENCES users(id));"

    with pytest.raises(duckdb.PermissionException, match="foreign key to 'main.users'"):
        world(setup, {"users": WRITES["insert"]})


def test_write_granted_after_attach_is_refused_until_allowed(world):
    source, target, bridge_id = world(PARENT_SETUP, {"users": {"select": POLICY}})
    grant = "SELECT vcatbridge_set_policy(?, 'main.users', 'insert', ?)"

    with pytest.raises(duckdb.PermissionException, match="foreign key to 'main.secret_orgs'"):
        source.execute(grant, [bridge_id, POLICY])
    source.execute(
        "SELECT vcatbridge_set_policy(?, 'main.users', 'foreign_keys', 'add', 'main.secret_orgs')", [bridge_id]
    )
    source.execute(grant, [bridge_id, POLICY])

    assert outcome(target, "INSERT INTO app.main.users VALUES (10, 1, 7)") == ("ok", [(1,)])


def test_allow_needs_a_declared_foreign_key(world):
    source, _, bridge_id = world(PARENT_SETUP, {"users": {"select": POLICY}})

    with pytest.raises(duckdb.IOException, match="has no foreign key to 'main.users'"):
        source.execute(
            "SELECT vcatbridge_set_policy(?, 'main.secret_orgs', 'foreign_keys', 'add', 'main.users')", [bridge_id]
        )


def test_target_cannot_allow_a_foreign_key(world):
    _, target, bridge_id = world(PARENT_SETUP, {"users": {"select": POLICY}})

    with pytest.raises(duckdb.PermissionException, match="different source connection"):
        target.execute(
            "SELECT vcatbridge_set_policy(?, 'main.users', 'foreign_keys', 'add', 'main.secret_orgs')", [bridge_id]
        )


def test_allow_is_refused_after_finalize(world):
    source, _, bridge_id = world(PARENT_SETUP, {"users": {"select": POLICY}})
    source.execute("SELECT vcatbridge_finalize_policy(?)", [bridge_id])

    with pytest.raises(duckdb.PermissionException, match="finalized"):
        source.execute(
            "SELECT vcatbridge_set_policy(?, 'main.users', 'foreign_keys', 'add', 'main.secret_orgs')", [bridge_id]
        )


@pytest.mark.xfail(strict=True, reason="accepted: a foreign_keys entry reveals which parent keys exist")
@pytest.mark.parametrize("sql", PARENT_PROBES.values(), ids=PARENT_PROBES.keys())
def test_parent_foreign_key_is_not_an_ungranted_key_oracle(world, sql):
    grants = {"users": {"select": POLICY, "insert": ("true", POLICY), "update": POLICY}}
    present, absent = (
        outcome(world(PARENT_SETUP, grants, foreign_keys=[("users", "secret_orgs")])[1], sql.format(key=key))
        for key in (7, 8)
    )

    assert present == absent, f"an ungranted parent key changed the outcome\n  present {present}\n  absent  {absent}"


# --- G10: an ungranted child table whose foreign key references a granted table -----------------

CHILD_SETUP = """
CREATE TABLE users(id INTEGER PRIMARY KEY, tenant INTEGER);
INSERT INTO users VALUES (1, 1);
CREATE TABLE secret_payroll(id INTEGER PRIMARY KEY, uid INTEGER REFERENCES users(id));
"""

CHILD_PROBES = {
    "delete": "DELETE FROM app.main.users WHERE id = 1",
    "update": "UPDATE app.main.users SET id = 5 WHERE id = 1",
}


@pytest.mark.xfail(
    strict=True,
    reason="accepted: an ungranted table's foreign key error names the referencing column, not the referencing table",
)
@pytest.mark.parametrize("sql", CHILD_PROBES.values(), ids=CHILD_PROBES.keys())
def test_child_foreign_key_is_not_an_ungranted_row_oracle(world, sql):
    grants = {"users": ["select", "update", "delete", "drop"]}
    referenced, unreferenced = (
        outcome(world(CHILD_SETUP + payroll, grants)[1], sql)
        for payroll in ("INSERT INTO secret_payroll VALUES (1, 1)", "")
    )

    assert (
        referenced == unreferenced
    ), f"an ungranted child row changed the outcome\n  referenced   {referenced}\n  unreferenced {unreferenced}"


@pytest.mark.xfail(strict=True, reason="accepted: ddl grants may reveal ungranted table names")
def test_drop_error_does_not_name_ungranted_child_table(world):
    _, target, _ = world(CHILD_SETUP, {"users": ["select", "drop"]})

    result = outcome(target, "DROP TABLE app.main.users")

    assert "secret_payroll" not in str(result), f"error names an ungranted table: {result}"
