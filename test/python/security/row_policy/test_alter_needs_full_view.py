"""ATTACK  target ALTERs a table whose select policy hides rows, so the source rewrites rows the target
        cannot see (G4: `ALTER ... TYPE ... USING` quotes a hidden value in its error)
BREACH  the ALTER reaches the source, or its error names a hidden value
PASS    refused before the source runs it, unless the select policy folds to true for this context
"""

import duckdb
import pytest

from conftest import bridge, new_connection, unique_id

SETUP = """
CREATE TABLE users(id INTEGER PRIMARY KEY, tenant INTEGER, secret VARCHAR, n INTEGER);
INSERT INTO users VALUES (1, 1, '1', 10), (2, 1, '2', 20), (3, 2, 'TOPSECRET', 30), (4, 2, 'hidden4', 40);
"""

LEAKING_ALTER = "ALTER TABLE app.main.users ALTER COLUMN secret TYPE INTEGER USING secret::INTEGER"
PLAIN_ALTER = "ALTER TABLE app.main.users ALTER COLUMN n TYPE BIGINT"


def column_type(source, column):
    query = "SELECT data_type FROM duckdb_columns() WHERE table_name = 'users' AND column_name = ?"
    return source.execute(query, [column]).fetchone()[0]


@pytest.fixture
def pair():
    source, target = new_connection(), new_connection()
    source.execute(SETUP)
    yield source, target
    source.close()
    target.close()


HIDING_GRANTS = {
    "select_policy": {"select": "tenant = 1", "alter": "true"},
    "alter_only": {"alter": "true"},
}


@pytest.mark.parametrize("grants", HIDING_GRANTS.values(), ids=HIDING_GRANTS.keys())
@pytest.mark.parametrize("sql", [LEAKING_ALTER, PLAIN_ALTER], ids=["using", "plain"])
def test_alter_refused_when_rows_are_hidden(pair, grants, sql):
    source, target = pair
    bridge(source, target, {"users": grants})

    with pytest.raises(duckdb.PermissionException, match="full table") as refused:
        target.execute(sql)

    assert "TOPSECRET" not in str(refused.value)
    assert column_type(source, "secret") == "VARCHAR"
    assert column_type(source, "n") == "INTEGER"


def test_alter_allowed_when_nothing_is_hidden(pair):
    source, target = pair
    bridge(source, target, {"users": {"select": "true", "alter": "true"}})

    target.execute(PLAIN_ALTER)

    assert column_type(source, "n") == "BIGINT"


def gated_bridge(source, target, user):
    """select `$user = 'admin' OR tenant = $tenant`, alter `$user = 'admin'`. Returns the bridge id."""
    bridge_id = unique_id()
    token = source.execute("SELECT vcatbridge_register_source(?, 'memory')", [bridge_id]).fetchone()[0]
    source.execute("SELECT vcatbridge_set_context(?, 'user', ?)", [bridge_id, user])
    source.execute("SELECT vcatbridge_set_context(?, 'tenant', 1)", [bridge_id])
    source.execute(
        "SELECT vcatbridge_set_policy(?, 'main.users', 'select', '$user = ''admin'' OR tenant = $tenant')", [bridge_id]
    )
    source.execute("SELECT vcatbridge_set_policy(?, 'main.users', 'alter', '$user = ''admin''')", [bridge_id])
    target.execute(f"ATTACH '' AS app (TYPE vcatbridge, ID '{bridge_id}', TOKEN '{token}')")
    return bridge_id


def test_context_gated_alter_allowed_for_admin(pair):
    source, target = pair
    gated_bridge(source, target, "admin")

    target.execute(PLAIN_ALTER)

    assert column_type(source, "n") == "BIGINT"


def test_context_gated_alter_refused_for_other_user(pair):
    source, target = pair
    gated_bridge(source, target, "bob")

    with pytest.raises(duckdb.PermissionException):
        target.execute(LEAKING_ALTER)

    assert column_type(source, "secret") == "VARCHAR"


def test_context_is_read_at_alter_time(pair):
    source, target = pair
    bridge_id = gated_bridge(source, target, "admin")
    source.execute("SELECT vcatbridge_set_context(?, 'user', 'bob')", [bridge_id])

    with pytest.raises(duckdb.PermissionException):
        target.execute(PLAIN_ALTER)

    assert column_type(source, "n") == "INTEGER"


def test_alter_grant_may_not_reference_a_column(pair):
    source, _ = pair
    bridge_id = unique_id()
    source.execute("SELECT vcatbridge_register_source(?, 'memory')", [bridge_id])

    with pytest.raises(duckdb.Error, match="column"):
        source.execute("SELECT vcatbridge_set_policy(?, 'main.users', 'alter', 'tenant = 1')", [bridge_id])
