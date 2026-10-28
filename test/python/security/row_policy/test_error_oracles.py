"""ATTACK  target writes a predicate or DDL that errors depending on a hidden row
BREACH  the outcome differs between a world where the hidden row matches and one where it does not,
        or a hidden value appears in an error message
PASS    every pair of worlds gives the same outcome, and errors name only visible values

`users` here carries a `secret` column: visible rows hold castable values, hidden rows do not, so
any cast or error() evaluated on a hidden row shows up in the outcome.
"""

import pytest

from conftest import bridge, new_connection
from security.world import outcome

POLICY = "tenant = 1"

SETUP = """
CREATE TABLE users(id INTEGER PRIMARY KEY, tenant INTEGER, secret VARCHAR, n INTEGER);
INSERT INTO users VALUES (1, 1, '1', 10), (2, 1, '2', 20), (3, 2, 'TOPSECRET', 30), (4, 2, 'hidden4', 0);
INSERT INTO users SELECT i, 1 + i % 2, CASE WHEN i % 2 = 0 THEN i::VARCHAR ELSE 'H' || i END, i FROM range(5, 10000) t(i);
"""

HIDDEN_VALUES = ["TOPSECRET", "hidden4", "H5"]


@pytest.fixture
def world():
    """Builds a fresh source/target pair per call so a write probe cannot leak into the next."""
    opened = []

    def make(grants, setup=SETUP):
        source, target = new_connection(), new_connection()
        opened.extend([source, target])
        source.execute(setup)
        bridge(source, target, {"users": grants})
        return source, target

    yield make
    for con in opened:
        con.close()


def assert_no_hidden_value(result):
    if result[0] == "error":
        leaked = [v for v in HIDDEN_VALUES if v in result[2]]
        assert not leaked, f"error names hidden values {leaked}: {result[2]}"


# --- G1: a pushed filter must not run on rows the policy hides -----------------------------------

FILTERS = [
    "secret::INTEGER > 0",
    "CAST(secret AS INTEGER) >= 0",
    "100 // n > 0",
    "error(secret) IS NULL",
    "CASE WHEN tenant = 2 THEN error('leak:' || secret) END IS NULL",
    "id IN (3, 4) AND secret::INTEGER > 0",
]


@pytest.mark.parametrize("predicate", FILTERS)
def test_filter_never_sees_hidden_rows(world, predicate):
    _, target = world({"select": POLICY})
    result = outcome(target, f"SELECT id FROM app.main.users WHERE {predicate} ORDER BY id")
    assert_no_hidden_value(result)
    if predicate.startswith(("secret", "CAST", "100")):
        assert result[0] == "ok", f"filter ran on a hidden row: {result}"


# --- G2: conditional errors must not reveal whether a hidden row exists --------------------------

READ_PROBES = {
    "case_error": "SELECT id FROM app.main.users WHERE CASE WHEN secret = '{v}' THEN error('x') ELSE true END",
    "id_error": "SELECT id FROM app.main.users WHERE id = {v} AND error('x') IS NULL",
    "divide": "SELECT id FROM app.main.users WHERE 1 // (id - {v}) IS NOT NULL",
}

#: Each pair is (matches a hidden row, matches nothing).
GUESSES = {"case_error": ("TOPSECRET", "NOPE"), "id_error": ("3", "999999"), "divide": ("3", "999999")}


@pytest.mark.parametrize("probe", READ_PROBES, ids=READ_PROBES.keys())
def test_read_error_is_not_an_existence_oracle(world, probe):
    _, target = world({"select": POLICY})
    hit, miss = (outcome(target, READ_PROBES[probe].format(v=v)) for v in GUESSES[probe])
    assert hit == miss, f"hidden row changed the outcome\n  hit  {hit}\n  miss {miss}"


# --- G3: UPDATE/DELETE seams go through the same path ------------------------------------------

WRITE_GRANTS = {"select": POLICY, "update": "true", "delete": "true"}

WRITE_PAIRS = {
    "update_case": "UPDATE app.main.users SET n = n WHERE CASE WHEN secret = '{v}' THEN error('hit') ELSE true END",
    "update_set": "UPDATE app.main.users SET n = (CASE WHEN secret = '{v}' THEN error('hit') ELSE n END)",
    "delete_case": "DELETE FROM app.main.users WHERE CASE WHEN secret = '{v}' THEN error('hit') ELSE false END",
}

WRITES = [
    "UPDATE app.main.users SET n = n WHERE secret::INTEGER > 0",
    "UPDATE app.main.users SET n = 100 // n",
    "DELETE FROM app.main.users WHERE secret::INTEGER < 0",
    "DELETE FROM app.main.users WHERE 100 // n < 0",
]


@pytest.mark.parametrize("probe", WRITE_PAIRS, ids=WRITE_PAIRS.keys())
def test_write_error_is_not_an_existence_oracle(world, probe):
    results = []
    for guess in ("TOPSECRET", "NOPE"):
        _, target = world(WRITE_GRANTS)
        results.append(outcome(target, WRITE_PAIRS[probe].format(v=guess)))
    hit, miss = results
    assert hit == miss, f"hidden row changed the outcome\n  hit  {hit}\n  miss {miss}"


@pytest.mark.parametrize("sql", WRITES)
def test_write_predicate_never_sees_hidden_rows(world, sql):
    source, target = world(WRITE_GRANTS)
    hidden_before = source.execute("SELECT * FROM users WHERE tenant = 2 ORDER BY id").fetchall()

    result = outcome(target, sql)

    assert result[0] == "ok", f"write predicate ran on a hidden row: {result}"
    assert source.execute("SELECT * FROM users WHERE tenant = 2 ORDER BY id").fetchall() == hidden_before


# --- G6: ADD PRIMARY KEY must not reveal duplicates among hidden rows ----------------------------


def keyless_setup(hidden_tenant):
    return f"""
    CREATE TABLE users(id INTEGER, tenant INTEGER, secret VARCHAR, n INTEGER);
    INSERT INTO users VALUES (1, 1, '1', 10), (2, 1, '2', 20), (3, 2, 'x', 30), (4, {hidden_tenant}, '5', 40);
    """


@pytest.mark.parametrize("grants", [{"select": POLICY, "alter": "true"}, {"alter": "true"}], ids=["select", "alter"])
def test_add_primary_key_is_not_a_duplicate_oracle(world, grants):
    results = []
    for hidden_tenant in (2, 3):
        _, target = world(grants, keyless_setup(hidden_tenant))
        results.append(outcome(target, "ALTER TABLE app.main.users ADD PRIMARY KEY (tenant)"))
    duplicate, distinct = results
    assert duplicate == distinct, f"hidden duplicates changed the outcome\n  dup {duplicate}\n  distinct {distinct}"


# --- G5: SET NOT NULL reveals whether any hidden row is NULL -------------------------------------


def nullable_setup(hidden_n):
    return f"""
    CREATE TABLE users(id INTEGER, tenant INTEGER, secret VARCHAR, n INTEGER);
    INSERT INTO users VALUES (1, 1, '1', 10), (2, 1, '2', 20), (3, 2, 'x', {hidden_n}), (4, 2, '5', 40);
    """


@pytest.mark.parametrize("grants", [{"select": POLICY, "alter": "true"}, {"alter": "true"}], ids=["select", "alter"])
def test_set_not_null_is_not_a_null_oracle(world, grants):
    results = []
    for hidden_n in ("NULL", "30"):
        _, target = world(grants, nullable_setup(hidden_n))
        results.append(outcome(target, "ALTER TABLE app.main.users ALTER COLUMN n SET NOT NULL"))
    null, filled = results
    assert null == filled, f"a hidden NULL changed the outcome\n  null   {null}\n  filled {filled}"


# --- G7: CREATE in a create-granted schema reveals ungranted table names --------------------------

NAME_PROBES = {
    "create": "CREATE TABLE app.main.secret(x INTEGER)",
    "create_or_replace": "CREATE OR REPLACE TABLE app.main.secret(x INTEGER)",
    "rename": "CREATE TABLE app.main.tmp(x INTEGER); ALTER TABLE app.main.tmp RENAME TO secret",
}


@pytest.mark.xfail(strict=True, reason="accepted: fixing it needs target-created tables in their own namespace")
@pytest.mark.parametrize("sql", NAME_PROBES.values(), ids=NAME_PROBES.keys())
def test_create_is_not_an_ungranted_name_oracle(sql):
    results = []
    for setup in (f"CREATE TABLE secret(id INTEGER); {SETUP}", SETUP):
        source, target = new_connection(), new_connection()
        try:
            source.execute(setup)
            bridge_id = bridge(source, target, {"users": {"select": POLICY}})
            source.execute("SELECT vcatbridge_set_policy(?, 'main', 'create', 'true')", [bridge_id])
            results.append(outcome(target, sql))
        finally:
            source.close()
            target.close()
    present, absent = results
    assert present == absent, f"an ungranted name changed the outcome\n  present {present}\n  absent  {absent}"
