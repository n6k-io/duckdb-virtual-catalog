"""ATTACK  target DDL carries an expression (DEFAULT, CHECK, GENERATED) that is stored on the source (E2)
BREACH  the expression runs on the source when the owner later inserts
PASS    the DDL is refused, or nothing target-authored runs on the owner's insert
"""

import duckdb
import pytest

from conftest import bridge, new_connection
from security.world import SETUP


@pytest.fixture
def ddl_world():
    """Target holds create on `main` and select + alter + update on `users` (crossing wants update for
    ADD COLUMN with a non-constant default). The source owns a UDF the target
    cannot bind. Yields (source, target, calls)."""
    calls = []

    def source_only(x):
        calls.append(x)
        return 99

    source, target = new_connection(), new_connection()
    source.execute(SETUP)
    source.create_function("source_only", source_only, ["INTEGER"], "INTEGER")
    bridge_id = bridge(source, target, {"users": {"select": "true", "alter": "true", "update": "true"}})
    source.execute("SELECT bridge_policy(?, 'main', 'create', 'true')", [bridge_id])
    yield source, target, calls
    source.close()
    target.close()


def try_execute(con, sql):
    try:
        con.execute(sql)
    except Exception:  # noqa: BLE001 -- a refusal is the pass
        pass


def source_has_table(source, name):
    return source.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name = ?", [name]).fetchone()[0] > 0


def test_set_default_cannot_name_a_source_only_function(ddl_world):
    source, target, calls = ddl_world
    try_execute(target, "ALTER TABLE app.main.users ALTER COLUMN tenant SET DEFAULT source_only(7)")

    source.execute("INSERT INTO users(id) VALUES (10)")

    assert calls == [], f"source_only ran on the owner's insert with {calls}"


def test_set_default_expression_does_not_run_on_owner_insert(ddl_world):
    source, target, _ = ddl_world
    source.execute("ALTER TABLE users ADD COLUMN note VARCHAR")
    try_execute(target, "ALTER TABLE app.main.users ALTER COLUMN note SET DEFAULT current_database()")

    source.execute("INSERT INTO users(id, tenant) VALUES (10, 1)")

    assert source.execute("SELECT note FROM users WHERE id = 10").fetchone() != ("memory",)


#: name -> (target DDL, owner insert, (query, value that proves the expression ran on the source))
CREATES = {
    "default": (
        "CREATE TABLE app.main.d(id INTEGER PRIMARY KEY, x VARCHAR DEFAULT current_database())",
        "INSERT INTO d(id) VALUES (1)",
        ("SELECT x FROM d", ("memory",)),
    ),
    "generated": (
        "CREATE TABLE app.main.g(id INTEGER PRIMARY KEY, x VARCHAR GENERATED ALWAYS AS (current_database() || id))",
        "INSERT INTO g(id) VALUES (1)",
        ("SELECT x FROM g", ("memory1",)),
    ),
}


@pytest.mark.parametrize("ddl, insert, ran", CREATES.values(), ids=CREATES.keys())
def test_create_table_expression_does_not_run_on_owner_insert(ddl_world, ddl, insert, ran):
    source, target, _ = ddl_world
    try_execute(target, ddl)
    name = insert.split()[2].split("(")[0]
    if not source_has_table(source, name):
        return

    source.execute(insert)

    query, proof = ran
    assert source.execute(query).fetchone() != proof, f"{ddl}\n  ran on the owner's insert"


def test_create_table_check_does_not_constrain_owner(ddl_world):
    source, target, _ = ddl_world
    try_execute(target, "CREATE TABLE app.main.c(id INTEGER PRIMARY KEY, x INTEGER CHECK (x < 5))")
    if not source_has_table(source, "c"):
        return

    source.execute("INSERT INTO c VALUES (1, 10)")


#: name -> (target DDL, refusal)
REFUSED = {
    "create_default": (
        "CREATE TABLE app.main.d(id INTEGER PRIMARY KEY, x VARCHAR DEFAULT current_database())",
        "must be a constant",
    ),
    "create_generated": (
        "CREATE TABLE app.main.g(id INTEGER PRIMARY KEY, x VARCHAR GENERATED ALWAYS AS (current_database() || id))",
        "generated column",
    ),
    "create_check": (
        "CREATE TABLE app.main.c(id INTEGER PRIMARY KEY, x INTEGER CHECK (x < 5))",
        "CHECK constraint",
    ),
    "add_column_default": (
        "ALTER TABLE app.main.users ADD COLUMN note VARCHAR DEFAULT current_database()",
        "must be a constant",
    ),
    "set_default": (
        "ALTER TABLE app.main.users ALTER COLUMN tenant SET DEFAULT abs(-7)",
        "must be a constant",
    ),
}


@pytest.mark.parametrize("ddl, refusal", REFUSED.values(), ids=REFUSED.keys())
def test_expression_in_target_ddl_is_refused(ddl_world, ddl, refusal):
    source, target, _ = ddl_world
    tables_before = source.execute("SELECT table_name FROM duckdb_tables() ORDER BY 1").fetchall()
    columns_before = source.execute("SELECT column_name, column_default FROM duckdb_columns() ORDER BY 1").fetchall()

    with pytest.raises(duckdb.PermissionException, match=refusal):
        target.execute(ddl)

    assert source.execute("SELECT table_name FROM duckdb_tables() ORDER BY 1").fetchall() == tables_before
    assert (
        source.execute("SELECT column_name, column_default FROM duckdb_columns() ORDER BY 1").fetchall()
        == columns_before
    )


CONSTANT_DEFAULTS = [
    "CREATE TABLE app.main.k(id INTEGER PRIMARY KEY, x INTEGER DEFAULT 7, d DATE DEFAULT '2020-01-01'::DATE)",
    "ALTER TABLE app.main.users ADD COLUMN note VARCHAR DEFAULT 'x'",
    "ALTER TABLE app.main.users ALTER COLUMN tenant SET DEFAULT -1",
]


@pytest.mark.parametrize("ddl", CONSTANT_DEFAULTS, ids=["create", "add_column", "set_default"])
def test_constant_default_is_allowed(ddl_world, ddl):
    _, target, _ = ddl_world
    target.execute(ddl)
