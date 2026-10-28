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
    source.execute("SELECT vcatbridge_set_policy(?, 'main', 'create', 'true')", [bridge_id])
    yield source, target, calls
    source.close()
    target.close()


def try_execute(con, sql):
    try:
        con.execute(sql)
    except Exception:  # noqa: BLE001 -- a refusal is the pass
        pass


def catalog_snapshot(source):
    return (
        source.execute("SELECT table_name FROM duckdb_tables() ORDER BY 1").fetchall(),
        source.execute("SELECT column_name, column_default FROM duckdb_columns() ORDER BY 1").fetchall(),
    )


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
    before = catalog_snapshot(source)

    with pytest.raises(duckdb.PermissionException, match=refusal):
        target.execute(ddl)

    assert catalog_snapshot(source) == before


# --- E6: ALTER COLUMN TYPE ... USING runs its expression on the source ---------------------------


def test_alter_type_using_cannot_call_source_only_function(ddl_world):
    _, target, calls = ddl_world
    try_execute(target, "ALTER TABLE app.main.users ALTER tenant TYPE BIGINT USING source_only(tenant)")

    assert calls == [], f"source_only ran on the source with {calls}"


def test_alter_type_using_cannot_expand_source_macro(ddl_world):
    source, target, _ = ddl_world
    source.execute("CREATE MACRO owner_macro(x) AS 'OWNER_MACRO_RAN'")
    try_execute(target, "ALTER TABLE app.main.users ALTER tenant TYPE VARCHAR USING owner_macro(tenant)")

    assert source.execute("SELECT count(*) FROM users WHERE tenant::VARCHAR = 'OWNER_MACRO_RAN'").fetchone() == (0,)


def test_alter_type_using_cannot_read_source_setting(ddl_world):
    source, target, _ = ddl_world
    source.execute("SET GLOBAL http_proxy_password = 'SOURCE_SECRET'")
    try_execute(
        target, "ALTER TABLE app.main.users ALTER tenant TYPE VARCHAR USING current_setting('http_proxy_password')"
    )

    assert source.execute("SELECT count(*) FROM users WHERE tenant::VARCHAR = 'SOURCE_SECRET'").fetchone() == (0,)


#: id -> (field path to add, struct-bearing column the target's table declares)
ADD_FIELD = {
    "struct": ("s.h", "s STRUCT(a INTEGER)"),
    "list_of_struct": ("l.element.h", "l STRUCT(a INTEGER)[]"),
}


@pytest.mark.parametrize("field, column", ADD_FIELD.values(), ids=ADD_FIELD.keys())
def test_add_field_default_is_refused(ddl_world, field, column):
    source, target, _ = ddl_world
    source.execute("SET GLOBAL http_proxy_password = 'SOURCE_SECRET'")
    target.execute(f"CREATE TABLE app.main.nested(id INTEGER PRIMARY KEY, {column})")
    before = catalog_snapshot(source)

    with pytest.raises(duckdb.PermissionException, match="must be a constant"):
        target.execute(
            f"ALTER TABLE app.main.nested ADD COLUMN {field} VARCHAR DEFAULT current_setting('http_proxy_password')"
        )

    assert catalog_snapshot(source) == before


CONSTANT_DEFAULTS = [
    "CREATE TABLE app.main.k(id INTEGER PRIMARY KEY, x INTEGER DEFAULT 7, d DATE DEFAULT '2020-01-01'::DATE)",
    "ALTER TABLE app.main.users ADD COLUMN note VARCHAR DEFAULT 'x'",
    "ALTER TABLE app.main.users ALTER COLUMN tenant SET DEFAULT -1",
]


@pytest.mark.parametrize("ddl", CONSTANT_DEFAULTS, ids=["create", "add_column", "set_default"])
def test_constant_default_is_allowed(ddl_world, ddl):
    _, target, _ = ddl_world
    target.execute(ddl)
