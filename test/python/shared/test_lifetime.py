"""Lifetime and teardown -- the failure modes the docs warn about loudest.

The attached catalog holds a reference to the source DatabaseInstance, which is what makes DETACH
the thing that releases it. Same-instance tests cannot observe any of this.
"""

import subprocess
import sys
import textwrap

import pytest

from conftest import (
    BRIDGE_EXTENSION,
    PROVIDER_EXTENSION,
    READ,
    REPO_ROOT,
    bridge,
    new_connection,
    unbridge,
)

# Both loaded: the subprocess cases below cover a bridge and a provider, and the script is shared.
PRELUDE = f"""
import duckdb
con_kwargs = dict(config={{"allow_unsigned_extensions": "true"}})
def connect():
    c = duckdb.connect(**con_kwargs)
    c.execute("LOAD '{BRIDGE_EXTENSION}'")
    c.execute("LOAD '{PROVIDER_EXTENSION}'")
    return c
"""


def run_script(body):
    script = PRELUDE + textwrap.dedent(body)
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=60,
        cwd=REPO_ROOT,
    )


@pytest.fixture(autouse=True)
def _needs_extension():
    for extension in (BRIDGE_EXTENSION, PROVIDER_EXTENSION):
        if not extension.exists():
            pytest.skip(f"extension not built: {extension} -- run `make release`")


def test_source_connection_can_close_while_the_bridge_is_live():
    """The catalog pins the source DatabaseInstance, so the target keeps reading after the source
    connection is gone. This is the reference DETACH releases."""
    source = new_connection()
    target = new_connection()
    source.execute("CREATE TABLE users(id INTEGER PRIMARY KEY); INSERT INTO users VALUES (1), (2)")
    bridge(source, target, {"users": READ})
    assert target.execute("SELECT count(*) FROM app.main.users").fetchone() == (2,)

    source.close()
    assert target.execute("SELECT count(*) FROM app.main.users").fetchone() == (2,)
    unbridge(target)
    target.close()


def test_target_detach_then_reattach():
    source = new_connection()
    target = new_connection()
    source.execute("CREATE TABLE users(id INTEGER PRIMARY KEY); INSERT INTO users VALUES (1)")
    bridge_id = bridge(source, target, {"users": READ})
    target.execute("DETACH app")

    token = source.execute("SELECT bridge_register_source(?, 'memory')", [bridge_id]).fetchone()[0]
    source.execute("SELECT bridge_policy(?, 'main.users', 'select', 'true')", [bridge_id])
    target.execute(f"ATTACH '' AS app (TYPE virtual_catalog_bridge, ID '{bridge_id}', TOKEN '{token}')")
    assert target.execute("SELECT count(*) FROM app.main.users").fetchone() == (1,)
    source.close()
    target.close()


def test_process_exits_cleanly_with_a_bridge_still_bound():
    result = run_script(
        """
        source = connect()
        target = connect()
        source.execute("CREATE TABLE users(id INTEGER PRIMARY KEY)")
        source.execute("SELECT bridge_register_source('leak', 'memory', 'tok_leak')")
        source.execute("SELECT bridge_policy('leak', 'main.users', 'select', 'true')")
        target.execute("ATTACH '' AS app (TYPE virtual_catalog_bridge, ID 'leak', TOKEN 'tok_leak')")
        print(target.execute("SELECT count(*) FROM app.main.users").fetchone()[0])
    """
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "0"


def test_process_exits_cleanly_after_unregister():
    result = run_script(
        """
        source = connect()
        target = connect()
        source.execute("CREATE TABLE users(id INTEGER PRIMARY KEY)")
        source.execute("SELECT bridge_register_source('tidy', 'memory', 'tok_tidy')")
        source.execute("SELECT bridge_policy('tidy', 'main.users', 'select', 'true')")
        target.execute("ATTACH '' AS app (TYPE virtual_catalog_bridge, ID 'tidy', TOKEN 'tok_tidy')")
        target.execute("DETACH app")
        source.close()
        target.close()
        print("ok")
    """
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"


def test_process_exits_cleanly_with_the_source_closed_first():
    result = run_script(
        """
        source = connect()
        target = connect()
        source.execute("CREATE TABLE users(id INTEGER PRIMARY KEY); INSERT INTO users VALUES (1)")
        source.execute("SELECT bridge_register_source('orphan', 'memory', 'tok_orphan')")
        source.execute("SELECT bridge_policy('orphan', 'main.users', 'select', 'true')")
        target.execute("ATTACH '' AS app (TYPE virtual_catalog_bridge, ID 'orphan', TOKEN 'tok_orphan')")
        source.close()
        print(target.execute("SELECT count(*) FROM app.main.users").fetchone()[0])
    """
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "1"


def test_process_exits_cleanly_with_a_provider_still_registered():
    result = run_script(
        """
        import pyarrow as pa
        VARCHAR = duckdb.sqltype("VARCHAR")
        BLOB = duckdb.sqltype("BLOB")
        con = connect()
        con.execute("ATTACH ':memory:' AS app (TYPE virtual_catalog_provider)")
        schema = pa.schema([pa.field("id", pa.int32())])
        con.create_function("l", lambda: "main.t", [], VARCHAR)
        con.create_function("s", lambda n: schema.serialize().to_pybytes(), [VARCHAR], BLOB)
        con.execute(
            "SELECT provider_register('app', 'l', 's', 'sc', '', '', '', '')"
        ).fetchall()
        print(con.execute("SELECT count(*) FROM provider_table_permissions('app', schema := 'main')").fetchone()[0])
    """
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "1"


def test_many_bridges_set_up_and_torn_down():
    """Attach churn: the pending registration must actually be released, not just unbound."""
    source = new_connection()
    source.execute("CREATE TABLE users(id INTEGER PRIMARY KEY); INSERT INTO users VALUES (1)")
    for _ in range(25):
        target = new_connection()
        bridge(source, target, {"users": READ})
        assert target.execute("SELECT count(*) FROM app.main.users").fetchone() == (1,)
        unbridge(target)
        target.close()
    source.close()


def test_bridge_id_is_reusable_after_detach():
    source = new_connection()
    target = new_connection()
    source.execute("CREATE TABLE users(id INTEGER PRIMARY KEY); INSERT INTO users VALUES (1)")
    bridge_id = bridge(source, target, {"users": READ})
    unbridge(target)

    token = source.execute("SELECT bridge_register_source(?, 'memory')", [bridge_id]).fetchone()[0]
    source.execute("SELECT bridge_policy(?, 'main.users', 'select', 'true')", [bridge_id])
    target.execute(f"ATTACH '' AS app (TYPE virtual_catalog_bridge, ID '{bridge_id}', TOKEN '{token}')")
    assert target.execute("SELECT count(*) FROM app.main.users").fetchone() == (1,)
    unbridge(target)
    source.close()
    target.close()


def test_a_schema_dropped_and_recreated_gets_a_fresh_wrapper(source, target):
    """The dynamic-load counterpart of test/sql/virtual_catalog_schema_recreate.test, which does not
    fault in the statically-linked unittest binary. VirtualCatalog memoises one wrapper per schema
    NAME and never erases, so without an identity check the wrapper outlives the DuckSchemaEntry it
    holds a reference to."""
    source.execute("CREATE TABLE anchor(id INTEGER PRIMARY KEY)")
    bridge(source, target, {"anchor": READ})
    target.execute("CREATE SCHEMA app.s")
    target.execute("CREATE TABLE app.s.t(id INTEGER)")
    target.execute("INSERT INTO app.s.t VALUES (1)")
    assert target.execute("SELECT * FROM app.s.t").fetchall() == [(1,)]

    target.execute("DROP SCHEMA app.s CASCADE")
    target.execute("CREATE SCHEMA app.s")
    target.execute("CREATE TABLE app.s.t(id INTEGER)")
    target.execute("INSERT INTO app.s.t VALUES (2)")
    assert target.execute("SELECT * FROM app.s.t").fetchall() == [(2,)]
    assert target.execute(
        "SELECT table_name FROM duckdb_tables() WHERE database_name = 'app' AND schema_name = 's'"
    ).fetchall() == [("t",)]


def test_create_or_replace_schema_gets_a_fresh_wrapper(source, target):
    """CREATE OR REPLACE SCHEMA reaches DuckCatalog's non-virtual drop, so the wrapper swap is the
    only thing that notices the underlying entry was replaced."""
    source.execute("CREATE TABLE anchor(id INTEGER PRIMARY KEY)")
    bridge(source, target, {"anchor": READ})
    target.execute("CREATE SCHEMA app.s")
    target.execute("CREATE TABLE app.s.t(id INTEGER)")
    assert target.execute("SELECT count(*) FROM app.s.t").fetchone() == (0,)
    target.execute("DROP TABLE app.s.t")

    target.execute("CREATE OR REPLACE SCHEMA app.s")
    assert target.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'app' AND schema_name = 's'"
    ).fetchone() == (0,)
    target.execute("CREATE TABLE app.s.t(id INTEGER)")
    target.execute("INSERT INTO app.s.t VALUES (7)")
    assert target.execute("SELECT * FROM app.s.t").fetchall() == [(7,)]
