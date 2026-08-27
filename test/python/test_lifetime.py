"""Lifetime and teardown -- the failure modes the docs warn about loudest.

The registry holds a reference to the source DatabaseInstance, which is what makes a missing
unregister a leak rather than a nuisance. Same-instance tests cannot observe any of this.
"""

import subprocess
import sys
import textwrap

import pytest

from conftest import EXTENSION, REPO_ROOT, bridge, new_connection

PRELUDE = f"""
import duckdb
con_kwargs = dict(config={{"allow_unsigned_extensions": "true"}})
def connect():
    c = duckdb.connect(**con_kwargs)
    c.execute("LOAD '{EXTENSION}'")
    return c
"""


def run_script(body):
    script = PRELUDE + textwrap.dedent(body)
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True, text=True, timeout=60, cwd=REPO_ROOT,
    )


@pytest.fixture(autouse=True)
def _needs_extension():
    if not EXTENSION.exists():
        pytest.skip(f"extension not built: {EXTENSION} -- run `make release`")


def test_source_connection_can_close_while_the_bridge_is_live():
    """The registry pins the source DatabaseInstance, so the target keeps reading after the source
    connection is gone. This is the reference that makes teardown mandatory."""
    source = new_connection()
    target = new_connection()
    source.execute("CREATE TABLE users(id INTEGER PRIMARY KEY); INSERT INTO users VALUES (1), (2)")
    bridge(source, target, {"users": "read"})
    assert target.execute("SELECT count(*) FROM app.main.users").fetchone() == (2,)

    source.close()
    assert target.execute("SELECT count(*) FROM app.main.users").fetchone() == (2,)
    target.execute("SELECT vcat_unregister_bridge('app', 'main')").fetchall()
    target.close()


def test_target_detach_without_unregister_then_reattach():
    source = new_connection()
    target = new_connection()
    source.execute("CREATE TABLE users(id INTEGER PRIMARY KEY); INSERT INTO users VALUES (1)")
    bridge_id = bridge(source, target, {"users": "read"})
    target.execute("DETACH app")
    # The id is still claimed -- DETACH frees nothing, which is exactly what the docs warn about.
    token = source.execute(
        "SELECT vcat_register_source(?, 'memory', 'main', MAP {'users': 'read'}, MAP {}::MAP(VARCHAR, VARCHAR))",
        [bridge_id],
    ).fetchone()[0]
    target.execute("ATTACH ':memory:' AS app (TYPE virtual_catalog)")
    with pytest.raises(Exception, match="already exists"):
        target.execute("SELECT vcat_setup_bridge(?, ?, 'app', 'main')", [bridge_id, token]).fetchall()
    source.close()
    target.close()


def test_process_exits_cleanly_with_a_bridge_still_bound():
    result = run_script("""
        source = connect()
        target = connect()
        source.execute("CREATE TABLE users(id INTEGER PRIMARY KEY)")
        target.execute("ATTACH ':memory:' AS app (TYPE virtual_catalog)")
        tok = source.execute(
            "SELECT vcat_register_source('leak', 'memory', 'main', MAP {'users': 'read'}, MAP {}::MAP(VARCHAR, VARCHAR))"
        ).fetchone()[0]
        target.execute("SELECT vcat_setup_bridge('leak', ?, 'app', 'main')", [tok]).fetchall()
        print(target.execute("SELECT count(*) FROM app.main.users").fetchone()[0])
    """)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "0"


def test_process_exits_cleanly_after_unregister():
    result = run_script("""
        source = connect()
        target = connect()
        source.execute("CREATE TABLE users(id INTEGER PRIMARY KEY)")
        target.execute("ATTACH ':memory:' AS app (TYPE virtual_catalog)")
        tok = source.execute(
            "SELECT vcat_register_source('tidy', 'memory', 'main', MAP {'users': 'read'}, MAP {}::MAP(VARCHAR, VARCHAR))"
        ).fetchone()[0]
        target.execute("SELECT vcat_setup_bridge('tidy', ?, 'app', 'main')", [tok]).fetchall()
        target.execute("SELECT vcat_unregister_bridge('app', 'main')").fetchall()
        source.close()
        target.close()
        print("ok")
    """)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"


def test_process_exits_cleanly_with_the_source_closed_first():
    result = run_script("""
        source = connect()
        target = connect()
        source.execute("CREATE TABLE users(id INTEGER PRIMARY KEY); INSERT INTO users VALUES (1)")
        target.execute("ATTACH ':memory:' AS app (TYPE virtual_catalog)")
        tok = source.execute(
            "SELECT vcat_register_source('orphan', 'memory', 'main', MAP {'users': 'read'}, MAP {}::MAP(VARCHAR, VARCHAR))"
        ).fetchone()[0]
        target.execute("SELECT vcat_setup_bridge('orphan', ?, 'app', 'main')", [tok]).fetchall()
        source.close()
        print(target.execute("SELECT count(*) FROM app.main.users").fetchone()[0])
    """)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "1"


def test_process_exits_cleanly_with_a_provider_still_registered():
    result = run_script("""
        import pyarrow as pa
        VARCHAR = duckdb.sqltype("VARCHAR")
        BLOB = duckdb.sqltype("BLOB")
        con = connect()
        con.execute("ATTACH ':memory:' AS app (TYPE virtual_catalog)")
        schema = pa.schema([pa.field("id", pa.int32())])
        con.create_function("l", lambda: "t", [], VARCHAR)
        con.create_function("s", lambda n: schema.serialize().to_pybytes(), [VARCHAR], BLOB)
        con.execute(
            "SELECT vcat_register_provider('app', 'main', 'p', 'l', 's', 'sc', '', '', '', '')"
        ).fetchall()
        print(con.execute("SELECT count(*) FROM vcat_table_permissions('app', schema := 'main')").fetchone()[0])
    """)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "1"


def test_many_bridges_set_up_and_torn_down():
    """Registry churn: ids must actually be released, not just unbound."""
    source = new_connection()
    source.execute("CREATE TABLE users(id INTEGER PRIMARY KEY); INSERT INTO users VALUES (1)")
    for _ in range(25):
        target = new_connection()
        bridge(source, target, {"users": "read"})
        assert target.execute("SELECT count(*) FROM app.main.users").fetchone() == (1,)
        target.execute("SELECT vcat_unregister_bridge('app', 'main')").fetchall()
        target.close()
    source.close()


def test_bridge_id_is_reusable_after_unregister():
    source = new_connection()
    target = new_connection()
    source.execute("CREATE TABLE users(id INTEGER PRIMARY KEY); INSERT INTO users VALUES (1)")
    bridge_id = bridge(source, target, {"users": "read"})
    target.execute("SELECT vcat_unregister_bridge('app', 'main')").fetchall()

    token = source.execute(
        "SELECT vcat_register_source(?, 'memory', 'main', MAP {'users': 'read'}, MAP {}::MAP(VARCHAR, VARCHAR))",
        [bridge_id],
    ).fetchone()[0]
    target.execute("SELECT vcat_setup_bridge(?, ?, 'app', 'main')", [bridge_id, token]).fetchall()
    assert target.execute("SELECT count(*) FROM app.main.users").fetchone() == (1,)
    target.execute("SELECT vcat_unregister_bridge('app', 'main')").fetchall()
    source.close()
    target.close()
