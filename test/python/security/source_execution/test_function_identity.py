"""ATTACK  target calls a function it bound locally whose name the source also has, as a different
        function (E4)
BREACH  the source's function runs on pushdown, or the source plan breaks the target database
PASS    the target's function is what runs, or the expression stays on the target
"""

import duckdb
import pytest

from conftest import bridge, new_connection
from security.world import POLICY, SETUP

#: Provided by postgres_scanner as CONSISTENT, so the bridge may push it down.
NAME = "postgres_hstore_to_json"
MARKER = "SOURCE_UDF_RAN"


@pytest.fixture
def shadowed():
    """The target loads postgres_scanner; the source has an unrelated UDF with the same name and
    signature. Yields (target, calls)."""
    calls = []

    def impostor(x):
        calls.append(x)
        return MARKER

    source, target = new_connection(), new_connection()
    source.execute("SET autoload_known_extensions = false")
    source.execute(SETUP)
    source.create_function(NAME, impostor, ["VARCHAR"], "VARCHAR")
    bridge(source, target, {"users": {"select": POLICY}})
    try:
        target.execute("LOAD postgres_scanner")
    except duckdb.Error as e:
        pytest.skip(f"postgres_scanner not available: {e}")
    yield target, calls
    source.close()
    target.close()


SHADOWED_QUERIES = [
    f"SELECT id FROM app.main.users WHERE {NAME}(tenant::VARCHAR) = '{MARKER}'",
    f"SELECT id, {NAME}(tenant::VARCHAR) FROM app.main.users",
]


@pytest.mark.parametrize("sql", SHADOWED_QUERIES, ids=["filter", "projection"])
def test_same_name_source_function_does_not_run(shadowed, sql):
    target, calls = shadowed
    try:
        result = str(target.execute(sql).fetchall())
    except duckdb.Error as e:
        result = str(e)

    assert calls == [], f"source function ran with {calls}"
    assert MARKER not in result
