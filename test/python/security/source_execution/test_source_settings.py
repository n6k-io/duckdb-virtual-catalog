"""ATTACK  target pushes a function whose bind reads settings, so it binds again on the source (E5)
BREACH  a source-only setting value comes back in a row, or changes which rows match
PASS    the function is refused or evaluated on the target
"""

import subprocess
import sys
import textwrap

import pytest

from conftest import HERE, REPO_ROOT, bridge, new_connection
from security.world import POLICY, SETUP, outcome

SOURCE_SECRET = "SOURCE_SECRET"


@pytest.fixture
def settings_world():
    source, target = new_connection(), new_connection()
    source.execute(SETUP)
    source.execute(f"SET GLOBAL http_proxy_password = '{SOURCE_SECRET}'")
    target.execute("SET GLOBAL http_proxy_password = 'target_value'")
    bridge(source, target, {"users": {"select": POLICY}})
    yield target
    source.close()
    target.close()


def test_projection_does_not_read_source_setting(settings_world):
    result = outcome(settings_world, "SELECT id, current_setting('http_proxy_password') FROM app.main.users")

    assert SOURCE_SECRET not in str(result), f"source setting leaked: {result}"


def test_filter_is_not_a_source_setting_oracle(settings_world):
    sql = "SELECT id FROM app.main.users WHERE current_setting('http_proxy_password') = '{v}' ORDER BY id"
    hit, miss = (outcome(settings_world, sql.format(v=v)) for v in (SOURCE_SECRET, "NOPE"))

    assert hit == miss, f"source setting changed the outcome\n  hit  {hit}\n  miss {miss}"


#: Run in a child: the breach is the process dying.
REBOUND_AGGREGATE = f"""
import sys
sys.path.insert(0, {str(HERE)!r})
from conftest import bridge, new_connection

source, target = new_connection(), new_connection()
source.execute("CREATE TABLE people(id INTEGER PRIMARY KEY, name VARCHAR)")
source.execute("INSERT INTO people VALUES (1, 'a'), (2, 'B')")
source.execute("SET GLOBAL default_collation = 'nocase'")
bridge(source, target, {{"people": {{"select": "true"}}}})
try:
    target.execute("SELECT list_aggregate([name], 'max') FROM app.main.people").fetchall()
except Exception:
    pass
"""


def test_aggregate_rebound_under_source_collation_does_not_crash():
    new_connection().close()
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(REBOUND_AGGREGATE)],
        capture_output=True,
        text=True,
        timeout=60,
        cwd=REPO_ROOT,
    )

    assert result.returncode == 0, f"process died with {result.returncode}\n{result.stderr}"
