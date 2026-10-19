"""ATTACK  target SQL on a locked-down instance allocates outside the buffer manager, or runs forever (L3)
BREACH  process RSS climbs far past memory_limit, or the query is still running at the deadline
PASS    the query is refused or interrupted inside the caps

Each probe runs in a child process the parent watches and kills, so a breach cannot take the host
with it.
"""

import subprocess
import sys
import time

import pytest

from conftest import REPO_ROOT

pytestmark = pytest.mark.xfail(
    strict=True, reason="Known issue L3: memory_limit covers only buffer-managed memory, and there is no query timeout"
)

TEST_DIR = REPO_ROOT / "test" / "python"

MEMORY_LIMIT_MB = 256
#: Slack for the interpreter, the extension and allocator overhead on top of memory_limit.
RSS_CEILING_MB = 4 * MEMORY_LIMIT_MB
DEADLINE_S = 20

PROBE = """
import sys
sys.path.insert(0, {test_dir!r})
from conftest import new_connection
con = new_connection()
con.execute("SELECT virtual_catalog_lockdown('{limit}MB', 1)")
print("LOCKED", flush=True)
sql = {sql}
try:
    con.execute(sql).fetchall()
    print("FINISHED", flush=True)
except Exception as e:
    print("REFUSED", type(e).__name__, e, flush=True)
"""


def rss_mb(pid):
    out = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
    return int(out) // 1024 if out else 0


def watch(sql, *, built=False):
    """Returns (peak RSS in MB, why the child was killed or None, its stdout). A `built` sql is a Python
    expression the child evaluates, for statements too large to pass on its command line."""
    script = PROBE.format(test_dir=str(TEST_DIR), limit=MEMORY_LIMIT_MB, sql=sql if built else repr(sql))
    child = subprocess.Popen(
        [sys.executable, "-u", "-c", script], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    start, peak, killed = time.monotonic(), 0, None
    while child.poll() is None:
        peak = max(peak, rss_mb(child.pid))
        if peak > RSS_CEILING_MB:
            killed = "rss"
        elif time.monotonic() - start > DEADLINE_S:
            killed = "deadline"
        if killed:
            child.kill()
            break
        time.sleep(0.05)
    child.wait()
    return peak, killed, child.stdout.read()


SMALL_RANGE_TABLE = "CREATE TABLE t AS SELECT (i % 1000)::SMALLINT AS g FROM range(20000000) t(i)"

#: Each allocates outside the buffer manager.
UNMANAGED = {
    "range": "SELECT len(range(20000000))",
    "list_value": "SELECT len(list_value(range(100000000)))",
    "repeat": "SELECT length(repeat('x', 3000000000))",
    # Grouping on a small-range table column takes the perfect hash aggregate, whose states are untracked;
    # the same aggregates through the regular hash aggregate are refused at memory_limit.
    "string_agg_states": (
        f"{SMALL_RANGE_TABLE}; SELECT sum(length(s)) FROM (SELECT string_agg(repeat('x', 100), '') AS s FROM t GROUP BY g)"
    ),
    "list_states": f"{SMALL_RANGE_TABLE}; SELECT sum(len(l)) FROM (SELECT list(repeat('x', 100)) AS l FROM t GROUP BY g)",
    "client_result": "SELECT repeat('x', 1000) FROM range(3000000)",
}

#: Each is a Python expression the child evaluates to the statement.
UNMANAGED_BUILT = {
    "list_literal": "'SELECT len([' + ','.join(map(str, range(3000000))) + '])'",
}


def assert_under_ceiling(peak, killed, out):
    assert killed != "rss", f"peak RSS {peak}MB over {RSS_CEILING_MB}MB with memory_limit {MEMORY_LIMIT_MB}MB\n{out}"
    assert "LOCKED" in out, out


@pytest.mark.parametrize("sql", UNMANAGED.values(), ids=UNMANAGED.keys())
def test_unmanaged_allocation_stays_under_memory_limit(sql):
    assert_under_ceiling(*watch(sql))


@pytest.mark.parametrize("sql", UNMANAGED_BUILT.values(), ids=UNMANAGED_BUILT.keys())
def test_unmanaged_parse_stays_under_memory_limit(sql):
    assert_under_ceiling(*watch(sql, built=True))


def test_unbounded_query_is_interrupted():
    peak, killed, out = watch("SELECT count(*) FROM range(1000000) a, range(1000000) b")
    assert killed != "deadline", f"still running after {DEADLINE_S}s\n{out}"
    assert "LOCKED" in out, out
