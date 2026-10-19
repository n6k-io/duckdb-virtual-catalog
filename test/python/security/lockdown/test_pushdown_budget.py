"""ATTACK  target runs a heavy DISTINCT, GROUP BY or join over a bridged table; the bridge pushes it to
        the source, where the target's lockdown caps do not apply
BREACH  the source's operator memory for that one bridged query grows past the per-bridge budget
PASS    the source runs pushed work within the budget (spilling or refusing past it)

Operator memory is read from the source's duckdb_memory() while the target query runs; table data is
excluded, it is the source's own.
"""

import threading

import pytest

from conftest import READ, bridge, new_connection

pytestmark = pytest.mark.xfail(
    strict=True, reason="Known issue: work pushed to the source is not bounded by a per-bridge memory budget"
)

BUDGET_MB = 64
ROWS = 5_000_000

OPERATOR_TAGS = "('HASH_TABLE', 'ORDER_BY', 'COLUMN_DATA', 'ALLOCATOR', 'WINDOW')"

PUSHED = {
    "distinct": "SELECT count(*) FROM (SELECT DISTINCT v FROM app.main.big)",
    "group_by": "SELECT count(*) FROM (SELECT v, count(*) FROM app.main.big GROUP BY v)",
    "self_join": "SELECT count(*) FROM app.main.big a JOIN app.main.big b USING (v)",
}


@pytest.fixture(scope="module")
def big():
    source, target = new_connection(), new_connection()
    source.execute("CREATE TABLE big(id BIGINT PRIMARY KEY, v VARCHAR)")
    source.execute(f"INSERT INTO big SELECT i, 'value-' || i FROM range({ROWS}) t(i)")
    bridge(source, target, {"big": READ})
    yield source, target
    source.close()
    target.close()


def peak_operator_mb(source, run):
    """Runs `run` while sampling the source's operator memory. Returns (result, peak MB)."""
    peak, stop = [0], threading.Event()

    def sample():
        cursor = source.cursor()
        query = (
            f"SELECT coalesce(sum(memory_usage_bytes), 0) // 1048576 FROM duckdb_memory() WHERE tag IN {OPERATOR_TAGS}"
        )
        while not stop.is_set():
            peak[0] = max(peak[0], cursor.execute(query).fetchone()[0])

    sampler = threading.Thread(target=sample)
    sampler.start()
    try:
        result = run()
    finally:
        stop.set()
        sampler.join()
    return result, peak[0]


@pytest.mark.parametrize("sql", PUSHED.values(), ids=PUSHED.keys())
def test_pushed_work_stays_within_bridge_budget(big, sql):
    source, target = big
    assert "CROSSING_READ" in target.execute(f"EXPLAIN {sql}").fetchall()[0][1]

    result, peak = peak_operator_mb(source, lambda: target.execute(sql).fetchall())

    assert result == [(ROWS,)]
    assert peak <= BUDGET_MB, f"source operator memory peaked at {peak}MB for one bridged query, budget {BUDGET_MB}MB"
