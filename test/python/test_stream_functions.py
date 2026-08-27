"""Stream functions actually executed -- open/next/close driven to completion, and to failure.

sqllogictest can only see that a registration appears in vcat_stream_functions(); the generator
protocol itself needs a host.
"""

import json

import pyarrow as pa
import pytest

from conftest import BLOB, VARCHAR, schema_message, stream_bytes


class Generator:
    """Host side of the open/next/close protocol. `next` returns one Arrow IPC frame per call and
    NULL to signal exhaustion -- there is no separate end-of-stream signal."""

    def __init__(self, con, batches, schema):
        self.con = con
        self.batches = batches
        self.schema = schema
        self.state: dict[str, int] = {}
        self.calls: list[tuple] = []
        self.closed: list[str] = []
        self.fail_on_batch = None

    def register(self, prefix="g"):
        # null_handling="special" is mandatory, not a preference: open is always called with a NULL
        # fourth argument (there are no pushed rows on the local path), and DuckDB's default NULL
        # handling short-circuits a UDF with any NULL input to NULL without calling it -- which the
        # extension reports as "returned no Arrow schema when opened".
        self.con.create_function(
            f"{prefix}_open", self._open, [VARCHAR, VARCHAR, VARCHAR, BLOB], BLOB, null_handling="special"
        )
        # next needs it too, for the other direction: returning NULL is how the protocol signals
        # exhaustion, and under DEFAULT handling DuckDB rejects a NULL the UDF returned itself.
        self.con.create_function(f"{prefix}_next", self._next, [VARCHAR], BLOB, null_handling="special")
        self.con.create_function(f"{prefix}_close", self._close, [VARCHAR], VARCHAR)

    def _open(self, handle, function_name, args_json, pushed):
        self.calls.append(("open", handle, function_name, args_json))
        self.state[handle] = 0
        return schema_message(self.schema)

    def _next(self, handle):
        index = self.state[handle]
        self.calls.append(("next", handle, index))
        if self.fail_on_batch is not None and index == self.fail_on_batch:
            raise RuntimeError("generator exploded")
        if index >= len(self.batches):
            return None
        self.state[handle] = index + 1
        return stream_bytes(pa.Table.from_batches([self.batches[index]], schema=self.schema))

    def _close(self, handle):
        self.calls.append(("close", handle))
        self.closed.append(handle)
        return "ok"


SCHEMA = pa.schema([pa.field("id", pa.int32()), pa.field("label", pa.string())])


def batches(count, rows_each=3):
    out = []
    for b in range(count):
        base = b * rows_each
        out.append(
            pa.RecordBatch.from_arrays(
                [
                    pa.array(range(base, base + rows_each), type=pa.int32()),
                    pa.array([f"r{i}" for i in range(base, base + rows_each)]),
                ],
                schema=SCHEMA,
            )
        )
    return out


@pytest.fixture
def gen(con):
    con.execute("ATTACH ':memory:' AS app (TYPE virtual_catalog)")
    g = Generator(con, batches(3), SCHEMA)
    g.register()
    # .fetchall() is load-bearing: an unfetched SELECT never runs the scalar function, so the
    # catalog entry is never created and the failure only shows up at the call site.
    con.execute("SELECT vcat_create_stream_function('app', 'main', 'gen', 'g_open', 'g_next', 'g_close')").fetchall()
    return g


def test_stream_drains_every_batch(con, gen):
    rows = con.execute("SELECT * FROM app.main.gen() ORDER BY id").fetchall()
    assert rows == [(i, f"r{i}") for i in range(9)]


def test_close_runs_after_exhaustion(con, gen):
    con.execute("SELECT count(*) FROM app.main.gen()").fetchall()
    assert gen.closed, "close UDF was never called"


def test_next_is_called_once_past_the_last_batch(con, gen):
    con.execute("SELECT * FROM app.main.gen()").fetchall()
    nexts = [c for c in gen.calls if c[0] == "next"]
    assert len(nexts) == len(gen.batches) + 1


def test_empty_stream(con):
    con.execute("ATTACH ':memory:' AS e (TYPE virtual_catalog)")
    g = Generator(con, [], SCHEMA)
    g.register(prefix="e")
    # .fetchall() is load-bearing: an unfetched SELECT never runs the scalar function, so the
    # catalog entry is never created and the failure only shows up at the call site.
    con.execute("SELECT vcat_create_stream_function('e', 'main', 'gen', 'e_open', 'e_next', 'e_close')").fetchall()
    assert con.execute("SELECT * FROM e.main.gen()").fetchall() == []
    assert g.closed


def test_arguments_reach_open_as_json(con, gen):
    con.execute("SELECT * FROM app.main.gen(1, 'two')").fetchall()
    opens = [c for c in gen.calls if c[0] == "open"]
    assert opens, "open UDF was never called"
    assert json.loads(opens[-1][3]) is not None


def test_two_concurrent_scans_get_distinct_handles(con, gen):
    con.execute("SELECT * FROM app.main.gen() a, app.main.gen() b").fetchall()
    handles = {c[1] for c in gen.calls if c[0] == "open"}
    assert len(handles) == 2


def test_a_raising_next_surfaces_as_a_query_error_and_still_closes(con, gen):
    gen.fail_on_batch = 1
    with pytest.raises(Exception, match="generator exploded"):
        con.execute("SELECT * FROM app.main.gen()").fetchall()
    assert gen.closed, "close UDF was not called after the generator failed"


def test_early_termination_still_closes(con, gen):
    con.execute("SELECT * FROM app.main.gen() LIMIT 1").fetchall()
    assert gen.closed, "close UDF was not called after an early-terminated scan"


def test_dropping_the_stream_function_removes_it(con, gen):
    con.execute("SELECT vcat_drop_stream_function('app', 'main', 'gen')").fetchall()
    assert con.execute("SELECT count(*) FROM vcat_stream_functions() WHERE catalog = 'app'").fetchone() == (0,)
    with pytest.raises(Exception):
        con.execute("SELECT * FROM app.main.gen()").fetchall()
