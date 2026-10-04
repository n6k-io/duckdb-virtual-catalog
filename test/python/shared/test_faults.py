"""Regression tests for faults 1-4 of faults.md.

Three of the four kill the process, so they run in a subprocess: a SIGSEGV inside pytest takes
the whole session with it and reports nothing. `run_probe` asserts on the child's exit code, so
rc 139 (SIGSEGV) / 138 (SIGBUS) is a test failure with the script's own output attached.
"""

import signal
import subprocess
import sys
import textwrap

import pyarrow as pa
import pytest

from conftest import (
    BLOB,
    BRIDGE_EXTENSION,
    PROVIDER_EXTENSION,
    REPO_ROOT,
    VARCHAR,
    VARCHAR_LIST,
    new_connection,
    schema_message,
    stream_bytes,
    unique_id,
)

pytestmark = pytest.mark.skipif(
    not (BRIDGE_EXTENSION.exists() and PROVIDER_EXTENSION.exists()),
    reason="extensions not built -- run `make release`",
)

TEST_DIR = REPO_ROOT / "test" / "python"


def run_probe(body, timeout=180):
    """Run `body` as its own process. Returns stdout; fails the test on a crash or an exception."""
    script = "import sys\nsys.path.insert(0, %r)\n" % str(TEST_DIR) + textwrap.dedent(body)
    proc = subprocess.run(
        [sys.executable, "-u", "-c", script],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if proc.returncode != 0:
        # subprocess reports a signal as a negative return code, so SIGSEGV is -11, not 139.
        killed = signal.Signals(-proc.returncode).name if proc.returncode < 0 else None
        pytest.fail(
            f"probe exited {proc.returncode}{f' ({killed})' if killed else ''}\n"
            f"--- stdout ---\n{proc.stdout}\n--- stderr ---\n{proc.stderr}"
        )
    return proc.stdout


# --- 1. bridge swap frees catalog entries while binders hold raw pointers ------------------------


def test_bridge_swap_under_concurrent_binds_does_not_free_live_entries():
    """Detaching a bridge must retire its cached entries, not free them: LookupEntry hands out a
    raw CatalogEntry* and drops every lock before the binder is done with it."""
    out = run_probe(
        """
        import threading, time
        from conftest import new_connection, bridge, unique_id, READ

        s = new_connection(); t = new_connection()
        s.execute("CREATE TABLE big(id INTEGER PRIMARY KEY, v VARCHAR); "
                  "INSERT INTO big SELECT i,'v'||i FROM range(20000) tbl(i)")
        bridge(s, t, {"big": READ})

        stop = False
        errs = []

        def binder():
            c = t.cursor()
            while not stop:
                try:
                    c.execute("SELECT id FROM app.main.big WHERE id=1").fetchall()
                except Exception as e:
                    errs.append(repr(e)[:120])

        threads = [threading.Thread(target=binder) for _ in range(12)]
        for th in threads:
            th.start()
        try:
            for _ in range(200):
                time.sleep(0.002)
                # Detach first: the catalog name is what a second bridge would collide on.
                t.execute("DETACH app")
                b = unique_id()
                tok = s.execute("SELECT bridge_register_source(?, 'memory')", [b]).fetchone()[0]
                s.execute("SELECT bridge_policy(?,'main.big','select','true')", [b])
                t.execute(
                    "ATTACH '' AS app (TYPE virtual_catalog_bridge, ID '%s', TOKEN '%s')" % (b, tok)
                )
        finally:
            # Without this the readers spin forever and the probe hangs rather than reporting.
            stop = True
            for th in threads:
                th.join()

        # A bind racing a swap may legitimately lose the table -- that is a clean catalog error.
        # Memory corruption is what must not appear.
        corrupt = [e for e in errs if "out of range" in e or "INTERNAL" in e]
        assert not corrupt, corrupt[:5]
        print("ok")
        """
    )
    assert "ok" in out


# --- 2. cyclic bridge ---------------------------------------------------------------------------


def test_register_source_refuses_a_virtual_catalog_source():
    """A virtual_catalog as its own source recurses without bound on the first read."""
    con = new_connection()
    con.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
    host = unique_id()
    token = con.execute("SELECT bridge_register_source(?, 'memory')", [host]).fetchone()[0]
    con.execute("SELECT bridge_policy(?, 'main.t', 'select', 'true')", [host])
    con.execute(f"ATTACH '' AS app (TYPE virtual_catalog_bridge, ID '{host}', TOKEN '{token}')")
    with pytest.raises(Exception, match="virtual_catalog"):
        con.execute("SELECT bridge_register_source(?, 'app')", [unique_id()])
    con.close()


def test_policy_refuses_a_source_catalog_that_became_virtual_after_registration():
    """The catalog is named at registration but not read until the first grant, so the check has
    to be repeated there."""
    con = new_connection()
    bridge_id = unique_id()
    con.execute("ATTACH ':memory:' AS app")
    con.execute("CREATE TABLE app.main.t(id INTEGER PRIMARY KEY, v VARCHAR)")
    con.execute("SELECT bridge_register_source(?, 'app')", [bridge_id])
    con.execute("DETACH app")

    con.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
    host = unique_id()
    token = con.execute("SELECT bridge_register_source(?, 'memory')", [host]).fetchone()[0]
    con.execute("SELECT bridge_policy(?, 'main.t', 'select', 'true')", [host])
    con.execute(f"ATTACH '' AS app (TYPE virtual_catalog_bridge, ID '{host}', TOKEN '{token}')")

    with pytest.raises(Exception, match="virtual_catalog"):
        con.execute("SELECT bridge_policy(?, 'main.t', 'select', 'true')", [bridge_id])
    con.close()


def test_cyclic_bridge_never_installs():
    """The full three-hop cycle from faults.md #2. Whichever call is refused, the process must
    survive and the final read must not recurse."""
    out = run_probe(
        """
        from conftest import new_connection, unique_id

        a = new_connection(); b = new_connection()
        a.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, v VARCHAR); INSERT INTO t1 VALUES (1,'x')")

        i1 = unique_id()
        tk = a.execute("SELECT bridge_register_source(?, 'memory')", [i1]).fetchone()[0]
        a.execute("SELECT bridge_policy(?,'main.t1','select','true')", [i1])
        b.execute("ATTACH '' AS app (TYPE virtual_catalog_bridge, ID '%s', TOKEN '%s')" % (i1, tk))
        assert b.execute("SELECT * FROM app.main.t1").fetchall() == [(1, 'x')]

        i2 = unique_id()
        try:
            tk2 = b.execute("SELECT bridge_register_source(?, 'app')", [i2]).fetchone()[0]
        except Exception as e:
            assert "virtual_catalog" in str(e), e
            print("ok")
            raise SystemExit(0)

        b.execute("SELECT bridge_policy(?,'main.t1','select','true')", [i2])
        b.execute("ATTACH '' AS app2 (TYPE virtual_catalog_bridge, ID '%s', TOKEN '%s')" % (i2, tk2))

        i3 = unique_id()
        tk3 = b.execute("SELECT bridge_register_source(?, 'app2')", [i3]).fetchone()[0]
        b.execute("SELECT bridge_policy(?,'main.t1','select','true')", [i3])
        b.execute("DETACH app")
        b.execute("ATTACH '' AS app (TYPE virtual_catalog_bridge, ID '%s', TOKEN '%s')" % (i3, tk3))
        raise AssertionError("the cycle installed; the read below would recurse without bound")
        """
    )
    assert "ok" in out


# --- 3. provider/stream schema drift ------------------------------------------------------------

DECLARED = pa.schema([("id", pa.int32()), ("name", pa.utf8())])


def register_drifting_provider(con, scan):
    """A provider declaring DECLARED whose scan UDF answers with whatever `scan` returns."""
    tag = unique_id("d")
    con.create_function(f"{tag}_list", lambda: "main.users", [], VARCHAR)
    con.create_function(f"{tag}_schema", lambda n: schema_message(DECLARED), [VARCHAR], BLOB)
    con.create_function(f"{tag}_scan", scan, [VARCHAR, VARCHAR_LIST, VARCHAR], BLOB)
    con.execute(
        "SELECT provider_register('app',?,?,?,'','','','')",
        [f"{tag}_list", f"{tag}_schema", f"{tag}_scan"],
    )
    con.execute("SELECT provider_invalidate_tables('app')")


def test_provider_returning_fewer_columns_than_projected_is_refused():
    """Two columns projected, one returned: the unmapped column used to be indexed anyway."""
    out = run_probe(
        """
        import pyarrow as pa
        from conftest import new_connection, VARCHAR, BLOB, VARCHAR_LIST, schema_message, stream_bytes

        con = new_connection(bridge=False, provider=True)
        con.execute("ATTACH ':memory:' AS app (TYPE virtual_catalog_provider)")
        declared = pa.schema([("id", pa.int32()), ("name", pa.utf8())])
        con.create_function("q_list", lambda: "main.users", [], VARCHAR)
        con.create_function("q_schema", lambda n: schema_message(declared), [VARCHAR], BLOB)
        con.create_function(
            "q_scan",
            lambda n, c, f: stream_bytes(pa.table({"c0": pa.array([1, 2], type=pa.int32())})),
            [VARCHAR, VARCHAR_LIST, VARCHAR],
            BLOB,
        )
        con.execute("SELECT provider_register('app','q_list','q_schema','q_scan','','','','')")
        try:
            rows = con.execute("SELECT id, name FROM app.main.users").fetchall()
        except Exception as e:
            print("refused:", str(e)[:120])
            print("ok")
        else:
            raise AssertionError(f"short scan result was accepted: {rows}")
        """
    )
    assert "ok" in out


def test_provider_returning_a_short_primary_key_block_is_refused():
    """On DML the key columns are appended after the projected ones and read by position; a scan
    that omits them used to read children[] past the end."""
    out = run_probe(
        """
        import pyarrow as pa
        from conftest import new_connection, VARCHAR, BLOB, VARCHAR_LIST, BIGINT, schema_message, stream_bytes

        con = new_connection(bridge=False, provider=True)
        con.execute("ATTACH ':memory:' AS app (TYPE virtual_catalog_provider)")
        declared = pa.schema([("id", pa.int32()), ("name", pa.utf8())]).with_metadata(
            {b"vcat.primary_keys": b"id"}
        )
        con.create_function("q_list", lambda: "main.users", [], VARCHAR)
        con.create_function("q_schema", lambda n: schema_message(declared), [VARCHAR], BLOB)
        # Answers the projection but never appends the trailing key block.
        con.create_function(
            "q_scan",
            lambda n, c, f: stream_bytes(pa.table({"c0": pa.array(["ana", "bo"])})),
            [VARCHAR, VARCHAR_LIST, VARCHAR],
            BLOB,
        )
        con.create_function("q_delete", lambda n, payload: 0, [VARCHAR, BLOB], BIGINT)
        con.execute(
            "SELECT provider_register('app','q_list','q_schema','q_scan','','','q_delete','')"
        )
        try:
            con.execute("DELETE FROM app.main.users WHERE name = 'ana'")
        except Exception as e:
            print("refused:", str(e)[:120])
            print("ok")
        else:
            raise AssertionError("scan without the key block was accepted")
        """
    )
    assert "ok" in out


def test_provider_returning_the_wrong_column_type_is_refused():
    """Declared INTEGER, returned utf8: no crash, but the integer column comes back as the utf8
    offsets buffer reinterpreted. The bridge path already rejects this (bridge_scan.hpp)."""
    con = new_connection(bridge=False, provider=True)
    con.execute("ATTACH ':memory:' AS app (TYPE virtual_catalog_provider)")
    register_drifting_provider(
        con,
        lambda n, c, f: stream_bytes(pa.table({"c0": pa.array(["A" * 20, "B" * 20]), "c1": pa.array(["ana", "bo"])})),
    )
    with pytest.raises(Exception, match="(?i)type|column"):
        con.execute("SELECT id, name FROM app.main.users").fetchall()
    con.close()


def test_stream_function_batch_narrower_than_the_declared_schema_is_refused():
    """`open` declares two columns, `next` yields a batch with one."""
    out = run_probe(
        """
        import pyarrow as pa
        from conftest import new_connection, VARCHAR, BLOB, schema_message, stream_bytes

        con = new_connection(bridge=False, provider=True)
        declared = pa.schema([("id", pa.int32()), ("label", pa.utf8())])
        narrow = pa.table({"id": pa.array([1, 2], type=pa.int32())})
        served = {}

        def _open(handle, function_name, args_json, pushed):
            served[handle] = False
            return schema_message(declared)

        def _next(handle):
            if served.get(handle):
                return None
            served[handle] = True
            return stream_bytes(narrow)

        con.create_function("s_open", _open, [VARCHAR, VARCHAR, VARCHAR, BLOB], BLOB, null_handling="special")
        con.create_function("s_next", _next, [VARCHAR], BLOB, null_handling="special")
        con.create_function("s_close", lambda h: "ok", [VARCHAR], VARCHAR)
        con.execute("SELECT provider_create_stream_function('memory','main','gen','s_open','s_next','s_close')")
        try:
            rows = con.execute("SELECT * FROM gen()").fetchall()
        except Exception as e:
            print("refused:", str(e)[:120])
            print("ok")
        else:
            raise AssertionError(f"narrow batch was accepted: {rows}")
        """
    )
    assert "ok" in out


# --- 4. truncated Arrow IPC body ----------------------------------------------------------------

FULL_DATA = pa.table(
    {
        "c0": pa.array(list(range(1000)), type=pa.int32()),
        "c1": pa.array(["x" * 50] * 1000),
    }
)


@pytest.mark.parametrize("cut", [40000, 20000])
def test_truncated_arrow_body_is_refused_not_read_past_the_end(cut):
    """body_size_bytes comes from the payload header; an intact header over a short body used to
    make nanoarrow read past the end of the UDF's BLOB and return those bytes as VARCHAR.

    Asserted on the rows, not just on "something raised": at cut=40000 the original repro does not
    raise at all, it returns 800 rows of out-of-bounds memory, and a truncation that trips DuckDB's
    unicode validation instead of the length check would pass an exception-only test."""
    con = new_connection(bridge=False, provider=True)
    con.execute("ATTACH ':memory:' AS app (TYPE virtual_catalog_provider)")
    full = stream_bytes(FULL_DATA)
    register_drifting_provider(con, lambda n, c, f: full[: len(full) - cut])
    try:
        rows = con.execute("SELECT id, name FROM app.main.users").fetchall()
    except Exception as e:
        assert "truncated Arrow IPC message" in str(e), f"refused, but not by the length check: {e}"
    else:
        past_end = [r for r in rows if r[1] != "x" * 50]
        pytest.fail(f"{len(rows)} rows returned, {len(past_end)} past the end of the buffer: {past_end[:1]}")
    finally:
        con.close()
