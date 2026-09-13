"""A DML addresses rows by the key columns of its own target scan, and only those.

Another bridged scan in the same statement -- of a different table, or of the target itself -- must
neither lend its columns to the write nor be mistaken for the target. A set-op subquery is the
reliable way to place such a scan left of the DML target in the physical plan; plain IN (SELECT ...),
USING, CTEs and delim joins keep the target leftmost.
"""

import pytest

from conftest import READWRITE, bridge

# `rowid * 0` is a no-op on the value; it forces the decoy scan to project rowid, which keeps it on
# the target next to the write.
DECOY_ROWIDS = "SELECT uid + rowid * 0 FROM app.main.other UNION ALL SELECT 99"
DECOY_PLAIN = "SELECT uid FROM app.main.other UNION ALL SELECT 99"


@pytest.fixture
def bridged(source, target):
    """Target `users(id)`, decoy `other(id, uid)` -- the decoy shares the target's PK column name."""
    source.execute("CREATE TABLE users(id INTEGER PRIMARY KEY, name VARCHAR)")
    source.execute("INSERT INTO users SELECT i, 'u' || i FROM range(1, 21) tbl(i)")
    source.execute("CREATE TABLE other(id INTEGER PRIMARY KEY, uid INTEGER)")
    source.execute("INSERT INTO other VALUES (17, 1), (18, 2), (19, 3)")
    bridge(source, target, {"users": READWRITE, "other": READWRITE})
    return source, target


@pytest.fixture
def bridged_no_name_collision(source, target):
    """Same shape, but the decoy has no column named `id`.

    Under the old first-scan-wins matcher the decoy was handed the *target's* PK column names, so
    this shape failed closed on the source with a binder error instead of corrupting. It is the
    case that distinguishes a real fix from a luckier guess.
    """
    source.execute("CREATE TABLE users(id INTEGER PRIMARY KEY, name VARCHAR)")
    source.execute("INSERT INTO users SELECT i, 'u' || i FROM range(1, 21) tbl(i)")
    source.execute("CREATE TABLE other(oid INTEGER PRIMARY KEY, uid INTEGER)")
    source.execute("INSERT INTO other VALUES (17, 1), (18, 2), (19, 3)")
    bridge(source, target, {"users": READWRITE, "other": READWRITE})
    return source, target


@pytest.fixture
def bridged_composite(source, target):
    source.execute("CREATE TABLE users(a INTEGER, b INTEGER, PRIMARY KEY(a, b))")
    source.execute("INSERT INTO users SELECT i, i FROM range(1, 21) tbl(i)")
    source.execute("CREATE TABLE other(oid INTEGER PRIMARY KEY, uid INTEGER)")
    source.execute("INSERT INTO other VALUES (17, 1), (18, 2), (19, 3)")
    bridge(source, target, {"users": READWRITE, "other": READWRITE})
    return source, target


@pytest.fixture
def bridged_self(source, target):
    source.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)")
    source.execute("INSERT INTO t SELECT i, i FROM range(1, 21) tbl(i)")
    bridge(source, target, {"t": READWRITE})
    return source, target


def source_ids(source, table="users", column="id"):
    return [r[0] for r in source.execute(f"SELECT {column} FROM {table} ORDER BY {column}").fetchall()]


# --- a decoy scan of a different table ------------------------------------------------------


def test_delete_behind_a_decoy_scan_removes_the_rows_it_named(bridged):
    source, target = bridged
    target.execute(f"DELETE FROM app.main.users WHERE id IN ({DECOY_ROWIDS})")
    assert source_ids(source) == list(range(4, 21))


def test_update_behind_a_decoy_scan_writes_the_rows_it_named(bridged):
    source, target = bridged
    target.execute(f"UPDATE app.main.users SET name = 'PWN' WHERE id IN ({DECOY_ROWIDS})")
    pwned = source.execute("SELECT id FROM users WHERE name = 'PWN' ORDER BY id").fetchall()
    assert pwned == [(1,), (2,), (3,)]


def test_delete_behind_a_decoy_scan_without_rowids(bridged):
    source, target = bridged
    target.execute(f"DELETE FROM app.main.users WHERE id IN ({DECOY_PLAIN})")
    assert source_ids(source) == list(range(4, 21))


def test_delete_behind_a_decoy_that_shares_no_column_name(bridged_no_name_collision):
    source, target = bridged_no_name_collision
    target.execute(f"DELETE FROM app.main.users WHERE id IN ({DECOY_ROWIDS})")
    assert source_ids(source) == list(range(4, 21))


def test_delete_behind_a_decoy_with_a_composite_key_target(bridged_composite):
    source, target = bridged_composite
    target.execute(f"DELETE FROM app.main.users WHERE a IN ({DECOY_ROWIDS})")
    assert source_ids(source, column="a") == list(range(4, 21))


# --- a second scan of the target table itself ------------------------------------------------


def test_delete_behind_a_decoy_scan_of_the_target_table(bridged_self):
    """Both scans are of the target; only the one the delete stands on addresses its rows."""
    source, target = bridged_self
    sql = (
        "DELETE FROM app.main.t WHERE id IN " "(SELECT v + rowid * 0 FROM app.main.t WHERE v > 17 UNION ALL SELECT 99)"
    )
    target.execute(sql)
    assert source_ids(source, "t") == list(range(1, 18))


def test_self_join_delete_still_works(bridged_self):
    source, target = bridged_self
    target.execute("DELETE FROM app.main.t WHERE id IN (SELECT id FROM app.main.t WHERE v > 17)")
    assert source_ids(source, "t") == list(range(1, 18))


def test_self_join_update_still_works(bridged_self):
    source, target = bridged_self
    target.execute("UPDATE app.main.t SET v = 0 WHERE id IN (SELECT id FROM app.main.t WHERE id < 3)")
    assert source.execute("SELECT v FROM t ORDER BY id").fetchall()[:3] == [(0,), (0,), (3,)]
