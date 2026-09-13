"""Column names that used to be read as SQL syntax.

The source query is built as a relation, so a column name is a name. It was previously built as
text and then edited: the primary key was appended by searching the finished SQL for " FROM " and
splicing at the match, which found the wrong one when a column name contained that word. With a
second column named as the mangled result, the mangled name bound successfully and the scan
returned a shape the caller did not expect -- a segfault, not an error.
"""

import pytest

from conftest import READ, READWRITE, bridge

#: Each contains a fragment that reads as syntax if the query is assembled or scanned as text.
ODD = [
    "x FROM y",
    "x, id FROM y",
    "a FROM b WHERE c",
    'quote" FROM d',
    "SELECT",
    "FROM",
    "*",
]


@pytest.fixture(params=ODD)
def odd_column(request):
    """The column name, and the same name quoted for use in SQL."""
    return request.param, '"' + request.param.replace('"', '""') + '"'


def test_select_survives_odd_column_name(source, target, odd_column):
    _, quoted = odd_column
    source.execute(f"CREATE TABLE t(id INTEGER PRIMARY KEY, {quoted} INTEGER)")
    source.execute("INSERT INTO t VALUES (1, 10), (2, 20)")
    bridge(source, target, {"t": READ})

    assert target.execute("SELECT * FROM app.main.t ORDER BY id").fetchall() == [(1, 10), (2, 20)]
    assert target.execute(f"SELECT {quoted} FROM app.main.t ORDER BY id").fetchall() == [(10,), (20,)]


def test_delete_survives_odd_column_name(source, target, odd_column):
    """The delete path is the one that appends the primary key to the scan."""
    _, quoted = odd_column
    source.execute(f"CREATE TABLE t(id INTEGER PRIMARY KEY, {quoted} INTEGER)")
    source.execute("INSERT INTO t VALUES (1, 10), (2, 20)")
    bridge(source, target, {"t": READWRITE})

    target.execute(f"DELETE FROM app.main.t WHERE {quoted} = 10")

    assert source.execute("SELECT id FROM t ORDER BY id").fetchall() == [(2,)]


def test_delete_when_the_mangled_name_is_also_a_real_column(source, target):
    """The original crash: splicing turned "x FROM y" into "x, id FROM y", which existed."""
    source.execute('CREATE TABLE t(id INTEGER PRIMARY KEY, "x FROM y" INTEGER, "x, id FROM y" INTEGER)')
    source.execute("INSERT INTO t VALUES (1, 10, 777), (2, 20, 888)")
    bridge(source, target, {"t": READWRITE})

    target.execute('DELETE FROM app.main.t WHERE "x FROM y" = 10')

    assert source.execute("SELECT id FROM t ORDER BY id").fetchall() == [(2,)]


def test_update_survives_odd_column_name(source, target, odd_column):
    """The update path names the column in SET as well as appending the key to the scan."""
    _, quoted = odd_column
    source.execute(f"CREATE TABLE t(id INTEGER PRIMARY KEY, {quoted} INTEGER)")
    source.execute("INSERT INTO t VALUES (1, 10), (2, 20)")
    bridge(source, target, {"t": READWRITE})

    target.execute(f"UPDATE app.main.t SET {quoted} = 99 WHERE id = 1")

    assert source.execute(f"SELECT id, {quoted} FROM t ORDER BY id").fetchall() == [(1, 99), (2, 20)]


def test_select_survives_odd_primary_key_name(source, target, odd_column):
    name, quoted = odd_column
    source.execute(f"CREATE TABLE t({quoted} INTEGER PRIMARY KEY, v INTEGER)")
    source.execute("INSERT INTO t VALUES (1, 10), (2, 20)")
    bridge(source, target, {"t": READ})

    assert target.execute(f"SELECT {quoted}, v FROM app.main.t ORDER BY 1").fetchall() == [(1, 10), (2, 20)]


def test_delete_survives_odd_primary_key_name(source, target, odd_column):
    """The key names the WHERE column of the DELETE, not just the scan's projection."""
    _, quoted = odd_column
    source.execute(f"CREATE TABLE t({quoted} INTEGER PRIMARY KEY, v INTEGER)")
    source.execute("INSERT INTO t VALUES (1, 10), (2, 20)")
    bridge(source, target, {"t": READWRITE})

    target.execute("DELETE FROM app.main.t WHERE v = 10")

    assert source.execute(f"SELECT {quoted} FROM t").fetchall() == [(2,)]


def test_update_survives_odd_primary_key_name(source, target, odd_column):
    _, quoted = odd_column
    source.execute(f"CREATE TABLE t({quoted} INTEGER PRIMARY KEY, v INTEGER)")
    source.execute("INSERT INTO t VALUES (1, 10), (2, 20)")
    bridge(source, target, {"t": READWRITE})

    target.execute("UPDATE app.main.t SET v = 99 WHERE v = 10")

    assert source.execute(f"SELECT {quoted}, v FROM t ORDER BY 1").fetchall() == [(1, 99), (2, 20)]


def test_write_survives_odd_composite_primary_key(source, target, odd_column):
    """A composite key takes the other DELETE branch: a conjunction, not an IN list."""
    _, quoted = odd_column
    source.execute(f"CREATE TABLE t(a INTEGER, {quoted} INTEGER, v INTEGER, PRIMARY KEY (a, {quoted}))")
    source.execute("INSERT INTO t VALUES (1, 1, 10), (1, 2, 20)")
    bridge(source, target, {"t": READWRITE})

    target.execute("UPDATE app.main.t SET v = 99 WHERE v = 10")
    assert source.execute(f"SELECT v FROM t ORDER BY a, {quoted}").fetchall() == [(99,), (20,)]

    target.execute("DELETE FROM app.main.t WHERE v = 99")
    assert source.execute("SELECT v FROM t").fetchall() == [(20,)]
