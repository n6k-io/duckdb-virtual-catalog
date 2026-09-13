"""A declared primary key that is not unique turns a per-row write into a per-group write."""

import pytest

from conftest import BRIDGE_EXTENSION, READWRITE, attach_type, bridge, fn, new_connection, unique_id

pytestmark = pytest.mark.skipif(
    not BRIDGE_EXTENSION.exists(),
    reason=f"extension not built: {BRIDGE_EXTENSION} -- run `make release`",
)

FANOUT_ROWS = [(1, 5, 1, "mine"), (2, 5, 2, "HIDDEN-A"), (3, 6, 2, "HIDDEN-B")]


def fanout_source(source):
    source.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, grp INTEGER, tenant INTEGER, secret VARCHAR)")
    source.executemany("INSERT INTO t1 VALUES (?, ?, ?, ?)", FANOUT_ROWS)


def source_rows(source):
    return source.execute("SELECT * FROM t1 ORDER BY id").fetchall()


def test_update_through_a_non_unique_key_is_refused_and_rolls_back(source, target):
    fanout_source(source)
    bridge(
        source,
        target,
        {"t1": {"select": "tenant = 1", "update": "true", "delete": "true"}},
        pk_overrides={"t1": ["grp"]},
    )

    assert target.execute("SELECT * FROM app.main.t1").fetchall() == [(1, 5, 1, "mine")]

    with pytest.raises(Exception, match="not unique on the source"):
        target.execute("UPDATE app.main.t1 SET secret = 'PWNED'")

    assert source_rows(source) == FANOUT_ROWS


def test_delete_through_a_non_unique_key_is_refused_and_rolls_back(source, target):
    fanout_source(source)
    bridge(
        source,
        target,
        {"t1": {"select": "tenant = 1", "update": "true", "delete": "true"}},
        pk_overrides={"t1": ["grp"]},
    )

    with pytest.raises(Exception, match="not unique on the source"):
        target.execute("DELETE FROM app.main.t1")

    assert source_rows(source) == FANOUT_ROWS


def test_composite_key_subset_is_refused(source, target):
    """Two declared key columns take the per-row composite DELETE path, not the batched one."""
    source.execute(
        "CREATE TABLE t2(a INTEGER, b INTEGER, c INTEGER, tenant INTEGER, secret VARCHAR, " "PRIMARY KEY (a, b, c))"
    )
    rows = [(1, 1, 1, 1, "mine"), (1, 1, 2, 2, "HIDDEN"), (2, 1, 1, 1, "also-mine")]
    source.executemany("INSERT INTO t2 VALUES (?, ?, ?, ?, ?)", rows)
    bridge(
        source,
        target,
        {"t2": {"select": "tenant = 1", "delete": "true"}},
        pk_overrides={"t2": ["a", "b"]},
    )

    with pytest.raises(Exception, match="not unique on the source"):
        target.execute("DELETE FROM app.main.t2")

    assert source.execute("SELECT * FROM t2 ORDER BY a, b, c").fetchall() == rows


def test_a_real_key_still_writes_across_batch_boundaries(source, target):
    """The DELETE path batches 1000 keys per statement; a valid key must not trip the guard."""
    source.execute("CREATE TABLE big(id INTEGER PRIMARY KEY, name VARCHAR)")
    source.execute("INSERT INTO big SELECT i, 'u' || i FROM range(1, 2501) t(i)")
    bridge(source, target, {"big": READWRITE})

    assert target.execute("UPDATE app.main.big SET name = 'P'").fetchall() == [(2500,)]
    assert target.execute("DELETE FROM app.main.big WHERE id <= 2200").fetchall() == [(2200,)]
    assert source.execute("SELECT count(*) FROM big").fetchone() == (300,)


def test_an_asserted_but_unique_key_behaves_exactly_as_before(source, target):
    """UNIQUE NOT NULL with no PK: discovery finds nothing, so the key is asserted -- and is fine."""
    source.execute("CREATE TABLE u(code VARCHAR UNIQUE NOT NULL, tenant INTEGER, v VARCHAR)")
    source.executemany("INSERT INTO u VALUES (?, ?, ?)", [("a", 1, "x"), ("b", 1, "y"), ("c", 2, "hidden")])
    bridge(
        source,
        target,
        {"u": {"select": "tenant = 1", "update": "true", "delete": "true"}},
        pk_overrides={"u": ["code"]},
    )

    assert target.execute("UPDATE app.main.u SET v = 'P'").fetchall() == [(2,)]
    assert target.execute("DELETE FROM app.main.u WHERE code = 'a'").fetchall() == [(1,)]
    assert source.execute("SELECT * FROM u ORDER BY code").fetchall() == [
        ("b", 1, "P"),
        ("c", 2, "hidden"),
    ]


def test_a_nullable_key_column_still_silently_matches_nothing(source, target):
    """Known gap: 0 affected rows passes the guard, so a NULL key is a no-op rather than an error."""
    source.execute("CREATE TABLE n(k INTEGER, tenant INTEGER, v VARCHAR)")
    source.executemany("INSERT INTO n VALUES (?, ?, ?)", [(None, 1, "x"), (2, 1, "y")])
    bridge(
        source,
        target,
        {"n": {"select": "tenant = 1", "delete": "true"}},
        pk_overrides={"n": ["k"]},
    )

    assert target.execute("DELETE FROM app.main.n WHERE v = 'x'").fetchall() == [(0,)]
    assert source.execute("SELECT count(*) FROM n").fetchone() == (2,)


def test_primary_key_query_supplies_a_key_discovery_cannot_find(source, target):
    source.execute("CREATE TABLE q(code VARCHAR, tenant INTEGER, v VARCHAR)")
    source.executemany("INSERT INTO q VALUES (?, ?, ?)", [("a", 1, "x"), ("b", 2, "hidden")])

    bridge_id = unique_id()
    token = source.execute(f"SELECT {fn(source, 'register_source')}(?, 'memory')", [bridge_id]).fetchone()[0]
    source.execute(f"SELECT {fn(source, 'primary_key_query')}(?, 'main.q', ?)", [bridge_id, "SELECT 'code'"])
    source.execute(f"SELECT {fn(source, 'policy')}(?, 'main.q', 'select', 'tenant = 1')", [bridge_id])
    source.execute(f"SELECT {fn(source, 'policy')}(?, 'main.q', 'update', 'true')", [bridge_id])
    target.execute(f"ATTACH '' AS app (TYPE {attach_type(target)}, ID '{bridge_id}', TOKEN '{token}')")

    assert target.execute("UPDATE app.main.q SET v = 'P'").fetchall() == [(1,)]
    assert source.execute("SELECT * FROM q ORDER BY code").fetchall() == [
        ("a", 1, "P"),
        ("b", 2, "hidden"),
    ]


def test_primary_key_check_rejects_a_bad_key_before_any_write(source, target):
    fanout_source(source)
    bridge_id = unique_id()
    source.execute(f"SELECT {fn(source, 'register_source')}(?, 'memory')", [bridge_id])
    source.execute(f"SELECT {fn(source, 'primary_key')}(?, 'main.t1', ['grp'])", [bridge_id])

    with pytest.raises(Exception, match="not unique on the source"):
        source.execute(
            f"SELECT {fn(source, 'primary_key_check')}(?, 'main.t1', ?)",
            [bridge_id, "SELECT grp FROM t1 GROUP BY grp HAVING count(*) > 1"],
        )

    assert source_rows(source) == FANOUT_ROWS


def test_primary_key_check_accepts_a_key_that_holds(source):
    fanout_source(source)
    bridge_id = unique_id()
    source.execute(f"SELECT {fn(source, 'register_source')}(?, 'memory')", [bridge_id])
    source.execute(f"SELECT {fn(source, 'primary_key')}(?, 'main.t1', ['id'])", [bridge_id])

    assert source.execute(
        f"SELECT {fn(source, 'primary_key_check')}(?, 'main.t1', ?)",
        [bridge_id, "SELECT id FROM t1 GROUP BY id HAVING count(*) > 1"],
    ).fetchone() == ("ok",)
