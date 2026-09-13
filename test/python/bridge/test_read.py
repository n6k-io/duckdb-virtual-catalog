"""Reads across two DatabaseInstances, with pushdown checked against the source's own answer."""

import pytest

from conftest import READ, READWRITE, bridge, fn, unique_id

SETUP = """
CREATE TABLE users(
    id INTEGER PRIMARY KEY,
    name VARCHAR,
    score INTEGER,
    joined DATE,
    ratio DOUBLE
);
INSERT INTO users VALUES
    (1, 'ana',  10, DATE '2024-01-01', 1.5),
    (2, 'bo',   20, DATE '2024-06-15', 2.5),
    (3, 'cy',   30, DATE '2025-02-02', NULL),
    (4, NULL,   40, NULL,              4.5),
    (5, 'e,f',  50, DATE '2025-12-31', 5.5);
"""

# Every operator the docs list as pushed down. Each is run through the bridge and against the
# source directly; nothing is asserted about specific rows, because the source IS the oracle.
PREDICATES = [
    "id = 3",
    "id != 3",
    "id < 3",
    "id <= 3",
    "id > 3",
    "id >= 3",
    "id IN (1, 4)",
    "name IS NULL",
    "name IS NOT NULL",
    "name = 'e,f'",
    "name = 'o''brien'",
    "joined >= DATE '2025-01-01'",
    "joined IS NULL",
    "ratio > 2.0",
    "score > 10 AND score < 50",
    "id > 1 AND name IS NOT NULL",
    "id = 1 OR id = 5",
    "name LIKE 'a%'",
]


@pytest.fixture
def bridged(source, target):
    source.execute(SETUP)
    bridge(source, target, {"users": READ})
    return source, target


@pytest.mark.parametrize("predicate", PREDICATES)
def test_filter_pushdown_matches_source(bridged, predicate):
    source, target = bridged
    expected = source.execute(f"SELECT * FROM users WHERE {predicate} ORDER BY id").fetchall()
    actual = target.execute(f"SELECT * FROM app.main.users WHERE {predicate} ORDER BY id").fetchall()
    assert actual == expected


@pytest.mark.parametrize(
    "projection",
    [
        "id",
        "name, id",
        "score, score",
        "ratio, joined, name, score, id",
    ],
)
def test_projection_pushdown_matches_source(bridged, projection):
    source, target = bridged
    expected = source.execute(f"SELECT {projection} FROM users ORDER BY id").fetchall()
    actual = target.execute(f"SELECT {projection} FROM app.main.users ORDER BY id").fetchall()
    assert actual == expected


def test_types_and_nulls_survive_the_crossing(bridged):
    source, target = bridged
    assert (
        target.execute("SELECT * FROM app.main.users ORDER BY id").fetchall()
        == source.execute("SELECT * FROM users ORDER BY id").fetchall()
    )


def test_column_types_match_the_source(bridged):
    source, target = bridged
    expected = source.execute("DESCRIBE users").fetchall()
    actual = target.execute("DESCRIBE app.main.users").fetchall()
    assert [(c[0], c[1]) for c in actual] == [(c[0], c[1]) for c in expected]


def test_aggregate_over_the_bridge(bridged):
    _, target = bridged
    assert target.execute("SELECT count(*), sum(score) FROM app.main.users").fetchone() == (5, 150)


def test_a_read_only_grant_still_produces_a_table(bridged):
    """Not a view: grants decide the verbs, never the entry kind. A native table is the oracle for
    every metadata query, and a view would answer half of them differently."""
    _, target = bridged
    assert target.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name = 'users'").fetchone() == (1,)
    assert target.execute("SELECT count(*) FROM duckdb_views() WHERE view_name = 'users'").fetchone() == (0,)


def test_writes_to_a_read_only_table_are_refused(bridged):
    """The entry kind no longer carries the refusal, so the grant check has to."""
    _, target = bridged
    with pytest.raises(Exception, match="insert"):
        target.execute("INSERT INTO app.main.users VALUES (9, 'x', 1, NULL, NULL)")
    with pytest.raises(Exception, match="delete"):
        target.execute("DELETE FROM app.main.users WHERE id = 1")
    with pytest.raises(Exception, match="update"):
        target.execute("UPDATE app.main.users SET score = 0")


def test_source_writes_are_visible_immediately(bridged):
    """No snapshot isolation: the target always sees the source's latest committed data."""
    source, target = bridged
    assert target.execute("SELECT count(*) FROM app.main.users").fetchone() == (5,)
    source.execute("INSERT INTO users VALUES (6, 'zed', 60, NULL, NULL)")
    assert target.execute("SELECT count(*) FROM app.main.users").fetchone() == (6,)


def test_a_three_part_grant_name_is_rejected(source, target):
    """A grant name is 'schema.table'. The source catalog is fixed by bridge_register_source, so a
    third part has nowhere to go."""
    source.execute("ATTACH ':memory:' AS shop")
    source.execute("CREATE TABLE shop.main.orders(id INTEGER PRIMARY KEY, item VARCHAR)")
    source.execute(SETUP)
    with pytest.raises(Exception, match="must name a schema and a table"):
        bridge(source, target, {"shop.main.orders": READ})


def test_a_bare_grant_name_is_rejected(source, target):
    source.execute(SETUP)
    bridge_id = unique_id()
    source.execute(f"SELECT {fn(source, 'register_source')}(?, 'memory')", [bridge_id])
    with pytest.raises(Exception, match="must be schema-qualified"):
        source.execute(f"SELECT {fn(source, 'policy')}(?, 'users', 'select', 'true')", [bridge_id])


def test_source_tables_outside_the_default_catalog_reach_via_source_args(source, target):
    """The source catalog is named in bridge_register_source; the schema comes from the grant."""
    source.execute("ATTACH ':memory:' AS shop")
    source.execute("CREATE TABLE shop.main.orders(id INTEGER PRIMARY KEY, item VARCHAR)")
    source.execute("INSERT INTO shop.main.orders VALUES (1, 'widget')")
    bridge(source, target, {"main.orders": READ}, source_catalog="shop")
    assert target.execute("SELECT item FROM app.main.orders").fetchall() == [("widget",)]


def test_tables_outside_the_permission_map_are_invisible(bridged):
    source, target = bridged
    source.execute("CREATE TABLE secret(id INTEGER PRIMARY KEY)")
    with pytest.raises(Exception, match="does not exist"):
        target.execute("SELECT * FROM app.main.secret").fetchall()


# --- streaming transport -------------------------------------------------------------------------
# The bridge streams the source result rather than materialising it, so a scan can now be abandoned
# with rows still buffered on the source. These pin the behaviours that only a streaming transport
# can get wrong.


@pytest.fixture
def big(source, target):
    """Large enough that a materialising transport is obvious and an abandoned scan leaves real work
    unconsumed."""
    source.execute(
        "CREATE TABLE big AS SELECT i::INTEGER AS id, 'name_' || (i % 1000) AS name, "
        "((i * 7) % 997)::INTEGER AS score FROM range(400000) t(i)"
    )
    bridge(source, target, {"big": READ})
    return source, target


def test_early_termination_does_not_wedge_the_source(big):
    """LIMIT leaves the source stream undrained. The next query on either side must still work."""
    source, target = big
    assert len(target.execute("SELECT * FROM app.main.big LIMIT 10").fetchall()) == 10
    assert target.execute("SELECT count(*) FROM app.main.big").fetchone() == (400000,)
    assert source.execute("SELECT count(*) FROM big").fetchone() == (400000,)


def test_repeated_early_termination(big):
    """Each scan opens its own source connection; abandoning many in a row must not accumulate."""
    source, target = big
    for _ in range(25):
        assert len(target.execute("SELECT id FROM app.main.big LIMIT 3").fetchall()) == 3
    assert target.execute("SELECT count(*) FROM app.main.big").fetchone() == (400000,)


def test_source_write_commits_while_a_bridged_scan_is_open(big):
    """The scan holds a read transaction on the source for its duration. A concurrent committed
    write on the source must not deadlock or be blocked by it."""
    source, target = big
    source.execute("INSERT INTO big VALUES (999999, 'late', 1)")
    assert target.execute("SELECT count(*) FROM app.main.big").fetchone() == (400001,)
    assert target.execute("SELECT name FROM app.main.big WHERE id = 999999").fetchall() == [("late",)]


def test_chunks_outlive_the_scan(big):
    """Hash join and sort buffer scanned chunks well past the scan that produced them. The output
    references source chunks, so this is where a lifetime error would surface."""
    source, target = big
    assert target.execute("SELECT count(*) FROM app.main.big a JOIN app.main.big b USING (id)").fetchone() == (400000,)
    assert (
        target.execute("SELECT name FROM app.main.big ORDER BY score, id LIMIT 3").fetchall()
        == source.execute("SELECT name FROM big ORDER BY score, id LIMIT 3").fetchall()
    )


def test_string_columns_survive_the_source_connection(big):
    """Strings are the case where referencing rather than copying could hand back freed memory."""
    source, target = big
    rows = target.execute("SELECT DISTINCT name FROM app.main.big ORDER BY name LIMIT 5").fetchall()
    assert rows == source.execute("SELECT DISTINCT name FROM big ORDER BY name LIMIT 5").fetchall()
    assert all(r[0].startswith("name_") for r in rows)
