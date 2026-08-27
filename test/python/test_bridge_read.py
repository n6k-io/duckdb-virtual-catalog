"""Reads across two DatabaseInstances, with pushdown checked against the source's own answer."""

import pytest

from conftest import bridge

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
    bridge(source, target, {"users": "read"})
    return source, target


@pytest.mark.parametrize("predicate", PREDICATES)
def test_filter_pushdown_matches_source(bridged, predicate):
    source, target = bridged
    expected = source.execute(f"SELECT * FROM users WHERE {predicate} ORDER BY id").fetchall()
    actual = target.execute(f"SELECT * FROM app.main.users WHERE {predicate} ORDER BY id").fetchall()
    assert actual == expected


@pytest.mark.parametrize("projection", [
    "id",
    "name, id",
    "score, score",
    "ratio, joined, name, score, id",
])
def test_projection_pushdown_matches_source(bridged, projection):
    source, target = bridged
    expected = source.execute(f"SELECT {projection} FROM users ORDER BY id").fetchall()
    actual = target.execute(f"SELECT {projection} FROM app.main.users ORDER BY id").fetchall()
    assert actual == expected


def test_types_and_nulls_survive_the_crossing(bridged):
    source, target = bridged
    assert target.execute("SELECT * FROM app.main.users ORDER BY id").fetchall() == \
        source.execute("SELECT * FROM users ORDER BY id").fetchall()


def test_column_types_match_the_source(bridged):
    source, target = bridged
    expected = source.execute("DESCRIBE users").fetchall()
    actual = target.execute("DESCRIBE app.main.users").fetchall()
    assert [(c[0], c[1]) for c in actual] == [(c[0], c[1]) for c in expected]


def test_aggregate_over_the_bridge(bridged):
    _, target = bridged
    assert target.execute("SELECT count(*), sum(score) FROM app.main.users").fetchone() == (5, 150)


def test_scan_function_is_directly_callable(source, target):
    source.execute(SETUP)
    bridge_id = bridge(source, target, {"users": "read"})
    assert target.execute(
        f"SELECT count(*) FROM vcat_scan('{bridge_id}', 'users')"
    ).fetchone() == (5,)


def test_source_writes_are_visible_immediately(bridged):
    """No snapshot isolation: the target always sees the source's latest committed data."""
    source, target = bridged
    assert target.execute("SELECT count(*) FROM app.main.users").fetchone() == (5,)
    source.execute("INSERT INTO users VALUES (6, 'zed', 60, NULL, NULL)")
    assert target.execute("SELECT count(*) FROM app.main.users").fetchone() == (6,)


def test_dotted_source_names_are_rejected(source, target):
    """docs/virtual-catalog.md documents `catalog.table` and `catalog.schema.table` permission keys
    as the way to expose tables from another catalog attached to the source. The code rejects any
    dot outright (bridge_bridge.cpp:172), so the documented feature does not exist. Asserting the
    real behaviour so the contradiction is visible rather than latent."""
    source.execute("ATTACH ':memory:' AS shop")
    source.execute("CREATE TABLE shop.main.orders(id INTEGER PRIMARY KEY, item VARCHAR)")
    source.execute(SETUP)
    with pytest.raises(Exception, match="must be a bare table name"):
        bridge(source, target, {"shop.main.orders": "read"})


def test_source_tables_outside_the_default_schema_reach_via_source_args(source, target):
    """What does work: naming a different source catalog/schema in vcat_register_source itself."""
    source.execute("ATTACH ':memory:' AS shop")
    source.execute("CREATE TABLE shop.main.orders(id INTEGER PRIMARY KEY, item VARCHAR)")
    source.execute("INSERT INTO shop.main.orders VALUES (1, 'widget')")
    bridge(source, target, {"orders": "read"}, source_catalog="shop", source_schema="main")
    assert target.execute("SELECT item FROM app.main.orders").fetchall() == [("widget",)]


def test_tables_outside_the_permission_map_are_invisible(bridged):
    source, target = bridged
    source.execute("CREATE TABLE secret(id INTEGER PRIMARY KEY)")
    with pytest.raises(Exception, match="does not exist"):
        target.execute("SELECT * FROM app.main.secret").fetchall()
