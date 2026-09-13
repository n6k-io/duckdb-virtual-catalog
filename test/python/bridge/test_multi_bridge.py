"""Several bridges in one process, one attached catalog each.

One ATTACH is one bridge, so these are the shapes that involve more than one of them at a time: two
sources serving the same schema name into different catalogs, a bridge torn down without disturbing
its neighbour, and a reader that must not be disturbed by either.
"""

import threading

import pytest

from conftest import READ, bridge, new_connection, unbridge


@pytest.fixture
def two_sources():
    a = new_connection()
    b = new_connection()
    a.execute("CREATE TABLE alpha(id INTEGER PRIMARY KEY, v VARCHAR); INSERT INTO alpha VALUES (1, 'from-a')")
    b.execute("CREATE TABLE beta(id INTEGER PRIMARY KEY, v VARCHAR); INSERT INTO beta VALUES (2, 'from-b')")
    yield a, b
    a.close()
    b.close()


def test_two_bridges_serve_the_same_schema_name(two_sources, target):
    a, b = two_sources
    bridge(a, target, {"alpha": READ}, catalog="app_a")
    bridge(b, target, {"beta": READ}, catalog="app_b")

    assert target.execute("SELECT v FROM app_a.main.alpha").fetchall() == [("from-a",)]
    assert target.execute("SELECT v FROM app_b.main.beta").fetchall() == [("from-b",)]
    assert target.execute("SELECT schema, name FROM bridge_table_permissions('app_a')").fetchall() == [
        ("main", "alpha"),
    ]

    unbridge(target, "app_a")
    assert target.execute("SELECT v FROM app_b.main.beta").fetchall() == [("from-b",)]
    with pytest.raises(Exception, match="does not exist"):
        target.execute("SELECT * FROM app_a.main.alpha")

    unbridge(target, "app_b")


def test_two_bridges_serve_different_schemas(two_sources, target):
    """The target schemas are named by the grants and created by the attach -- no CREATE SCHEMA."""
    a, b = two_sources
    a.execute("CREATE SCHEMA one; CREATE TABLE one.alpha(id INTEGER PRIMARY KEY, v VARCHAR)")
    a.execute("INSERT INTO one.alpha VALUES (1, 'from-a')")
    b.execute("CREATE SCHEMA two; CREATE TABLE two.beta(id INTEGER PRIMARY KEY, v VARCHAR)")
    b.execute("INSERT INTO two.beta VALUES (2, 'from-b')")
    bridge(a, target, {"one.alpha": READ}, catalog="app_a")
    bridge(b, target, {"two.beta": READ}, catalog="app_b")

    assert target.execute("SELECT v FROM app_a.one.alpha").fetchall() == [("from-a",)]
    assert target.execute("SELECT v FROM app_b.two.beta").fetchall() == [("from-b",)]

    unbridge(target, "app_a")
    unbridge(target, "app_b")


def test_the_same_table_may_be_served_by_two_bridges(two_sources, target):
    """Separate catalogs, separate routing tables: two bridges over one source table is not a
    collision, and each carries its own policy."""
    a, _ = two_sources
    bridge(a, target, {"alpha": READ}, catalog="app_a")
    bridge(a, target, {"alpha": {"select": "id = 99"}}, catalog="app_b")

    assert target.execute("SELECT v FROM app_a.main.alpha").fetchall() == [("from-a",)]
    assert target.execute("SELECT v FROM app_b.main.alpha").fetchall() == []

    unbridge(target, "app_a")
    unbridge(target, "app_b")


def test_one_bridge_spans_two_source_schemas(two_sources, target):
    a, _ = two_sources
    a.execute(
        "CREATE SCHEMA sales; CREATE TABLE sales.orders(id INTEGER PRIMARY KEY); INSERT INTO sales.orders VALUES (7)"
    )
    bridge(a, target, {"main.alpha": READ, "sales.orders": READ})

    assert target.execute("SELECT v FROM app.main.alpha").fetchall() == [("from-a",)]
    assert target.execute("SELECT id FROM app.sales.orders").fetchall() == [(7,)]
    assert target.execute("SELECT schema, name FROM bridge_table_permissions('app') ORDER BY schema").fetchall() == [
        ("main", "alpha"),
        ("sales", "orders"),
    ]

    unbridge(target)
    with pytest.raises(Exception, match="does not exist"):
        target.execute("SELECT * FROM app.sales.orders")


def test_detach_frees_the_id_and_the_source(two_sources, target):
    """DETACH used to strand the bridge: nothing named it, so its registry entry pinned the source
    DatabaseInstance and its id for the life of the process."""
    a, _ = two_sources
    bridge_id = bridge(a, target, {"alpha": READ})
    unbridge(target)

    token = a.execute("SELECT bridge_register_source(?, 'memory')", [bridge_id]).fetchone()[0]
    a.execute("SELECT bridge_policy(?, 'main.alpha', 'select', 'true')", [bridge_id])
    target.execute(f"ATTACH '' AS app (TYPE virtual_catalog_bridge, ID '{bridge_id}', TOKEN '{token}')")
    assert target.execute("SELECT v FROM app.main.alpha").fetchall() == [("from-a",)]
    unbridge(target)


def test_dropping_a_bridged_schema_is_refused(two_sources, target):
    a, _ = two_sources
    a.execute("CREATE SCHEMA one; CREATE TABLE one.alpha(id INTEGER PRIMARY KEY)")
    bridge(a, target, {"one.alpha": READ})

    with pytest.raises(Exception, match="serving a bridge"):
        target.execute("DROP SCHEMA app.one")

    unbridge(target)


def test_scanning_one_bridge_while_another_churns(two_sources, target):
    """The regression test for scoping the fill lock per bridge: a reader on bridge A must not be
    serialised behind -- or freed by -- the attach/detach of bridge B."""
    a, b = two_sources
    bridge(a, target, {"alpha": READ}, catalog="app_a")

    stop = False
    errors = []

    def reader():
        cur = target.cursor()
        while not stop:
            try:
                cur.execute("SELECT v FROM app_a.main.alpha").fetchall()
            except Exception as e:  # noqa: BLE001 -- recorded, asserted on below
                errors.append(repr(e)[:160])

    threads = [threading.Thread(target=reader) for _ in range(4)]
    for th in threads:
        th.start()
    try:
        for _ in range(40):
            bridge(b, target, {"beta": READ}, catalog="app_b")
            unbridge(target, "app_b")
    finally:
        stop = True
        for th in threads:
            th.join()

    assert errors == []
    unbridge(target, "app_a")
