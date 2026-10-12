"""Chained bridges: a bridge whose source catalog is itself a bridge.

`a` holds the real tables, `b` bridges them as `app`, and `c` bridges `b`'s `app` as `app`. Each is
its own DatabaseInstance.
"""

import pytest

from conftest import READ, READWRITE, bridge, new_connection, unique_id


@pytest.fixture
def hops():
    a, b, c = new_connection(), new_connection(), new_connection()
    a.execute("CREATE TABLE users(id INTEGER PRIMARY KEY, name VARCHAR)")
    a.execute("INSERT INTO users VALUES (1, 'ada'), (2, 'grace'), (3, 'alan')")
    yield a, b, c
    for con in (c, b, a):
        con.close()


def chain(a, b, c, inner, outer, pk_overrides=None):
    bridge(a, b, inner)
    return bridge(b, c, outer, pk_overrides=pk_overrides, source_catalog="app")


def handshake(source, target, source_catalog, context, grants):
    """Register, set context, grant and attach; `grants` is a list of (table, verb, using)."""
    bridge_id = unique_id()
    token = source.execute("SELECT bridge_register_source(?, ?)", [bridge_id, source_catalog]).fetchone()[0]
    for name, value in context.items():
        source.execute("SELECT bridge_set_context(?, ?, ?)", [bridge_id, name, value])
    for table, verb, using in grants:
        if verb == "insert":
            source.execute("SELECT bridge_policy(?, ?, ?, ?, ?)", [bridge_id, table, verb, using, "true"])
        else:
            source.execute("SELECT bridge_policy(?, ?, ?, ?)", [bridge_id, table, verb, using])
    target.execute(f"ATTACH '' AS app (TYPE virtual_catalog_bridge, ID '{bridge_id}', TOKEN '{token}')")
    return bridge_id


def test_two_hop_read(hops):
    a, b, c = hops
    chain(a, b, c, {"users": READ}, {"users": READ})
    assert c.execute("SELECT id, name FROM app.main.users ORDER BY id").fetchall() == [
        (1, "ada"),
        (2, "grace"),
        (3, "alan"),
    ]
    assert c.execute("SELECT count(*) FROM app.main.users WHERE id > 1").fetchone() == (2,)


def test_two_hop_filter_crosses_into_the_first_hop(hops):
    a, b, c = hops
    chain(a, b, c, {"users": READ}, {"users": READ})
    plan = c.execute("EXPLAIN SELECT name FROM app.main.users WHERE id = 2").fetchall()[0][1]
    assert "CROSSING_READ" in plan
    assert "FILTER" in plan.replace(" ", "")


def test_two_hop_writes_with_the_key_declared_again(hops):
    a, b, c = hops
    chain(a, b, c, {"users": READWRITE}, {"users": READWRITE}, pk_overrides={"users": "id"})
    c.execute("INSERT INTO app.main.users VALUES (4, 'edsger')")
    c.execute("UPDATE app.main.users SET name = 'ADA' WHERE id = 1")
    c.execute("DELETE FROM app.main.users WHERE id = 3")
    assert a.execute("SELECT id, name FROM users ORDER BY id").fetchall() == [
        (1, "ADA"),
        (2, "grace"),
        (4, "edsger"),
    ]


def test_two_hop_writes_find_the_native_key_through_the_first_hop(hops):
    """The first hop describes the source table with its constraints, so the outer grant discovers
    the native primary key without a bridge_primary_key."""
    a, b, c = hops
    chain(a, b, c, {"users": READWRITE}, {"users": READWRITE})
    c.execute("UPDATE app.main.users SET name = 'ADA' WHERE id = 1")
    c.execute("DELETE FROM app.main.users WHERE id = 3")
    assert a.execute("SELECT id, name FROM users ORDER BY id").fetchall() == [(1, "ADA"), (2, "grace")]


def test_rollback_on_the_outer_hop_reaches_the_source(hops):
    a, b, c = hops
    chain(a, b, c, {"users": READWRITE}, {"users": READWRITE}, pk_overrides={"users": "id"})
    c.execute("BEGIN")
    c.execute("INSERT INTO app.main.users VALUES (4, 'edsger')")
    c.execute("DELETE FROM app.main.users WHERE id = 1")
    c.execute("ROLLBACK")
    assert a.execute("SELECT id FROM users ORDER BY id").fetchall() == [(1,), (2,), (3,)]


def test_inner_policy_still_applies(hops):
    a, b, c = hops
    chain(a, b, c, {"users": {"select": "id <= 2"}}, {"users": READ})
    assert c.execute("SELECT id FROM app.main.users ORDER BY id").fetchall() == [(1,), (2,)]


def test_outer_policy_narrows(hops):
    a, b, c = hops
    chain(a, b, c, {"users": READ}, {"users": {"select": "id = 3"}})
    assert c.execute("SELECT id FROM app.main.users").fetchall() == [(3,)]


def test_each_hop_applies_its_own_context(hops):
    a, b, c = hops
    handshake(a, b, "memory", {"lim": 2}, [("main.users", "select", "id <= $lim")])
    handshake(b, c, "app", {"lim": 2}, [("main.users", "select", "id >= $lim")])
    assert c.execute("SELECT id FROM app.main.users").fetchall() == [(2,)]


def test_two_hop_ddl_when_granted(hops):
    a, b, c = hops
    handshake(a, b, "memory", {}, [("main", "create", "true"), ("main.users", "select", "true")])
    handshake(b, c, "app", {}, [("main", "create", "true"), ("main.users", "select", "true")])
    c.execute("CREATE TABLE app.main.made(id INTEGER PRIMARY KEY)")
    c.execute("INSERT INTO app.main.made VALUES (1)")
    assert a.execute("SELECT id FROM made").fetchall() == [(1,)]


def test_detaching_the_inner_hop_fails_the_outer_clearly(hops):
    a, b, c = hops
    chain(a, b, c, {"users": READ}, {"users": READ})
    b.execute("DETACH app")
    with pytest.raises(Exception, match="source catalog 'app' was detached"):
        c.execute("SELECT count(*) FROM app.main.users").fetchall()


def test_renaming_the_inner_hop_is_followed(hops):
    a, b, c = hops
    chain(a, b, c, {"users": READ}, {"users": READ})
    b.execute("ALTER DATABASE app SET ALIAS TO renamed")
    assert c.execute("SELECT count(*) FROM app.main.users").fetchone() == (3,)


def test_chain_within_one_instance():
    con = new_connection()
    con.execute("CREATE TABLE users(id INTEGER PRIMARY KEY, name VARCHAR)")
    con.execute("INSERT INTO users VALUES (1, 'ada'), (2, 'grace')")
    bridge(con, con, {"users": READWRITE}, catalog="b1")
    bridge(con, con, {"users": READWRITE}, catalog="c1", source_catalog="b1")
    con.execute("UPDATE c1.main.users SET name = 'ADA' WHERE id = 1")
    assert con.execute("SELECT id, name FROM c1.main.users ORDER BY id").fetchall() == [(1, "ADA"), (2, "grace")]
    assert con.execute("SELECT name FROM users WHERE id = 1").fetchone() == ("ADA",)
    con.close()


def test_two_hop_read_streams(hops):
    """Rows past the first five million raise, so a hop that materializes its result fails; one that
    streams hands back the first rows long before reaching them."""
    a, b, c = hops
    for con in (a, b, c):
        con.execute("SET threads = 1")
    a.execute(
        "CREATE VIEW v AS SELECT i, CASE WHEN i < 5000000 THEN i ELSE error('materialized')::BIGINT END AS x "
        "FROM range(10000000) t(i)"
    )
    assert len(a.execute("SELECT i, x FROM v").fetchmany(10)) == 10
    chain(a, b, c, {"v": READ}, {"v": READ})
    assert len(c.execute("SELECT i, x FROM app.main.v").fetchmany(10)) == 10
