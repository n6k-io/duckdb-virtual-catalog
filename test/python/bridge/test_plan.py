"""The plan-carrying bridge path.

The scan advertises no filter pushdown: it builds a LogicalOperator on the target and executes it on
the source through LogicalPlanStatement. What is pinned here is that a plan built on one
DatabaseInstance runs on the other, that the rows match the source oracle, and that the boundary this
gives up -- DuckDB no longer folds anything into the scan -- has not let anything through it should
not.
"""

import re

import duckdb
import pytest

from conftest import READ, attach_type, bridge, fn, unique_id

ROWS = 5_000

SETUP = f"""
CREATE TABLE wide AS
SELECT
    i::INTEGER                                AS id,
    'user_' || (i % 500)                      AS name,
    'city_' || (i % 97)                       AS city,
    ((i * 7) % 1000)::INTEGER                 AS score,
    (i % 1000) / 7.0                          AS ratio,
    DATE '2020-01-01' + ((i % 2000)::INTEGER) AS joined
FROM range({ROWS}) t(i);
"""


@pytest.fixture
def bridged(source, target):
    """A bridge over `wide`, and the two connections behind it."""
    source.execute(SETUP)
    bridge(source, target, {"wide": READ})
    return source, target


def explain(con, sql):
    return con.execute("EXPLAIN " + sql).fetchall()[0][1]


def test_a_self_join_crosses_as_one_scan(bridged):
    """Both sides of the join move to the source and arrive as a single fragment, so the target is
    left with one scan and the join runs where the rows are."""
    _, target = bridged
    plan = explain(target, "SELECT count(*) FROM app.main.wide a JOIN app.main.wide b USING (id)")
    flat = plan.replace(" ", "")
    assert len(re.findall(r"TableIndex:(\d+)", flat)) == 1
    assert flat.count("CROSSING_TABLE_SCAN") == 1
    assert "COMPARISON_JOIN" in flat
    assert flat.count("memory.main.wide") == 2


def test_scan_node_carries_the_source_plan(bridged):
    """The node holds the plan the source will run, and EXPLAIN prints it without executing."""
    _, target = bridged
    plan = explain(target, "SELECT sum(score) FROM app.main.wide")
    assert "Plan" in plan
    assert "SEQ_SCAN" in plan


def test_select_star_matches_source(bridged):
    source, target = bridged
    native = source.execute("SELECT * FROM wide ORDER BY id").fetchall()
    bridged = target.execute("SELECT * FROM app.main.wide ORDER BY id").fetchall()
    assert bridged == native


def test_aggregate_matches_source(bridged):
    source, target = bridged
    native = source.execute("SELECT count(*), sum(score), max(name) FROM wide").fetchone()
    bridged = target.execute("SELECT count(*), sum(score), max(name) FROM app.main.wide").fetchone()
    assert bridged == native


def test_filter_and_projection_match_source(bridged):
    """Neither is pushed. The answer still has to be right."""
    sql = "SELECT city, count(*) FROM {t} WHERE score > 500 GROUP BY city ORDER BY city"
    source, target = bridged
    native = source.execute(sql.format(t="wide")).fetchall()
    bridged = target.execute(sql.format(t="app.main.wide")).fetchall()
    assert bridged == native


def plan_columns(con, sql):
    """How many columns the plan inside the fence reads from the source."""
    plan = explain(con, sql).replace(" ", "")
    return int(re.search(r"PlanColumns:(\d+)", plan).group(1))


def test_plan_reads_only_the_columns_the_query_needs(bridged):
    assert plan_columns(target_of(bridged), "SELECT * FROM app.main.wide") == 6
    assert plan_columns(target_of(bridged), "SELECT sum(score) FROM app.main.wide") == 1
    assert plan_columns(target_of(bridged), "SELECT city, sum(score) FROM app.main.wide GROUP BY city") == 2
    # COUNT(*) needs no column at all, so the plan reads one to produce rows.
    assert plan_columns(target_of(bridged), "SELECT count(*) FROM app.main.wide") == 1


def target_of(bridged):
    return bridged[1]


def scan_rows(con, sql):
    """Rows the bridge scan emitted. The scan is the bottom-most node, and the plan it carries is
    drawn without cardinalities, so the last count in the profile is the scan's."""
    profile = con.execute("EXPLAIN ANALYZE " + sql).fetchall()[0][1]
    return int(re.findall(r"([\d,]+) rows", profile)[-1].replace(",", ""))


def test_filter_is_absorbed(bridged):
    source, target = bridged
    sql = "SELECT count(*) FROM {t} WHERE score > 500 AND joined >= DATE '2022-01-01'"
    expected = source.execute(sql.format(t="wide")).fetchone()[0]
    assert target.execute(sql.format(t="app.main.wide")).fetchone()[0] == expected
    assert scan_rows(target, sql.format(t="app.main.wide")) < ROWS


def test_filter_is_absorbed_under_a_group_by(bridged):
    """A GROUP BY gives the FILTER a projection_map, so the node has to stay even when every
    predicate crossed. The rows must still be filtered on the source."""
    source, target = bridged
    sql = "SELECT city, count(*) FROM {t} WHERE score > 500 GROUP BY city ORDER BY city"
    expected = source.execute(sql.format(t="wide")).fetchall()
    assert target.execute(sql.format(t="app.main.wide")).fetchall() == expected
    assert scan_rows(target, sql.format(t="app.main.wide")) < ROWS


def test_unabsorbable_conjunct_stays_outside(bridged):
    """One half of the predicate can cross and the other cannot; the absorbable half still does."""
    _, target = bridged
    target.create_function(
        "target_only", lambda s: s.endswith("7"), [duckdb.sqltype("VARCHAR")], duckdb.sqltype("BOOLEAN")
    )
    sql = "SELECT count(*) FROM app.main.wide WHERE score > 500 AND target_only(name)"
    native = target.execute("SELECT count(*) FROM app.main.wide WHERE score > 500 AND name LIKE '%7'").fetchone()[0]
    assert target.execute(sql).fetchone()[0] == native
    assert scan_rows(target, sql) < ROWS


def test_target_udf_stays_on_the_target(bridged):
    """A function that exists only on the target must never reach the source.

    DuckDB only hands arbitrary expressions to a table function that sets pushdown_expression. The
    scan must not set it, so the FILTER stays above the scan.
    """
    _, target = bridged
    target.create_function(
        "target_only", lambda s: s.endswith("7"), [duckdb.sqltype("VARCHAR")], duckdb.sqltype("BOOLEAN")
    )
    sql = "SELECT count(*) FROM app.main.wide WHERE target_only(name)"
    assert "FILTER" in explain(target, sql)
    assert target.execute(sql).fetchone()[0] == ROWS // 10
    # Nothing was absorbable, so every row still crosses and the UDF runs on the target.
    assert scan_rows(target, sql) == ROWS


def test_scans_do_not_leak_across_statements(bridged):
    """Each scan opens and closes its own source transaction; a second query must still work."""
    _, target = bridged
    for _ in range(3):
        assert target.execute("SELECT count(*) FROM app.main.wide").fetchone()[0] == ROWS


def test_two_scans_of_one_bridge_run_in_sequence(bridged):
    """Both scans share the transaction's one source connection, which runs one query at a time.
    They do not overlap -- each drains and releases the connection before the next opens -- so the
    join works. If that ever stops being true the guard in BridgeSourceTransactions throws rather
    than letting the second scan silently truncate the first."""
    source, target = bridged
    native = source.execute("SELECT count(*) FROM wide a JOIN wide b USING (id)").fetchone()
    bridged = target.execute("SELECT count(*) FROM app.main.wide a JOIN app.main.wide b USING (id)").fetchone()
    assert bridged == native


def test_scans_in_one_transaction_share_a_snapshot(source, target):
    """The source connection lives as long as the target transaction, so a second statement in it
    sees what the first saw -- not a row inserted on the source in between."""
    source.execute(SETUP)
    bridge(source, target, {"wide": READ})

    target.execute("BEGIN")
    first = target.execute("SELECT count(*) FROM app.main.wide").fetchone()[0]
    source.execute("INSERT INTO wide VALUES (99999, 'x', 'x', 1, 1.0, DATE '2021-01-01')")
    second = target.execute("SELECT count(*) FROM app.main.wide").fetchone()[0]
    target.execute("COMMIT")

    assert first == ROWS
    assert second == first
    assert target.execute("SELECT count(*) FROM app.main.wide").fetchone()[0] == ROWS + 1


# --- row policies ---------------------------------------------------------------------------------


def test_row_policy_is_enforced(source, target):
    source.execute(SETUP)
    bridge(source, target, {"wide": {"select": "score > 500"}})
    expected = source.execute("SELECT count(*) FROM wide WHERE score > 500").fetchone()[0]
    assert target.execute("SELECT count(*) FROM app.main.wide").fetchone()[0] == expected


def test_row_policy_on_a_column_the_query_never_reads(source, target):
    """The predicate's column has to reach the plan even though nothing selects it."""
    source.execute(SETUP)
    bridge(source, target, {"wide": {"select": "city = 'city_42'"}})
    expected = source.execute("SELECT sum(score) FROM wide WHERE city = 'city_42'").fetchone()[0]
    assert target.execute("SELECT sum(score) FROM app.main.wide").fetchone()[0] == expected


def test_row_policy_survives_an_absorbed_filter(source, target):
    """An absorbed predicate stacks on top of the policy; it must not replace or bypass it."""
    source.execute(SETUP)
    bridge(source, target, {"wide": {"select": "city = 'city_42'"}})
    expected = source.execute("SELECT count(*) FROM wide WHERE city = 'city_42' AND score > 500").fetchone()[0]
    assert target.execute("SELECT count(*) FROM app.main.wide WHERE score > 500").fetchone()[0] == expected


# --- writes ---------------------------------------------------------------------------------------

WRITABLE = ["select", "insert"]


@pytest.fixture
def writable(source, target):
    source.execute(SETUP)
    bridge(source, target, {"wide": WRITABLE})
    return source, target


ROW = "(99999, 'x', 'x', 1, 1.0, DATE '2021-01-01')"


def test_insert_lands_on_the_source(writable):
    source, target = writable
    target.execute(f"INSERT INTO app.main.wide VALUES {ROW}")
    assert source.execute("SELECT count(*) FROM wide WHERE id = 99999").fetchone()[0] == 1


def test_insert_from_a_select(writable):
    """Reads and writes the same bridge in one statement. The write feeds the source a chunk at a
    time, so its query is open while the scan is still draining -- outside a transaction it takes a
    connection of its own, so the two do not collide."""
    source, target = writable
    expected = source.execute("SELECT count(*) FROM wide WHERE score > 990").fetchone()[0]
    target.execute("INSERT INTO app.main.wide SELECT * FROM app.main.wide WHERE score > 990")
    assert source.execute("SELECT count(*) FROM wide").fetchone()[0] == ROWS + expected


def test_insert_commits_with_the_target(writable):
    source, target = writable
    target.execute("BEGIN")
    target.execute(f"INSERT INTO app.main.wide VALUES {ROW}")
    target.execute("COMMIT")
    assert source.execute("SELECT count(*) FROM wide WHERE id = 99999").fetchone()[0] == 1


def test_insert_rolls_back_with_the_target(writable):
    """The source connection belongs to the target's transaction, so aborting the target aborts the
    source write too."""
    source, target = writable
    target.execute("BEGIN")
    target.execute(f"INSERT INTO app.main.wide VALUES {ROW}")
    target.execute("ROLLBACK")
    assert source.execute("SELECT count(*) FROM wide WHERE id = 99999").fetchone()[0] == 0


def test_insert_carries_a_plan(writable):
    """The write fence holds a plan whose leaf is the seam, not a bare pile of rows."""
    _, target = writable
    profile = target.execute(f"EXPLAIN ANALYZE INSERT INTO app.main.wide VALUES {ROW}").fetchall()[0][1]
    assert "Plan" in profile
    assert "INSERT" in profile
    # A literal VALUES list is something the source can produce itself, so it goes into the seam while
    # the plan is still being built and the statement runs there whole. A feed the source cannot hold
    # fills the seam at execution instead, and shows a chunk scan.
    assert "insert on source" in profile or "COLUMN_DATA_SCAN" in profile or "CHUNK_GET" in profile


@pytest.fixture
def checked(source, target):
    """Insert granted with a check predicate: rows written must satisfy it."""
    source.execute(SETUP)
    bridge_id = unique_id()
    token = source.execute(f"SELECT {fn(source, 'register_source')}(?, 'memory')", [bridge_id]).fetchone()[0]
    source.execute(f"SELECT {fn(source, 'policy')}(?, 'main.wide', 'select', 'true')", [bridge_id])
    source.execute(f"SELECT {fn(source, 'policy')}(?, 'main.wide', 'insert', 'true', ?)", [bridge_id, "score < 100"])
    target.execute(f"ATTACH '' AS app (TYPE {attach_type(target)}, ID '{bridge_id}', TOKEN '{token}')")
    return source, target


def test_check_allows_a_conforming_row(checked):
    source, target = checked
    target.execute("INSERT INTO app.main.wide VALUES (99999, 'x', 'x', 1, 1.0, DATE '2021-01-01')")
    assert source.execute("SELECT count(*) FROM wide WHERE id = 99999").fetchone()[0] == 1


def test_check_rejects_a_violating_row(checked):
    source, target = checked
    with pytest.raises(duckdb.Error, match="violates the check predicate"):
        target.execute("INSERT INTO app.main.wide VALUES (99999, 'x', 'x', 500, 1.0, DATE '2021-01-01')")
    assert source.execute("SELECT count(*) FROM wide WHERE id = 99999").fetchone()[0] == 0


def test_check_rejects_a_null(checked):
    """A check that evaluates to NULL is a violation, not a pass."""
    source, target = checked
    with pytest.raises(duckdb.Error, match="violates the check predicate"):
        target.execute("INSERT INTO app.main.wide VALUES (99999, 'x', 'x', NULL, 1.0, DATE '2021-01-01')")
    assert source.execute("SELECT count(*) FROM wide WHERE id = 99999").fetchone()[0] == 0


def test_one_violating_row_rejects_the_whole_statement(checked):
    source, target = checked
    with pytest.raises(duckdb.Error, match="violates the check predicate"):
        target.execute(
            "INSERT INTO app.main.wide "
            "VALUES (99998, 'x', 'x', 1, 1.0, DATE '2021-01-01'), "
            "       (99999, 'x', 'x', 500, 1.0, DATE '2021-01-01')"
        )
    assert source.execute("SELECT count(*) FROM wide WHERE id >= 99998").fetchone()[0] == 0


def test_check_is_refused_on_select(source):
    source.execute(SETUP)
    bridge_id = unique_id()
    source.execute(f"SELECT {fn(source, 'register_source')}(?, 'memory')", [bridge_id])
    with pytest.raises(duckdb.Error, match="only supported for 'insert'"):
        source.execute(f"SELECT {fn(source, 'policy')}(?, 'main.wide', 'select', 'true', 'score < 100')", [bridge_id])


def test_insert_without_a_grant_is_refused(bridged):
    _, target = bridged
    with pytest.raises(duckdb.Error, match="does not have 'insert' permission"):
        target.execute(f"INSERT INTO app.main.wide VALUES {ROW}")


# --- update ---------------------------------------------------------------------------------------


@pytest.fixture
def updatable(source, target):
    source.execute("CREATE TABLE keyed(id INTEGER PRIMARY KEY, name VARCHAR, score INTEGER)")
    source.execute("INSERT INTO keyed VALUES (1, 'ana', 10), (2, 'bo', 20), (3, 'cy', 30)")
    bridge(source, target, {"keyed": ["select", "update"]})
    return source, target


def keyed_rows(source):
    return source.execute("SELECT * FROM keyed ORDER BY id").fetchall()


def test_update_lands_on_the_source(updatable):
    source, target = updatable
    target.execute("UPDATE app.main.keyed SET score = 99 WHERE id = 2")
    assert keyed_rows(source) == [(1, "ana", 10), (2, "bo", 99), (3, "cy", 30)]


def test_update_touching_several_rows(updatable):
    source, target = updatable
    target.execute("UPDATE app.main.keyed SET score = score + 1 WHERE id < 3")
    assert keyed_rows(source) == [(1, "ana", 11), (2, "bo", 21), (3, "cy", 30)]


def test_update_matching_nothing_changes_nothing(updatable):
    source, target = updatable
    before = keyed_rows(source)
    target.execute("UPDATE app.main.keyed SET score = 0 WHERE id = 999")
    assert keyed_rows(source) == before


def test_update_several_columns(updatable):
    source, target = updatable
    target.execute("UPDATE app.main.keyed SET name = 'zz', score = 7 WHERE id = 1")
    assert keyed_rows(source) == [(1, "zz", 7), (2, "bo", 20), (3, "cy", 30)]


def test_update_rolls_back_with_the_target(updatable):
    source, target = updatable
    target.execute("BEGIN")
    target.execute("UPDATE app.main.keyed SET score = 0")
    target.execute("ROLLBACK")
    assert keyed_rows(source) == [(1, "ana", 10), (2, "bo", 20), (3, "cy", 30)]


def test_update_with_a_composite_primary_key(source, target):
    source.execute(
        "CREATE TABLE events(tenant INTEGER, eid INTEGER, note VARCHAR, PRIMARY KEY(tenant, eid));"
        "INSERT INTO events VALUES (1, 1, 'a'), (1, 2, 'b'), (2, 1, 'c')"
    )
    bridge(source, target, {"events": ["select", "update"]})
    target.execute("UPDATE app.main.events SET note = 'B' WHERE tenant = 1 AND eid = 2")
    assert source.execute("SELECT * FROM events ORDER BY tenant, eid").fetchall() == [
        (1, 1, "a"),
        (1, 2, "B"),
        (2, 1, "c"),
    ]


def test_update_on_a_keyless_table_with_a_declared_key(source, target):
    source.execute("CREATE TABLE nopk(x INTEGER, y VARCHAR); INSERT INTO nopk VALUES (1, 'a'), (2, 'b')")
    bridge(source, target, {"nopk": ["select", "update"]}, pk_overrides={"nopk": "x"})
    target.execute("UPDATE app.main.nopk SET y = 'A' WHERE x = 1")
    assert source.execute("SELECT * FROM nopk ORDER BY x").fetchall() == [(1, "A"), (2, "b")]


def test_update_spanning_several_chunks_writes_every_row(source, target):
    """The sink holds narrowed chunks until Finalize, so it must own their storage: the child refills
    one output chunk in place between calls."""
    source.execute("CREATE TABLE big(id INTEGER PRIMARY KEY, name VARCHAR)")
    source.execute("INSERT INTO big SELECT i, 'u' || i FROM range(1, 6001) t(i)")
    bridge(source, target, {"big": ["select", "update"]})
    target.execute("UPDATE app.main.big SET name = 'set'")
    assert source.execute("SELECT count(*) FROM big WHERE name = 'set'").fetchone() == (6000,)


def test_update_refuses_a_declared_key_that_is_not_unique(source, target):
    """The key is declared, not verified, so a duplicate would silently update several source rows
    per key. The statement is refused instead."""
    source.execute("CREATE TABLE dup(x INTEGER, y VARCHAR); INSERT INTO dup VALUES (1, 'a'), (1, 'b')")
    bridge(source, target, {"dup": ["select", "update"]}, pk_overrides={"dup": "x"})
    with pytest.raises(duckdb.Error, match="is not unique on the source"):
        target.execute("UPDATE app.main.dup SET y = 'A' WHERE x = 1")
    assert source.execute("SELECT * FROM dup ORDER BY y").fetchall() == [(1, "a"), (1, "b")]


@pytest.fixture
def checked_update(source, target):
    """Update granted with a check: the row as it will be after the update must satisfy it."""
    source.execute("CREATE TABLE keyed(id INTEGER PRIMARY KEY, name VARCHAR, score INTEGER)")
    source.execute("INSERT INTO keyed VALUES (1, 'ana', 10), (2, 'bo', 20)")
    bridge_id = unique_id()
    token = source.execute(f"SELECT {fn(source, 'register_source')}(?, 'memory')", [bridge_id]).fetchone()[0]
    source.execute(f"SELECT {fn(source, 'policy')}(?, 'main.keyed', 'select', 'true')", [bridge_id])
    source.execute(f"SELECT {fn(source, 'policy')}(?, 'main.keyed', 'update', 'true', ?)", [bridge_id, "score < 100"])
    target.execute(f"ATTACH '' AS app (TYPE {attach_type(target)}, ID '{bridge_id}', TOKEN '{token}')")
    return source, target


def test_update_check_allows_a_conforming_row(checked_update):
    source, target = checked_update
    target.execute("UPDATE app.main.keyed SET score = 50 WHERE id = 1")
    assert source.execute("SELECT score FROM keyed WHERE id = 1").fetchone() == (50,)


def test_update_check_rejects_the_new_value(checked_update):
    """The check reads the post-update row: 500 violates it even though the old value did not."""
    source, target = checked_update
    with pytest.raises(duckdb.Error, match="violates the check predicate"):
        target.execute("UPDATE app.main.keyed SET score = 500 WHERE id = 1")
    assert source.execute("SELECT score FROM keyed WHERE id = 1").fetchone() == (10,)


def test_update_check_rejects_a_null(checked_update):
    source, target = checked_update
    with pytest.raises(duckdb.Error, match="violates the check predicate"):
        target.execute("UPDATE app.main.keyed SET score = NULL WHERE id = 1")
    assert source.execute("SELECT score FROM keyed WHERE id = 1").fetchone() == (10,)


def test_update_check_reads_columns_the_statement_does_not_set(source, target):
    """A check on a column the update leaves alone reads it from the source row."""
    source.execute("CREATE TABLE keyed(id INTEGER PRIMARY KEY, name VARCHAR, score INTEGER)")
    source.execute("INSERT INTO keyed VALUES (1, 'ana', 10), (2, 'blocked', 20)")
    bridge_id = unique_id()
    token = source.execute(f"SELECT {fn(source, 'register_source')}(?, 'memory')", [bridge_id]).fetchone()[0]
    source.execute(f"SELECT {fn(source, 'policy')}(?, 'main.keyed', 'select', 'true')", [bridge_id])
    source.execute(
        f"SELECT {fn(source, 'policy')}(?, 'main.keyed', 'update', 'true', ?)", [bridge_id, "name <> 'blocked'"]
    )
    target.execute(f"ATTACH '' AS app (TYPE {attach_type(target)}, ID '{bridge_id}', TOKEN '{token}')")

    target.execute("UPDATE app.main.keyed SET score = 1 WHERE id = 1")
    assert source.execute("SELECT score FROM keyed WHERE id = 1").fetchone() == (1,)
    with pytest.raises(duckdb.Error, match="violates the check predicate"):
        target.execute("UPDATE app.main.keyed SET score = 1 WHERE id = 2")


# --- delete ---------------------------------------------------------------------------------------


@pytest.fixture
def deletable(source, target):
    source.execute("CREATE TABLE keyed(id INTEGER PRIMARY KEY, name VARCHAR, score INTEGER)")
    source.execute("INSERT INTO keyed VALUES (1, 'ana', 10), (2, 'bo', 20), (3, 'cy', 30)")
    bridge(source, target, {"keyed": ["select", "delete"]})
    return source, target


def test_delete_lands_on_the_source(deletable):
    source, target = deletable
    target.execute("DELETE FROM app.main.keyed WHERE id = 2")
    assert source.execute("SELECT id FROM keyed ORDER BY id").fetchall() == [(1,), (3,)]


def test_delete_all(deletable):
    source, target = deletable
    target.execute("DELETE FROM app.main.keyed")
    assert source.execute("SELECT count(*) FROM keyed").fetchone() == (0,)


def test_delete_matching_nothing_changes_nothing(deletable):
    source, target = deletable
    target.execute("DELETE FROM app.main.keyed WHERE id = 999")
    assert source.execute("SELECT count(*) FROM keyed").fetchone() == (3,)


def test_delete_rolls_back_with_the_target(deletable):
    source, target = deletable
    target.execute("BEGIN")
    target.execute("DELETE FROM app.main.keyed")
    target.execute("ROLLBACK")
    assert source.execute("SELECT count(*) FROM keyed").fetchone() == (3,)


def test_delete_with_a_composite_primary_key(source, target):
    source.execute(
        "CREATE TABLE events(tenant INTEGER, eid INTEGER, note VARCHAR, PRIMARY KEY(tenant, eid));"
        "INSERT INTO events VALUES (1, 1, 'a'), (1, 2, 'b'), (2, 1, 'c')"
    )
    bridge(source, target, {"events": ["select", "delete"]})
    target.execute("DELETE FROM app.main.events WHERE tenant = 2")
    assert source.execute("SELECT tenant, eid FROM events ORDER BY tenant, eid").fetchall() == [
        (1, 1),
        (1, 2),
    ]


def test_delete_refuses_a_declared_key_that_is_not_unique(source, target):
    source.execute("CREATE TABLE dup(x INTEGER, y VARCHAR); INSERT INTO dup VALUES (1, 'a'), (1, 'b')")
    bridge(source, target, {"dup": ["select", "delete"]}, pk_overrides={"dup": "x"})
    with pytest.raises(duckdb.Error, match="is not unique on the source"):
        target.execute("DELETE FROM app.main.dup WHERE x = 1")
    assert source.execute("SELECT count(*) FROM dup").fetchone() == (2,)


def test_delete_spanning_several_chunks_removes_every_row(source, target):
    source.execute("CREATE TABLE big(id INTEGER PRIMARY KEY, name VARCHAR)")
    source.execute("INSERT INTO big SELECT i, 'u' || i FROM range(1, 6001) t(i)")
    bridge(source, target, {"big": ["select", "delete"]})
    target.execute("DELETE FROM app.main.big")
    assert source.execute("SELECT count(*) FROM big").fetchone() == (0,)
