"""The same statement against an upstream directly and through a bridge over an identical one.

The direct run is the oracle: the scanner, its types and the upstream's constraints are the same on
both sides, so any difference is the bridge's.
"""

import duckdb
import pytest

from backends import NativeBackend
from conftest import READ, READWRITE, bridge_source


@pytest.fixture
def pair(open_upstream, target):
    """(direct, bridged): two identically seeded upstreams, the second served to `target`."""
    direct = open_upstream()
    bridged = open_upstream()
    bridge_source(bridged, target, {"users": READWRITE, "types": READ})
    return direct, bridged


def outcome(con, sql):
    try:
        return ("ok", con.execute(sql).fetchall())
    except duckdb.Error:
        return ("error",)


def both(pair, target, sql):
    direct, bridged = pair
    names = ("users", "types")
    return (
        outcome(direct.con, sql.format(**{n: n for n in names})),
        outcome(target, sql.format(**{n: bridged.target(n) for n in names})),
    )


def state(source):
    return source.con.execute("SELECT * FROM users ORDER BY id").fetchall()


READS = {
    "all": "SELECT * FROM {users} ORDER BY id",
    "reordered_projection": "SELECT score, id FROM {users} ORDER BY id",
    "aggregate": "SELECT count(*), count(score), sum(score) FROM {users}",
    "group_by": "SELECT score IS NULL, count(*) FROM {users} GROUP BY 1 ORDER BY 1",
    "self_join": "SELECT a.id, b.id FROM {users} a JOIN {users} b ON a.score < b.score ORDER BY 1, 2",
    "limit_offset": "SELECT id FROM {users} ORDER BY id LIMIT 2 OFFSET 1",
    "eq": "SELECT id FROM {users} WHERE id = 2",
    "is_null": "SELECT id FROM {users} WHERE score IS NULL",
    "is_not_null": "SELECT id FROM {users} WHERE score IS NOT NULL ORDER BY id",
    "null_comparison": "SELECT id FROM {users} WHERE score <> 10 ORDER BY id",
    "like": "SELECT id FROM {users} WHERE name LIKE 'a%'",
    "ilike": "SELECT id FROM {users} WHERE name ILIKE 'b%'",
    "case_sensitive_eq": "SELECT id FROM {users} WHERE name = 'bo'",
    "quoted_literal": "SELECT id FROM {users} WHERE name = 'd''x'",
    "in_list": "SELECT id FROM {users} WHERE id IN (1, 3) ORDER BY id",
    "between": "SELECT id FROM {users} WHERE score BETWEEN 10 AND 20 ORDER BY id",
    "or": "SELECT id FROM {users} WHERE id = 1 OR score > 15 ORDER BY id",
    "not": "SELECT id FROM {users} WHERE NOT (score > 15) ORDER BY id",
    "bigint_exact": "SELECT id FROM {types} WHERE i = 9007199254740993",
    "float": "SELECT id FROM {types} WHERE f > 1.25",
    "decimal": "SELECT id FROM {types} WHERE n > 12",
    "text": "SELECT id FROM {types} WHERE s = 'x''y'",
    "date": "SELECT id FROM {types} WHERE d = DATE '2024-02-29'",
    "timestamp": "SELECT id FROM {types} WHERE ts > TIMESTAMP '2024-02-29 12:00:00'",
    "types_null_row": "SELECT id FROM {types} WHERE s IS NULL",
}


@pytest.mark.parametrize("sql", READS.values(), ids=READS.keys())
def test_read_matches_direct(pair, target, sql):
    direct, bridged = both(pair, target, sql)
    assert direct[0] == "ok", f"oracle failed: {sql}"
    assert bridged == direct


def test_types_round_trip(pair, target):
    direct, bridged = pair
    describe = "SELECT column_name, column_type FROM (DESCRIBE {})"
    assert (
        target.execute(describe.format(bridged.target("types"))).fetchall()
        == direct.con.execute(describe.format("types")).fetchall()
    )
    assert (
        target.execute(f"SELECT * FROM {bridged.target('types')} ORDER BY id").fetchall()
        == direct.con.execute("SELECT * FROM types ORDER BY id").fetchall()
    )


WRITES = {
    "insert": "INSERT INTO {users} VALUES (5, 'ed', 50)",
    "insert_returning": "INSERT INTO {users} VALUES (5, 'ed', 50) RETURNING *",
    "insert_select": "INSERT INTO {users} SELECT id + 10, name, score FROM {users}",
    "update": "UPDATE {users} SET score = score + 1 WHERE id <= 2",
    "update_returning": "UPDATE {users} SET name = upper(name) WHERE id = 1 RETURNING id, name",
    "update_to_null": "UPDATE {users} SET score = NULL WHERE id = 4",
    "delete": "DELETE FROM {users} WHERE score IS NULL",
    "delete_returning": "DELETE FROM {users} WHERE id = 2 RETURNING *",
    "upsert_do_update": "INSERT INTO {users} VALUES (1, 'ana2', 11) ON CONFLICT (id) DO UPDATE SET name = excluded.name",
    "upsert_do_nothing": "INSERT INTO {users} VALUES (1, 'ana2', 11) ON CONFLICT DO NOTHING",
    "insert_or_replace": "INSERT OR REPLACE INTO {users} VALUES (2, 'bo2', 21)",
}


@pytest.fixture
def duckdb_oracle():
    con = duckdb.connect()
    con.execute(NativeBackend().seed)
    yield con
    con.close()


@pytest.mark.parametrize("sql", WRITES.values(), ids=WRITES.keys())
def test_write_matches_direct(pair, target, duckdb_oracle, sql):
    direct, bridged = both(pair, target, sql)
    expected, expected_state = direct, state(pair[0])
    if direct == ("error",) and bridged != ("error",):
        # The scanner refuses what the bridge runs itself (RETURNING, ON CONFLICT); DuckDB's own
        # semantics are then the only reference.
        expected = outcome(duckdb_oracle, sql.format(users="users"))
        expected_state = duckdb_oracle.execute("SELECT * FROM users ORDER BY id").fetchall()
    assert bridged == expected
    assert state(pair[1]) == expected_state


#: Each violates a constraint the seed declares, so the oracle must refuse it.
VIOLATIONS = {
    "insert_not_null": "INSERT INTO {users} VALUES (5, NULL, 1)",
    "insert_duplicate_key": "INSERT INTO {users} VALUES (1, 'x', 1)",
    "insert_check": "INSERT INTO {users} VALUES (5, 'x', -1)",
    "update_not_null": "UPDATE {users} SET name = NULL WHERE id = 1",
    "update_duplicate_key": "UPDATE {users} SET id = 2 WHERE id = 1",
    "update_check": "UPDATE {users} SET score = -1 WHERE id = 1",
}


@pytest.mark.parametrize("sql", VIOLATIONS.values(), ids=VIOLATIONS.keys())
def test_constraint_refused_like_direct(pair, target, sql):
    direct, bridged = both(pair, target, sql)
    assert direct == ("error",), f"oracle accepted a violation: {sql}"
    assert bridged == ("error",)
    assert state(pair[1]) == state(pair[0])


TRANSACTIONS = {
    "rollback_insert": ["BEGIN", "INSERT INTO {users} VALUES (5, 'ed', 50)", "ROLLBACK"],
    "commit_insert": ["BEGIN", "INSERT INTO {users} VALUES (5, 'ed', 50)", "COMMIT"],
    "rollback_update": ["BEGIN", "UPDATE {users} SET score = 0", "ROLLBACK"],
    "rollback_delete": ["BEGIN", "DELETE FROM {users}", "ROLLBACK"],
    "read_own_write": [
        "BEGIN",
        "INSERT INTO {users} VALUES (5, 'ed', 50)",
        "SELECT count(*) FROM {users}",
        "ROLLBACK",
    ],
    "statement_after_failure": [
        "BEGIN",
        "INSERT INTO {users} VALUES (5, 'ed', 50)",
        "INSERT INTO {users} VALUES (1, 'dup', 1)",
        "ROLLBACK",
    ],
}


@pytest.mark.parametrize("statements", TRANSACTIONS.values(), ids=TRANSACTIONS.keys())
def test_transaction_matches_direct(pair, target, statements):
    outcomes = [both(pair, target, sql) for sql in statements]
    assert [bridged for _, bridged in outcomes] == [direct for direct, _ in outcomes]
    assert state(pair[1]) == state(pair[0])
