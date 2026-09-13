"""Filter-clause rendering: nested groups and the operator whitelist.

The client does not re-apply pushed filters after an Arrow scan, so the WHERE
clause built here *is* the filter. Two properties matter and are pinned below:
a disjunction must stay a disjunction (flattening it into the AND-joined top
level silently drops rows), and `op` must never reach SQL unchecked, since it is
interpolated rather than bound.

The builder is C++ (`src/common/n6k_sql_builder.cpp`), shared with the reactor in
`src/n6k_server/request_handlers.cpp` and reached here through the
`n6k_testing_build_scan_sql` scalar — so these assertions cover the code both servers run.
"""

import json

import duckdb
import pytest


def _as_json(value):
    """Structured args cross into the scalar as JSON text; absent is the empty string."""
    return "" if value is None else json.dumps(value)


def scan_sql(con, catalog, schema, table, columns, filters):
    row = con.execute(
        "SELECT n6k_testing_build_scan_sql(?, ?, ?, ?, ?)",
        [catalog, schema, table, _as_json(columns), _as_json(filters)],
    ).fetchone()
    return row[0]


def where_of(con, filters):
    """The WHERE body of a scan, which is the only way filters reach SQL."""
    sql = scan_sql(con, "memory", "main", "t", None, filters)
    _, _, where = sql.partition(" WHERE ")
    return where


def test_flat_clauses_are_and_joined(n6k_con) -> None:
    assert where_of(n6k_con, [("a", ">=", 18), ("b", "=", "x")]) == '"a" >= 18 AND "b" = \'x\''


def test_or_group_stays_a_disjunction(n6k_con) -> None:
    assert where_of(n6k_con, [("or", [("a", ">", 5), ("a", "<", 2)])]) == '("a" > 5 OR "a" < 2)'


def test_and_group_nested_in_or(n6k_con) -> None:
    filters = [("or", [("and", [("a", ">", 5), ("a", "<", 10)]), ("a", "=", 0)])]
    assert where_of(n6k_con, filters) == '(("a" > 5 AND "a" < 10) OR "a" = 0)'


def test_null_and_in_operators(n6k_con) -> None:
    assert where_of(n6k_con, [("a", "is_null", None)]) == '"a" IS NULL'
    assert where_of(n6k_con, [("a", "is_not_null", None)]) == '"a" IS NOT NULL'
    assert where_of(n6k_con, [("a", "in", [1, 2])]) == '"a" IN (1, 2)'


def test_no_filters_yields_no_where(n6k_con) -> None:
    assert " WHERE " not in scan_sql(n6k_con, "memory", "main", "t", None, None)


def test_unknown_operator_is_rejected(n6k_con) -> None:
    # Previously interpolated straight into the SQL string.
    with pytest.raises(duckdb.InvalidInputException, match="unsupported filter operator"):
        where_of(n6k_con, [("a", "; DROP TABLE t --", 1)])


def test_unknown_group_kind_is_rejected(n6k_con) -> None:
    with pytest.raises(duckdb.InvalidInputException, match="unsupported filter group"):
        where_of(n6k_con, [("nand", [("a", "=", 1)])])


def test_malformed_clauses_are_rejected(n6k_con) -> None:
    # A 2-element clause is structurally a group, so it is reported as one.
    with pytest.raises(duckdb.InvalidInputException, match="unsupported filter group"):
        where_of(n6k_con, [("a", "=")])
    # Over-long: rejected rather than truncated to the first three elements.
    with pytest.raises(duckdb.InvalidInputException, match="malformed filter clause"):
        where_of(n6k_con, [("a", "=", 1, 2)])
    with pytest.raises(duckdb.InvalidInputException, match="malformed filter clause"):
        where_of(n6k_con, ["a = 1"])
    with pytest.raises(duckdb.InvalidInputException, match="malformed 'or' filter group"):
        where_of(n6k_con, [("or", [])])
    with pytest.raises(duckdb.InvalidInputException, match="'in' filter requires a list value"):
        where_of(n6k_con, [("a", "in", 1)])


def test_or_round_trips_against_real_duckdb(n6k_con) -> None:
    """The regression the nested form exists for.

    Flattening `a > 5 OR a < 2` into two AND-joined clauses yields
    `a > 5 AND a < 2`, which matches nothing.
    """
    n6k_con.execute("CREATE TABLE t (a INTEGER)")
    n6k_con.execute("INSERT INTO t VALUES (1), (3), (7)")

    sql = scan_sql(n6k_con, "memory", "main", "t", ["a"], [("or", [("a", ">", 5), ("a", "<", 2)])])
    rows = sorted(r[0] for r in n6k_con.execute(sql).fetchall())
    assert rows == [1, 7]
