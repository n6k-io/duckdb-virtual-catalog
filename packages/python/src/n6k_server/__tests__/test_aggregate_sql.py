"""n6k_testing_build_aggregate_sql: structure in, SQL out.

The client sends `{group_by, aggregates}` and never SQL text, so this is the
trust boundary for the aggregate op: `fn` is whitelisted before it reaches the
statement, identifiers are quoted, and the result aliases are generated here
rather than taken from the request.

The builder is C++ (`src/common/n6k_sql_builder.cpp`), shared with the reactor in
`src/n6k_server/request_handlers.cpp` and reached here through the
`n6k_testing_build_aggregate_sql` scalar — so these assertions cover the code both
servers run.
"""

import json

import duckdb
import pytest


def _as_json(value):
    """Structured args cross into the scalar as JSON text; absent is the empty string."""
    return "" if value is None else json.dumps(value)


def agg(con, catalog, schema, table, filters, group_by, aggregates):
    row = con.execute(
        "SELECT n6k_testing_build_aggregate_sql(?, ?, ?, ?, ?, ?)",
        [catalog, schema, table, _as_json(filters), _as_json(group_by), _as_json(aggregates)],
    ).fetchone()
    return row[0]


def test_group_by_with_aggregates(n6k_con) -> None:
    sql = agg(n6k_con, "db", "main", "events", None, ["region"], [{"fn": "sum", "col": "amount"}])
    assert sql == ('SELECT "region" AS "g0", sum("amount") AS "a0" ' 'FROM "db"."main"."events" GROUP BY 1')


def test_count_star_has_no_column(n6k_con) -> None:
    sql = agg(n6k_con, "db", "main", "t", None, [], [{"fn": "count"}])
    assert sql == 'SELECT count(*) AS "a0" FROM "db"."main"."t"'


def test_ungrouped_aggregate_omits_group_by(n6k_con) -> None:
    sql = agg(n6k_con, "db", "main", "t", None, [], [{"fn": "max", "col": "x"}])
    assert "GROUP BY" not in sql


def test_multiple_keys_group_positionally(n6k_con) -> None:
    sql = agg(n6k_con, "db", "main", "t", None, ["a", "b"], [{"fn": "count"}])
    assert sql.endswith("GROUP BY 1, 2")


def test_filters_become_a_where_clause(n6k_con) -> None:
    sql = agg(n6k_con, "db", "main", "t", [("ts", ">=", "2026-01-01")], ["region"], [{"fn": "count"}])
    assert "WHERE \"ts\" >= '2026-01-01'" in sql
    # WHERE precedes GROUP BY.
    assert sql.index("WHERE") < sql.index("GROUP BY")


def test_client_alias_is_ignored(n6k_con) -> None:
    """The contract is positional; an alias on the wire is advisory only, so it
    must never reach the statement."""
    sql = agg(n6k_con, "db", "main", "t", None, [], [{"fn": "count", "alias": '"; DROP TABLE t --'}])
    assert "DROP TABLE" not in sql
    assert sql == 'SELECT count(*) AS "a0" FROM "db"."main"."t"'


def test_unsupported_function_is_rejected(n6k_con) -> None:
    for fn in ("median", "string_agg", "count(*) FROM t --", ""):
        with pytest.raises(duckdb.InvalidInputException, match="unsupported aggregate function"):
            agg(n6k_con, "db", "main", "t", None, [], [{"fn": fn, "col": "x"}])


def test_non_count_requires_a_column(n6k_con) -> None:
    with pytest.raises(duckdb.InvalidInputException, match="requires a column"):
        agg(n6k_con, "db", "main", "t", None, [], [{"fn": "sum"}])


def test_empty_request_is_rejected(n6k_con) -> None:
    with pytest.raises(duckdb.InvalidInputException, match="no group keys and no aggregates"):
        agg(n6k_con, "db", "main", "t", None, [], [])


def test_identifiers_are_quoted(n6k_con) -> None:
    sql = agg(n6k_con, "db", 'we"ird', 'ta"ble', None, ['gr"p'], [{"fn": "sum", "col": 'a"mt'}])
    assert '"we""ird"' in sql
    assert '"ta""ble"' in sql
    assert '"gr""p"' in sql
    assert '"a""mt"' in sql


def test_round_trips_against_real_duckdb(n6k_con) -> None:
    n6k_con.execute("CREATE TABLE events (region VARCHAR, amount INTEGER)")
    n6k_con.execute("INSERT INTO events VALUES ('west', 10), ('west', 5), ('east', 7)")

    sql = agg(
        n6k_con,
        "memory",
        "main",
        "events",
        None,
        ["region"],
        [{"fn": "sum", "col": "amount"}, {"fn": "count"}],
    )
    rows = sorted(n6k_con.execute(sql).fetchall())
    assert rows == [("east", 7, 1), ("west", 15, 2)]

    # Positional contract: group keys first, then aggregates, in request order.
    assert [d[0] for d in n6k_con.execute(sql).description] == ["g0", "a0", "a1"]
