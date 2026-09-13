"""OP_AGGREGATE over the real wire, server-side only.

Speaks the protocol directly against the running test server, so the aggregate
op is proven before any client-side optimizer exists. Each result is checked
against the equivalent OP_QUERY, which is the same DuckDB doing the same work by
a different route.

db.main.users is seeded as (1,'Alice',30), (2,'Bob',25), (3,'Charlie',35).

Run:
    uv run pytest integration_tests/test_aggregate_op.py -v
"""

from typing import Any

import pyarrow as pa
import pytest

from _targets import REGISTER, TARGETS, connect
from n6k_protocol.protocol import CAP_AGGREGATE_PUSHDOWN, OP_AGGREGATE, OP_QUERY


@pytest.mark.asyncio
async def test_grouped_aggregate_matches_equivalent_query(server):
    async with connect(REGISTER) as (client, _):
        agg = await client.table(
            OP_AGGREGATE,
            schema="main",
            table="users",
            group_by=["age"],
            aggregates=[{"fn": "count"}, {"fn": "sum", "col": "id"}],
        )
        ref = await client.table(
            OP_QUERY,
            sql='SELECT age AS "g0", count(*) AS "a0", sum(id) AS "a1" ' "FROM db.main.users GROUP BY 1 ORDER BY 1",
        )
        assert agg.column_names == ["g0", "a0", "a1"]
        assert agg.schema == ref.schema
        assert agg.sort_by("g0").to_pylist() == ref.sort_by("g0").to_pylist()


@pytest.mark.asyncio
async def test_ungrouped_aggregate_returns_one_row(server):
    async with connect(REGISTER) as (client, _):
        agg = await client.table(
            OP_AGGREGATE,
            schema="main",
            table="users",
            group_by=[],
            aggregates=[{"fn": "count"}, {"fn": "min", "col": "name"}],
        )
        assert agg.num_rows == 1
        assert agg.to_pylist() == [{"a0": 3, "a1": "Alice"}]


@pytest.mark.asyncio
async def test_empty_match_still_returns_one_ungrouped_row(server):
    """An ungrouped aggregate over zero rows is one row (0, NULL), matching
    local DuckDB semantics — the client plan expects exactly that shape."""
    async with connect(REGISTER) as (client, _):
        agg = await client.table(
            OP_AGGREGATE,
            schema="main",
            table="users",
            filters=[["id", "<", 0]],
            group_by=[],
            aggregates=[{"fn": "count"}, {"fn": "min", "col": "name"}],
        )
        assert agg.to_pylist() == [{"a0": 0, "a1": None}]


@pytest.mark.asyncio
async def test_grouped_aggregate_over_no_rows_is_empty(server):
    async with connect(REGISTER) as (client, _):
        agg = await client.table(
            OP_AGGREGATE,
            schema="main",
            table="users",
            filters=[["id", "<", 0]],
            group_by=["age"],
            aggregates=[{"fn": "count"}],
        )
        assert agg.num_rows == 0


@pytest.mark.asyncio
async def test_filters_apply_including_nested_or(server):
    async with connect(REGISTER) as (client, _):
        agg = await client.table(
            OP_AGGREGATE,
            schema="main",
            table="users",
            filters=[["or", [["age", ">", 30], ["age", "<", 26]]]],
            group_by=[],
            aggregates=[{"fn": "count"}],
        )
        assert agg.to_pylist() == [{"a0": 2}]


@pytest.mark.asyncio
async def test_sum_of_integer_arrives_as_decimal(server):
    """sum(INTEGER) is HUGEINT in DuckDB but has no lossless Arrow encoding by
    default, so it ships as decimal128(38,0). The client reconciles that against
    its planned HUGEINT; this pins the wire half of that contract."""
    async with connect(REGISTER) as (client, _):
        agg = await client.table(
            OP_AGGREGATE,
            schema="main",
            table="users",
            group_by=[],
            aggregates=[{"fn": "sum", "col": "id"}],
        )
        assert agg.schema.field(0).type == pa.decimal128(38, 0)
        assert agg.to_pylist() == [{"a0": 6}]


# ── Every server, one SQL builder ───────────────────────────────────────────
#
# Every target composes its aggregate SQL with the same builder
# (src/common/n6k_sql_builder.cpp), which tests reach through the
# `n6k_testing_build_aggregate_sql` scalar, so they must agree exactly — not merely all be
# plausible. Every target seeds the same `db.main.users` rows from the shared
# fixtures.sql, which is what makes them directly comparable.


def _rows(table: pa.Table) -> list[str]:
    """Rows as sorted, comparable strings. `repr` rather than the values
    themselves because a row may mix types that don't order against each other
    (and may hold None), which is enough for an equality comparison."""
    return sorted(repr(sorted(row.items())) for row in table.to_pylist())


@pytest.mark.parametrize("target", TARGETS, ids=str)
@pytest.mark.asyncio
async def test_capability_is_advertised_by_every_server(server, target):
    """The client's optimizer rule rewrites the plan before any request is sent
    and cannot fall back, so the capability has to arrive with the handshake."""
    async with connect(target) as (_, hello):
        assert CAP_AGGREGATE_PUSHDOWN in hello.get("capabilities", [])


@pytest.mark.asyncio
async def test_all_servers_build_identical_aggregates(server):
    """Same request, same rows, same generated aliases, from every server."""
    cases: list[dict[str, Any]] = [
        # Ungrouped: count(*) alongside an aggregate that takes a column.
        {"group_by": [], "aggregates": [{"fn": "count"}, {"fn": "avg", "col": "age"}]},
        # Grouped, with a WHERE that must precede the GROUP BY.
        {
            "filters": [["age", ">=", 26]],
            "group_by": ["name"],
            "aggregates": [{"fn": "sum", "col": "age"}],
        },
        # A disjunction has to survive as one: flattened into the AND-joined top
        # level it would match nothing.
        {
            "filters": [["or", [["age", ">", 33], ["age", "<", 26]]]],
            "group_by": [],
            "aggregates": [{"fn": "count"}],
        },
    ]

    for case in cases:
        results = {}
        for target in TARGETS:
            async with connect(target) as (client, _):
                results[target.id] = await client.table(OP_AGGREGATE, schema="main", table="users", **case)

        reference_id, reference = next(iter(results.items()))
        for target_id, result in results.items():
            assert result.schema.names == reference.schema.names, (target_id, case)
            # A GROUP BY has no inherent row order and neither builder adds an
            # ORDER BY, so compare as sets of rows — the hash aggregates are
            # free to emit the same rows in different orders.
            assert _rows(result) == _rows(reference), (target_id, reference_id, case)


@pytest.mark.parametrize("target", TARGETS, ids=str)
@pytest.mark.asyncio
async def test_every_server_rejects_the_same_bad_input(server, target):
    """The whitelists are the point of sharing the builder: no server may
    let an unknown function or operator reach the statement."""
    async with connect(target) as (client, _):
        err = await client.error(
            OP_AGGREGATE,
            schema="main",
            table="users",
            group_by=[],
            aggregates=[{"fn": "median", "col": "age"}],
        )
        assert "unsupported aggregate function" in err["exception_message"]

        err = await client.error(
            OP_AGGREGATE,
            schema="main",
            table="users",
            filters=[["id", "; DROP TABLE users --", 1]],
            group_by=[],
            aggregates=[{"fn": "count"}],
        )
        assert "unsupported filter operator" in err["exception_message"]

        # A client-supplied alias is advisory only and must never be interpolated.
        agg = await client.table(
            OP_AGGREGATE,
            schema="main",
            table="users",
            group_by=[],
            aggregates=[{"fn": "count", "alias": '"; DROP TABLE users --'}],
        )
        assert agg.schema.names == ["a0"]
        assert agg.to_pylist() == [{"a0": 3}]
