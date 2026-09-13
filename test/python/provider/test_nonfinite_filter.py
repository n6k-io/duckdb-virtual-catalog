"""A non-finite DOUBLE in a pushed filter must not silently drop the whole filter set.

filter_json.hpp serializes FLOAT/DOUBLE with yyjson_mut_real. yyjson refuses to write inf/NaN with
flags 0 and returns NULL, so SerializeFilters produces an empty string while still reporting
all_exact -- SerializeFlatFiltersOrThrow then does not throw, the provider is called with no filters
at all, and DuckDB does not re-apply filters pushed into an Arrow scan. The query returns every row.
"""

import pyarrow as pa
import pytest

from provider_stub import FakeProvider

MEASUREMENTS = pa.table(
    {
        "id": pa.array([1, 2, 3], type=pa.int32()),
        "d": pa.array([1.5, 20.0, 300.0], type=pa.float64()),
    }
)


@pytest.fixture
def provider(con):
    con.execute("ATTACH ':memory:' AS app (TYPE virtual_catalog_provider)")
    p = FakeProvider(con)
    p.add_table("measurements", MEASUREMENTS, primary_key=["id"])
    p.register()
    return p


def test_finite_filter_is_pushed(con, provider):
    assert con.execute("SELECT id FROM app.main.measurements WHERE d < 100.0 ORDER BY id").fetchall() == [(1,), (2,)]


# The predicates that should return nothing are the ones that expose the bug: a dropped filter set
# reads as "no restriction", so they come back with every row instead. The ones that should return
# everything cannot tell the two apart, and pass either way.
DROPPED_FILTER_IS_VISIBLE = pytest.mark.xfail(
    strict=True, reason="yyjson cannot write inf/NaN, so the filter set is silently dropped"
)


@pytest.mark.parametrize(
    "predicate,expected",
    [
        ("d < 'inf'::DOUBLE", [(1,), (2,), (3,)]),
        pytest.param("d > 'inf'::DOUBLE", [], marks=DROPPED_FILTER_IS_VISIBLE),
        pytest.param("d < '-inf'::DOUBLE", [], marks=DROPPED_FILTER_IS_VISIBLE),
        ("d > '-inf'::DOUBLE", [(1,), (2,), (3,)]),
        pytest.param("d = 'nan'::DOUBLE", [], marks=DROPPED_FILTER_IS_VISIBLE),
        ("d < 'nan'::DOUBLE", [(1,), (2,), (3,)]),
    ],
)
def test_nonfinite_filter_is_applied(con, provider, predicate, expected):
    assert con.execute(f"SELECT id FROM app.main.measurements WHERE {predicate} ORDER BY id").fetchall() == expected


@DROPPED_FILTER_IS_VISIBLE
def test_nonfinite_filter_reaches_the_provider(con, provider):
    """The provider must be handed the filter, or told the scan could not push it -- never an empty
    filter string, which reads as 'no restriction'."""
    con.execute("SELECT id FROM app.main.measurements WHERE d > 'inf'::DOUBLE").fetchall()
    scans = [c for c in provider.calls if c[0] == "scan"]
    assert scans, "provider was never scanned"
    filters = scans[-1][-1]
    assert filters not in ("", None), f"filters were dropped: {filters!r}"
