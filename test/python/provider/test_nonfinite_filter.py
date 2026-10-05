"""A non-finite DOUBLE in a pushed filter must not silently drop the whole filter set."""

import duckdb
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


@pytest.mark.parametrize(
    "predicate,expected",
    [
        ("d < 'inf'::DOUBLE", [(1,), (2,), (3,)]),
        ("d > 'inf'::DOUBLE", []),
        ("d < '-inf'::DOUBLE", []),
        ("d > '-inf'::DOUBLE", [(1,), (2,), (3,)]),
    ],
)
def test_nonfinite_filter_is_applied(con, provider, predicate, expected):
    assert con.execute(f"SELECT id FROM app.main.measurements WHERE {predicate} ORDER BY id").fetchall() == expected


@pytest.mark.parametrize("predicate", ["d = 'nan'::DOUBLE", "d < 'nan'::DOUBLE", "d IN (1.5, 'nan'::DOUBLE)"])
def test_nan_filter_is_refused(con, provider, predicate):
    with pytest.raises(duckdb.NotImplementedException, match="no wire representation"):
        con.execute(f"SELECT id FROM app.main.measurements WHERE {predicate}").fetchall()


def test_nonfinite_filter_reaches_the_provider(con, provider):
    """The provider must be handed the filter, or told the scan could not push it -- never an empty
    filter string, which reads as 'no restriction'."""
    con.execute("SELECT id FROM app.main.measurements WHERE d > 'inf'::DOUBLE").fetchall()
    scans = [c for c in provider.calls if c[0] == "scan"]
    assert scans, "provider was never scanned"
    filters = scans[-1][-1]
    assert filters not in ("", None), f"filters were dropped: {filters!r}"
