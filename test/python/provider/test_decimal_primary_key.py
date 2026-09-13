"""A primary key whose Arrow layout is wider than its DuckDB InternalType must still round-trip.

provider_scan_shared.hpp's ReadArrowValue switches on LogicalType::InternalType() and indexes
buffers[1] with that C++ type's stride. An Arrow decimal128 column maps to DuckDB DECIMAL(9,2),
whose InternalType() is INT32 -- but the Arrow buffer holds 16 bytes per element. Element n is read
at byte offset 4*n of a 16-byte-stride buffer, and the garbage integer is then cast to DECIMAL, so
the key handed to the delete/update UDF names the wrong row or no row at all.
"""

import decimal

import pyarrow as pa
import pytest

from provider_stub import FakeProvider

PRICES = pa.table(
    {
        "price": pa.array(
            [decimal.Decimal("1.23"), decimal.Decimal("4.56"), decimal.Decimal("7.89")], type=pa.decimal128(9, 2)
        ),
        "label": pa.array(["a", "b", "c"]),
    }
)


@pytest.fixture
def provider(con):
    con.execute("ATTACH ':memory:' AS app (TYPE virtual_catalog_provider)")
    p = FakeProvider(con)
    p.add_table("prices", PRICES, primary_key=["price"])
    p.register()
    return p


def test_decimal_key_reads_back(con, provider):
    assert con.execute("SELECT price, label FROM app.main.prices ORDER BY price").fetchall() == [
        (decimal.Decimal("1.23"), "a"),
        (decimal.Decimal("4.56"), "b"),
        (decimal.Decimal("7.89"), "c"),
    ]


def test_delete_by_decimal_key_removes_exactly_that_row(con, provider):
    con.execute("DELETE FROM app.main.prices WHERE label = 'b'")
    assert sorted(r["label"] for r in provider.rows("prices")) == ["a", "c"]


def test_update_by_decimal_key_touches_exactly_that_row(con, provider):
    con.execute("UPDATE app.main.prices SET label = 'B' WHERE label = 'b'")
    assert {r["price"]: r["label"] for r in provider.rows("prices")} == {
        decimal.Decimal("1.23"): "a",
        decimal.Decimal("4.56"): "B",
        decimal.Decimal("7.89"): "c",
    }
