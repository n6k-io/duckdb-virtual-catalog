"""A primary key whose Arrow layout is narrower or wider than its DuckDB InternalType must still round-trip."""

import pyarrow as pa
import pytest

from provider_stub import FakeProvider

KEYS = {
    "time32": pa.array([1000, 2000, 3000], pa.time32("ms")),
    "date64": pa.array([86400000, 2 * 86400000, 3 * 86400000], pa.date64()),
}


@pytest.fixture(params=KEYS.values(), ids=KEYS.keys())
def provider(con, request):
    con.execute("ATTACH ':memory:' AS app (TYPE vcatprovider)")
    p = FakeProvider(con)
    p.add_table("events", pa.table({"k": request.param, "label": pa.array(["a", "b", "c"])}), primary_key=["k"])
    p.register()
    return p


def test_delete_by_temporal_key_removes_exactly_that_row(con, provider):
    con.execute("DELETE FROM app.main.events WHERE label = 'b'")
    assert sorted(r["label"] for r in provider.rows("events")) == ["a", "c"]


def test_update_by_temporal_key_touches_exactly_that_row(con, provider):
    con.execute("UPDATE app.main.events SET label = 'B' WHERE label = 'b'")
    assert [r["label"] for r in sorted(provider.rows("events"), key=lambda r: r["k"])] == ["a", "B", "c"]
