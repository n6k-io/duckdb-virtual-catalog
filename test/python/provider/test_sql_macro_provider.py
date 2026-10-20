"""A provider served by SQL macros alone: no host API is needed to hand the provider Arrow bytes."""

import pyarrow as pa

from conftest import schema_message, stream_bytes

TABLE = pa.table({"id": pa.array([1, 2], type=pa.int64())})


def test_sql_macros_can_serve_a_provider_table(con):
    con.execute("CREATE MACRO m_list() AS 'main.t'")
    con.execute(f"CREATE MACRO m_schema(name) AS from_hex('{schema_message(TABLE.schema).hex()}')")
    con.execute(f"CREATE MACRO m_scan(name, columns, filters) AS from_hex('{stream_bytes(TABLE).hex()}')")
    con.execute("ATTACH '' AS app (TYPE virtual_catalog_provider, list m_list, schema m_schema, scan m_scan)")

    assert con.execute("SELECT id FROM app.main.t ORDER BY id").fetchall() == [(1,), (2,)]
