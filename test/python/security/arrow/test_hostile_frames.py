"""ATTACK  a provider answers schema() or scan() with a hostile Arrow frame
BREACH  the process crashes, hangs, or DuckDB reads the frame with the wrong layout
PASS    a clean error
"""

import struct

import duckdb
import pyarrow as pa
import pytest

from conftest import BIGINT, BLOB, VARCHAR, VARCHAR_LIST, read_stream, schema_message, stream_bytes
from provider_stub import PRIMARY_KEY_METADATA_KEY
from shared.test_faults import run_probe


def serve(con, schema_bytes, scan=lambda columns: b"", delete=lambda payload: 0):
    """Attach `app` with a single table `main.t` whose wire bytes the test controls."""

    def h_list():
        return "main.t"

    def h_schema(name):
        return schema_bytes

    def h_scan(name, columns, filters):
        return scan(list(columns))

    def h_delete(name, payload):
        return delete(payload)

    con.create_function("h_list", h_list, [], VARCHAR)
    con.create_function("h_schema", h_schema, [VARCHAR], BLOB)
    con.create_function("h_scan", h_scan, [VARCHAR, VARCHAR_LIST, VARCHAR], BLOB)
    con.create_function("h_delete", h_delete, [VARCHAR, BLOB], BIGINT)
    con.execute(
        "ATTACH '' AS app (TYPE virtual_catalog_provider, list h_list, schema h_schema, scan h_scan, delete h_delete)"
    )


# --- A2: extreme metadata length prefix ---------------------------------------------------------

#: 8 + length overflows an int32 header size for the first two.
LENGTHS = [0x7FFFFFFF, 0x7FFFFFF8, 0x80000000, 0xFFFFFFFF]


@pytest.mark.parametrize("length", LENGTHS, ids=hex)
def test_extreme_length_prefix_is_refused(con, length):
    frame = struct.pack("<iIii", -1, length, 0x7FFFFF00, 0)
    serve(con, frame)
    with pytest.raises(duckdb.IOException, match="could not decode the Arrow IPC"):
        con.execute("DESCRIBE app.main.t").fetchall()


# --- A8: a scan payload that switches schema after its first batch ------------------------------

BOUND = pa.schema([pa.field("c0", pa.int32())])

LATER_SCHEMAS = {
    "wider": pa.schema([pa.field("c0", pa.int32()), pa.field("c1", pa.int32()), pa.field("c2", pa.int32())]),
    "retyped": pa.schema([pa.field("c0", pa.utf8())]),
}


@pytest.mark.parametrize("later", LATER_SCHEMAS.values(), ids=LATER_SCHEMAS.keys())
def test_scan_payload_cannot_switch_schema(con, later):
    batch = pa.record_batch([pa.array([111111, 222222, 333333], pa.int32())], schema=BOUND)
    payload = schema_message(BOUND) + batch.serialize().to_pybytes() + schema_message(later)
    serve(con, schema_message(BOUND), lambda columns: payload)
    with pytest.raises(duckdb.IOException, match="changed its schema mid-stream"):
        con.execute("SELECT * FROM app.main.t").fetchall()


# --- A9: primary key columns whose Arrow layout differs from DuckDB's internal stride -----------

#: key array -> the refusal it must produce.
KEYS = {
    "dictionary": (
        pa.DictionaryArray.from_arrays(pa.array([0, 1, 2, 0, 1], pa.int32()), pa.array(["alpha", "beta", "gamma"])),
        "cannot be read as a key",
    ),
    "run_end_encoded": (
        pa.RunEndEncodedArray.from_arrays(pa.array([2, 4, 5], pa.int32()), pa.array([10, 20, 30])),
        "RunEndEncoded not yet supported",
    ),
}


@pytest.mark.parametrize("key, refusal", KEYS.values(), ids=KEYS.keys())
def test_key_with_foreign_layout_is_refused(con, key, refusal):
    backing = pa.table([key], names=["k"])
    schema = backing.schema.with_metadata({PRIMARY_KEY_METADATA_KEY: b"k"})

    def scan(columns):
        return stream_bytes(
            pa.table([backing.column(c) for c in columns], names=[f"c{i}" for i in range(len(columns))])
        )

    serve(con, schema_message(schema), scan)
    # DELETE reads the trailing key block, which is where the key reader runs.
    with pytest.raises(duckdb.Error, match=refusal):
        con.execute("DELETE FROM app.main.t").fetchall()


# --- A10: a trailing key column narrower than the declared key type -----------------------------


def test_key_narrower_than_declared_type_is_refused(con):
    declared = pa.schema([pa.field("k", pa.int64())]).with_metadata({PRIMARY_KEY_METADATA_KEY: b"k"})
    narrow = pa.table([pa.array([0x11111111] * 2000, pa.time32("s"))], names=["k"])
    deleted = []

    def delete(payload):
        deleted.append(read_stream(payload).column(0).to_pylist())
        return len(deleted[-1])

    serve(con, schema_message(declared), lambda columns: stream_bytes(narrow), delete)
    with pytest.raises(duckdb.Error):
        con.execute("DELETE FROM app.main.t").fetchall()

    assert deleted == [], f"keys read past the Arrow buffer reached delete(): {deleted[0][:4]}"


# --- A4: a negative bodyLength must not stall the decoder ---------------------------------------


def forge_negative_body_length():
    """A SCHEMA message whose header claims bodyLength = -(header size), so header + body sums to 0.

    pyarrow omits bodyLength when it is 0, so a vtable slot for it is appended and the table is
    repointed at the new vtable.
    """
    raw = bytearray(schema_message(pa.schema([pa.field("c0", pa.int32())])))
    prefix = 8 if struct.unpack_from("<I", raw, 0)[0] == 0xFFFFFFFF else 4
    table = prefix + struct.unpack_from("<i", raw, prefix)[0]
    old_vtable = table - struct.unpack_from("<i", raw, table)[0]
    vtable_size, table_size = struct.unpack_from("<HH", raw, old_vtable)
    slots = [struct.unpack_from("<H", raw, old_vtable + 4 + 2 * i)[0] for i in range((vtable_size - 4) // 2)]
    assert len(slots) == 3, "expected version, header_type and header only"

    while (len(raw) - prefix) % 8:
        raw.append(0)
    body_length = len(raw)
    raw += struct.pack("<q", 0)
    new_vtable = len(raw)
    # The table's inline size must cover the new field, or the verifier refuses it as out of range.
    raw += struct.pack("<HH", vtable_size + 2, max(table_size, body_length - table + 8))
    raw += b"".join(struct.pack("<H", s) for s in slots)
    raw += struct.pack("<H", body_length - table)

    struct.pack_into("<i", raw, table, table - new_vtable)
    struct.pack_into("<i", raw, prefix - 4, len(raw) - prefix)
    struct.pack_into("<q", raw, body_length, -len(raw))
    return bytes(raw)


def test_negative_body_length_is_refused():
    out = run_probe(
        """
        import duckdb
        from conftest import new_connection
        from security.arrow.test_hostile_frames import forge_negative_body_length, serve

        con = new_connection(bridge=True, provider=True)
        serve(con, forge_negative_body_length())
        try:
            con.execute("DESCRIBE app.main.t").fetchall()
            print("ACCEPTED")
        except duckdb.IOException as e:
            print("REFUSED", e)
        """,
        timeout=30,
    )
    assert "REFUSED" in out and "negative body length" in out, out


# --- A7: a stream must not change its schema after open -----------------------------------------


def test_stream_cannot_change_schema():
    out = run_probe(
        """
        import duckdb
        import pyarrow as pa
        from conftest import BLOB, VARCHAR, new_connection, schema_message, stream_bytes

        opened = pa.schema([pa.field(n, pa.int64()) for n in "abc"])
        narrower = pa.table({"a": pa.array([1, 2, 3], pa.int64())})
        state = {}

        def g_open(handle, fn, args, pushed):
            state[handle] = 0
            return schema_message(opened)

        def g_next(handle):
            state[handle] += 1
            return stream_bytes(narrower) if state[handle] == 1 else None

        def g_close(handle):
            return "ok"

        con = new_connection(bridge=True, provider=True)
        con.execute("ATTACH ':memory:' AS app (TYPE virtual_catalog_provider)")
        con.create_function("g_open", g_open, [VARCHAR, VARCHAR, VARCHAR, BLOB], BLOB, null_handling="special")
        con.create_function("g_next", g_next, [VARCHAR], BLOB, null_handling="special")
        con.create_function("g_close", g_close, [VARCHAR], VARCHAR)
        con.execute(
            "SELECT provider_create_stream_function('app', 'main', 'gen', 'g_open', 'g_next', 'g_close')"
        ).fetchall()
        try:
            print("ACCEPTED", con.execute("SELECT * FROM app.main.gen()").fetchall())
        except duckdb.Error as e:
            print("REFUSED", e)
        """,
        timeout=30,
    )
    assert "REFUSED" in out and "changed its schema" in out, out
