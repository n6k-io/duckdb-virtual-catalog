"""ATTACK  a provider answers with a frame that only a verifier or full validation would reject
BREACH  the process dies (SIGSEGV / SIGBUS), hangs, or the frame is accepted
PASS    a clean duckdb.Error

Each probe runs in a child process: a crash inside pytest takes the session with it.
"""

import struct

import pyarrow as pa
import pytest

from conftest import schema_message, stream_bytes
from shared.test_faults import run_probe


def corrupt_root_offset():
    """A1: correct length prefix, flatbuffer root offset pointing far out of bounds."""
    raw = bytearray(schema_message(pa.schema([pa.field("c0", pa.int32())])))
    struct.pack_into("<i", raw, 8, 0x7FFFFF00)
    return bytes(raw), b""


def deep_schema():
    """A3: a struct nested past the verifier's depth limit. pyarrow itself overflows its stack
    serializing ~50k levels, so stay well below that."""
    t = pa.int32()
    for _ in range(1000):
        t = pa.struct([pa.field("c", t)])
    return schema_message(pa.schema([pa.field("root", t)])), b""


def non_monotonic_offsets():
    """A5: utf8 offsets 0, 10, 5, 15 -- in bounds, but element 1 has length -5."""
    schema = pa.schema([pa.field("c0", pa.utf8())])
    batch = pa.record_batch([pa.array(["AAAAA", "BBBBB", "CCCCC"])], schema=schema)
    raw = bytearray(batch.serialize().to_pybytes())
    at = raw.find(struct.pack("<4i", 0, 5, 10, 15))
    assert at != -1
    struct.pack_into("<4i", raw, at, 0, 10, 5, 15)
    return schema_message(schema), schema_message(schema) + bytes(raw)


def out_of_range_dictionary_index():
    """A6: index 1,000,000 into a 2-entry dictionary. FULL validation does not range-check indices."""
    schema = pa.schema([pa.field("c0", pa.dictionary(pa.int32(), pa.utf8()))])
    array = pa.DictionaryArray.from_arrays(pa.array([0, 1, 0], pa.int32()), pa.array(["x", "y"]))
    raw = bytearray(stream_bytes(pa.table([array], schema=schema)))
    at = raw.find(struct.pack("<3i", 0, 1, 0))
    assert at != -1
    struct.pack_into("<i", raw, at + 4, 1_000_000)
    return schema_message(schema), bytes(raw)


def extension_on_narrow_storage():
    """A11: int8 storage tagged arrow.uuid. DuckDB matches the extension by name alone and reads
    16 bytes per row from a 1-byte-per-row buffer."""
    field = pa.field("c0", pa.int8(), metadata={"ARROW:extension:name": "arrow.uuid"})
    schema = pa.schema([field])
    table = pa.table([pa.array([0x41] * 64, pa.int8())], schema=schema)
    return schema_message(schema), stream_bytes(table)


FRAMES = {
    "A1_root_offset": (corrupt_root_offset, "DESCRIBE app.main.t"),
    "A3_deep_schema": (deep_schema, "DESCRIBE app.main.t"),
    "A5_offsets": (non_monotonic_offsets, "SELECT c0 FROM app.main.t"),
    "A6_dictionary_index": (out_of_range_dictionary_index, "SELECT c0 FROM app.main.t"),
    "A11_extension_storage": (extension_on_narrow_storage, "SELECT c0 FROM app.main.t"),
}


@pytest.mark.parametrize("name", FRAMES, ids=FRAMES.keys())
def test_hostile_frame_is_refused_cleanly(name):
    out = run_probe(
        f"""
        import duckdb
        from conftest import new_connection
        from security.arrow.test_hostile_frames import serve
        from security.arrow.test_unverified_frames import FRAMES

        build, sql = FRAMES[{name!r}]
        schema_bytes, scan_bytes = build()
        con = new_connection(bridge=True, provider=True)
        serve(con, schema_bytes, lambda columns: scan_bytes)
        try:
            print("ACCEPTED", con.execute(sql).fetchall())
        except duckdb.Error as e:
            print("REFUSED", type(e).__name__, e)
        """,
        timeout=30,
    )
    assert out.startswith("REFUSED"), out
