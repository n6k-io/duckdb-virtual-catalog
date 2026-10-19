"""ATTACK  a provider answers with a frame that only a verifier or full validation would reject
BREACH  the process dies (SIGSEGV / SIGBUS), hangs, or the frame is accepted
PASS    a clean duckdb.Error

Each probe runs in a child process: a crash inside pytest takes the session with it.
"""

import signal
import struct
import subprocess
import sys

import pyarrow as pa
import pytest

from conftest import REPO_ROOT, schema_message

TEST_DIR = REPO_ROOT / "test" / "python"


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


FRAMES = {
    "A1_root_offset": (corrupt_root_offset, "DESCRIBE app.main.t"),
    "A3_deep_schema": (deep_schema, "DESCRIBE app.main.t"),
    "A5_offsets": (non_monotonic_offsets, "SELECT c0 FROM app.main.t"),
}

PROBE = """
import sys
sys.path.insert(0, {test_dir!r})
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
"""


@pytest.mark.parametrize("name", FRAMES, ids=FRAMES.keys())
def test_hostile_frame_is_refused_cleanly(name):
    proc = subprocess.run(
        [sys.executable, "-u", "-c", PROBE.format(test_dir=str(TEST_DIR), name=name)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )
    killed = f" ({signal.Signals(-proc.returncode).name})" if proc.returncode < 0 else ""
    detail = f"exit {proc.returncode}{killed}\n--- stdout ---\n{proc.stdout}\n--- stderr ---\n{proc.stderr[-2000:]}"
    assert proc.returncode == 0, detail
    assert proc.stdout.startswith("REFUSED"), detail
