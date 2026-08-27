"""Shared fixtures for the cross-connection tests.

sqllogictest runs everything on one DatabaseInstance, which is not how a bridge is used and cannot
reach a provider at all (the UDFs need a host language). These tests drive the real shapes: two
DatabaseInstances in one process for the bridge, Python UDFs for providers and stream functions.
"""

import pathlib
import uuid

import duckdb
import pyarrow as pa
import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
EXTENSION = REPO_ROOT / "build/release/extension/virtual_catalog/virtual_catalog.duckdb_extension"

VARCHAR = duckdb.sqltype("VARCHAR")
BLOB = duckdb.sqltype("BLOB")
BIGINT = duckdb.sqltype("BIGINT")
VARCHAR_LIST = duckdb.sqltype("VARCHAR[]")


def new_connection():
    """A connection with its own DatabaseInstance and the locally built extension loaded."""
    if not EXTENSION.exists():
        pytest.skip(f"extension not built: {EXTENSION} -- run `make release`")
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{EXTENSION}'")
    return con


@pytest.fixture
def source():
    con = new_connection()
    yield con
    con.close()


@pytest.fixture
def target():
    con = new_connection()
    yield con
    con.close()


@pytest.fixture
def con():
    """Single connection, for provider and stream-function tests where host UDFs must live on the
    same DatabaseInstance the extension opens its internal Connection against."""
    c = new_connection()
    yield c
    c.close()


def unique_id(prefix="b"):
    """Bridge ids and provider probes are process-wide, and pytest reuses one process."""
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def bridge(source, target, permissions, pk_overrides=None, catalog="app", schema="main",
           source_catalog="memory", source_schema="main"):
    """Run the documented handshake across two connections. Returns the bridge id."""
    bridge_id = unique_id()
    target.execute(f"ATTACH ':memory:' AS {catalog} (TYPE virtual_catalog)")
    if schema != "main":
        target.execute(f"CREATE SCHEMA {catalog}.{schema}")
    token = source.execute(
        "SELECT vcat_register_source(?, ?, ?, ?::MAP(VARCHAR, VARCHAR), ?::MAP(VARCHAR, VARCHAR))",
        [bridge_id, source_catalog, source_schema, permissions, pk_overrides or {}],
    ).fetchone()[0]
    target.execute("SELECT vcat_setup_bridge(?, ?, ?, ?)", [bridge_id, token, catalog, schema])
    return bridge_id


# --- Arrow wire helpers -------------------------------------------------------------------------
# The provider contract is Arrow IPC in both directions: a bare schema message for schema(), a full
# stream (schema + batches + end-of-stream) for scan(), and the same stream shape inbound on the
# write UDFs.


def schema_message(schema: pa.Schema) -> bytes:
    return schema.serialize().to_pybytes()


def stream_bytes(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        for batch in table.to_batches():
            writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


def read_stream(blob: bytes) -> pa.Table:
    return pa.ipc.open_stream(pa.py_buffer(blob)).read_all()
