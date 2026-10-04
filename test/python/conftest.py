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

HERE = pathlib.Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[1]
BUILT_EXTENSIONS = REPO_ROOT / "build/release/extension"
BRIDGE_EXTENSION = BUILT_EXTENSIONS / "virtual_catalog_bridge/virtual_catalog_bridge.duckdb_extension"
PROVIDER_EXTENSION = BUILT_EXTENSIONS / "virtual_catalog_provider/virtual_catalog_provider.duckdb_extension"

VARCHAR = duckdb.sqltype("VARCHAR")
BLOB = duckdb.sqltype("BLOB")
BIGINT = duckdb.sqltype("BIGINT")
VARCHAR_LIST = duckdb.sqltype("VARCHAR[]")


#: Directory name -> marker. security/ drives both paths and takes neither.
DIRECTORY_MARKERS = {"bridge": "bridge", "provider": "provider"}


def pytest_collection_modifyitems(items):
    for item in items:
        directory = pathlib.Path(item.fspath).resolve().parent
        if directory.parent != HERE:
            continue
        marker = DIRECTORY_MARKERS.get(directory.name)
        if marker:
            item.add_marker(marker)


def new_connection(*, bridge=True, provider=False):
    """A connection with its own DatabaseInstance and the locally built extensions loaded.

    The two are separate binaries; load only what the test needs, or both where it
    exercises them side by side in separate catalogs.
    """
    wanted = ([BRIDGE_EXTENSION] if bridge else []) + ([PROVIDER_EXTENSION] if provider else [])
    for extension in wanted:
        if not extension.exists():
            pytest.skip(f"extension not built: {extension} -- run `make release`")
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    for extension in wanted:
        con.execute(f"LOAD '{extension}'")
    return con


def fn(con, name):
    """The handshake function `name`. Kept so call sites read the same as the ATTACH type helper."""
    return f"bridge_{name}"


def attach_type(con):
    """The ATTACH TYPE string for the bridge."""
    return "virtual_catalog_bridge"


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
    c = new_connection(bridge=True, provider=True)
    yield c
    c.close()


def unique_id(prefix="b"):
    """Bridge ids and provider probes are process-wide, and pytest reuses one process."""
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


#: Verb shorthands for the common grant sets. A bare list means every verb unrestricted.
READ = ["select"]
READWRITE = ["select", "insert", "update", "delete"]


def bridge(
    source,
    target,
    grants,
    pk_overrides=None,
    catalog="app",
    schema="main",
    source_catalog="memory",
):
    """Run the documented handshake across two connections. Returns the bridge id.

    `grants` maps a table to either a list of verbs (each granted unrestricted) or a dict of
    verb -> predicate. A predicate is the USING half, or a `(using, check)` pair to set the
    WITH CHECK half too. Grant names are `schema.table`; a bare name is qualified with `schema`
    so the many call sites that only care about one schema stay terse.

    `insert` has no USING predicate to inherit a check from, so bridge_policy requires one
    explicitly. A case that says nothing about the write boundary gets `'true'` -- the same
    opt-out it would have to spell out in SQL. Pass a pair to mean anything else.
    """

    def qualify(name):
        return name if "." in name else f"{schema}.{name}"

    bridge_id = unique_id()
    token = source.execute(
        "SELECT bridge_register_source(?, ?)",
        [bridge_id, source_catalog],
    ).fetchone()[0]
    # Keys first: discovery runs on the first grant, and for a keyless table an update/delete grant
    # is refused before a later override could rescue it.
    for table, columns in (pk_overrides or {}).items():
        if isinstance(columns, str):
            columns = [c.strip() for c in columns.split(",") if c.strip()]
        source.execute("SELECT bridge_primary_key(?, ?, ?)", [bridge_id, qualify(table), columns])
    for table, verbs in grants.items():
        if not isinstance(verbs, dict):
            verbs = {verb: "true" for verb in verbs}
        for verb, predicate in verbs.items():
            if isinstance(predicate, tuple):
                using, check = predicate
            else:
                using, check = predicate, ("true" if verb == "insert" else None)
            if check is None:
                source.execute("SELECT bridge_policy(?, ?, ?, ?)", [bridge_id, qualify(table), verb, using])
            else:
                source.execute("SELECT bridge_policy(?, ?, ?, ?, ?)", [bridge_id, qualify(table), verb, using, check])
    # ATTACH options are folded to constants at bind time, so the token cannot be a parameter.
    target.execute(f"ATTACH '' AS {catalog} (TYPE virtual_catalog_bridge, ID '{bridge_id}', TOKEN '{token}')")
    return bridge_id


def unbridge(target, catalog="app"):
    """Tear down a bridge. One ATTACH is one bridge, so the catalog is the handle."""
    return target.execute(f"DETACH {catalog}").fetchall()


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
