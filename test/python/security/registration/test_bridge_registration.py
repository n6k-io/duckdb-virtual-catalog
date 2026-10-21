"""ATTACK  another instance in the process registers, guesses, or outlives a bridge id
BREACH  it takes over a live bridge, keeps guessing tokens, or wedges the registry
PASS    the registry refuses, and the real source keeps control
"""

import duckdb
import pytest

from conftest import READ, bridge, new_connection, unique_id
from security.world import SETUP
from shared.test_faults import run_probe

#: Mirrors MAX_TOKEN_FAILURES in duckdb_source.cpp.
MAX_TOKEN_FAILURES = 5


def test_live_bridge_id_cannot_be_re_registered(bridged):
    source, target, handshake = bridged
    source.execute(SETUP)
    bridge_id = handshake(source, target, {"users": READ})
    intruder = new_connection()
    try:
        with pytest.raises(duckdb.IOException, match="already registered"):
            intruder.execute("SELECT bridge_register_source(?, 'memory')", [bridge_id])
    finally:
        intruder.close()
    source.execute("SELECT bridge_set_context(?, 'tenant', 1)", [bridge_id])


def register_pending(source):
    source.execute(SETUP)
    bridge_id = unique_id()
    token = source.execute("SELECT bridge_register_source(?, 'memory')", [bridge_id]).fetchone()[0]
    source.execute("SELECT bridge_set_policy(?, 'main.users', 'select', 'true')", [bridge_id])
    return bridge_id, token


def attach(target, catalog, bridge_id, token):
    target.execute(f"ATTACH '' AS {catalog} (TYPE virtual_catalog_bridge, ID '{bridge_id}', TOKEN '{token}')")


def guess_wrong(target, bridge_id, times):
    for i in range(times):
        with pytest.raises(duckdb.PermissionException, match="invalid setup token"):
            attach(target, f"guess{i}", bridge_id, f"wrong{i}")


def test_wrong_tokens_burn_the_pending_entry(source, target):
    bridge_id, token = register_pending(source)
    guess_wrong(target, bridge_id, MAX_TOKEN_FAILURES)
    with pytest.raises(duckdb.PermissionException, match="no registered source"):
        attach(target, "app", bridge_id, token)


def test_correct_token_survives_fewer_failures(source, target):
    bridge_id, token = register_pending(source)
    guess_wrong(target, bridge_id, MAX_TOKEN_FAILURES - 1)
    attach(target, "app", bridge_id, token)
    assert target.execute("SELECT count(*) FROM app.main.users").fetchone() == (4,)


def test_expired_pending_source_does_not_deadlock():
    """A pending entry can hold the last reference to an instance that owns a live bridge. Purging it
    must not run that instance's destructor while the registry lock is held."""
    out = run_probe(
        """
        import time
        from conftest import READ, bridge, new_connection, unique_id

        leaf = new_connection()
        leaf.execute("CREATE TABLE t(id INTEGER PRIMARY KEY); INSERT INTO t VALUES (1)")
        middle = new_connection()
        bridge(leaf, middle, {"t": READ})

        pending_id = unique_id()
        middle.execute("SELECT bridge_register_source(?, 'app')", [pending_id])
        middle.execute("SELECT bridge_set_policy(?, 'main.t', 'select', 'true')", [pending_id])
        middle.close()
        del middle

        time.sleep(32)  # PENDING_SOURCE_TTL_SECONDS is 30
        new_connection().execute("SELECT bridge_register_source(?, 'memory')", [unique_id()])
        print("DONE")
        """,
        timeout=60,
    )
    assert "DONE" in out, out
