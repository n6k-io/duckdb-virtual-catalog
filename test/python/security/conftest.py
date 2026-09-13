"""Fixtures shared by the security suite.

Every fixture yields the target connection an attacker drives, plus whatever oracle proves a
breach: the source connection, a call log, a bridge id.

"""

import pytest

from conftest import bridge, new_connection
from security.world import POLICY, SETUP


@pytest.fixture
def bridged():
    """A source and target pair, plus the handshake that joins them."""
    connect, handshake = new_connection, bridge
    source = connect()
    target = connect()
    try:
        yield source, target, handshake
    finally:
        source.close()
        target.close()


@pytest.fixture
def attacker(bridged):
    """Target connection with `users` bridged under POLICY. Yields (source, target, bridge_id)."""
    source, target, handshake = bridged
    source.execute(SETUP)
    bridge_id = handshake(source, target, {"users": {"select": POLICY}})
    return source, target, bridge_id


@pytest.fixture
def attacker_granted_delete(bridged):
    """As `attacker`, plus an unrestricted delete grant -- the select policy is what must confine
    it. Yields (source, target)."""
    source, target, handshake = bridged
    source.execute(SETUP)
    handshake(source, target, {"users": {"select": POLICY, "delete": "true"}})
    return source, target
