"""ATTACK  target filters a bridged table with constants built to break out of the remote SQL the
        scanner generates for a pushed-down filter
BREACH  the canary comes back, rows differ from the same query run directly, or `secret` changes
PASS    the constant is matched as data
"""

import pytest

from conftest import READ, bridge_source
from security.upstream.world import leaked, secret_intact

PAYLOADS = [
    "x' OR '1'='1",
    "x'; DROP TABLE secret; --",
    "x\\' OR 1=1 --",
    "x' UNION SELECT password, NULL, NULL FROM secret --",
    "x' OR id IN (SELECT id FROM secret) --",
    "x/*",
    "*/ OR 1=1 --",
    "x$$ OR true --",
    "x$q$ OR true --",
    'x" OR "1"="1',
    "x` OR 1=1 --",
    "x\\",
    "%' OR '1'='1",
]

SHAPES = {
    "eq": "SELECT * FROM {users} WHERE name = {p} ORDER BY id",
    "not_eq": "SELECT * FROM {users} WHERE name <> {p} ORDER BY id",
    "like": "SELECT * FROM {users} WHERE name LIKE {p} ORDER BY id",
    "in_list": "SELECT * FROM {users} WHERE name IN ({p}, 'ana') ORDER BY id",
    "gt": "SELECT * FROM {users} WHERE name > {p} ORDER BY id",
}


def literal(payload):
    return "'" + payload.replace("'", "''") + "'"


@pytest.fixture
def pair(open_upstream, target):
    direct = open_upstream()
    bridged = open_upstream()
    bridge_source(bridged, target, {"users": READ})
    return direct, bridged


@pytest.mark.parametrize("shape", SHAPES.values(), ids=SHAPES.keys())
@pytest.mark.parametrize("payload", PAYLOADS)
def test_filter_constant_stays_data(pair, target, shape, payload):
    direct, bridged = pair
    expected = direct.con.execute(shape.format(users="users", p=literal(payload))).fetchall()

    try:
        rows = target.execute(shape.format(users=bridged.target("users"), p=literal(payload))).fetchall()
    except Exception as error:  # noqa: BLE001 -- refusing is safe, but the oracle answered
        pytest.fail(f"bridge refused what the upstream answers directly: {error}")

    assert not leaked(rows), f"leaked {leaked(rows)}"
    assert rows == expected
    assert secret_intact(bridged)


@pytest.mark.parametrize("payload", PAYLOADS)
def test_prepared_parameter_stays_data(pair, target, payload):
    direct, bridged = pair
    expected = direct.con.execute("SELECT * FROM users WHERE name = ? ORDER BY id", [payload]).fetchall()

    rows = target.execute(f"SELECT * FROM {bridged.target('users')} WHERE name = ? ORDER BY id", [payload]).fetchall()

    assert not leaked(rows), f"leaked {leaked(rows)}"
    assert rows == expected
    assert secret_intact(bridged)
