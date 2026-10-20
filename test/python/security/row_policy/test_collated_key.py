"""ATTACK  target writes through a primary key declared with a collation, so a visible key collates equal
        to a hidden one on the source
BREACH  a write that reads its rows reaches a row the select policy hides
PASS    a write reading its rows reaches only rows the select policy shows; one reading none reaches
        what its own policy allows
"""

import pytest

from conftest import bridge, new_connection
from security.world import outcome

#: collation -> (visible key, hidden key that collates equal to it)
COLLATIONS = {"nocase": ("a", "A"), "noaccent": ("e", "é")}

READING = {
    "delete_key": "DELETE FROM app.main.ev WHERE k = '{visible}'",
    "update_key": "UPDATE app.main.ev SET n = 7 WHERE k = '{visible}'",
    "update_from_column": "UPDATE app.main.ev SET n = n + 7",
}

#: write -> the hidden row after it, when the write's own policy allows that row
BARE = {
    "delete_all": ("DELETE FROM app.main.ev", []),
    "update_all": ("UPDATE app.main.ev SET n = 7", [(2, 7)]),
}


def collated(request, write_using):
    visible, hidden = COLLATIONS[request.param]
    source, target = new_connection(), new_connection()
    source.execute(f"CREATE TABLE ev(k VARCHAR COLLATE {request.param} PRIMARY KEY, tenant INTEGER, n INTEGER)")
    source.execute("INSERT INTO ev VALUES (?, 1, 0), (?, 2, 0)", [visible, hidden])
    bridge(source, target, {"ev": {"select": "tenant = 1", "update": write_using, "delete": write_using}})
    return source, target, visible


@pytest.fixture(params=COLLATIONS.keys())
def collated_world(request):
    source, target, visible = collated(request, "true")
    yield source, target, visible
    source.close()
    target.close()


@pytest.fixture(params=COLLATIONS.keys())
def collated_tenant_world(request):
    source, target, visible = collated(request, "tenant = 1")
    yield source, target, visible
    source.close()
    target.close()


def hidden_row(source):
    return source.execute("SELECT tenant, n FROM ev WHERE tenant = 2").fetchall()


@pytest.mark.parametrize("write", READING.values(), ids=READING.keys())
def test_reading_write_does_not_reach_hidden_row(collated_world, write):
    source, target, visible = collated_world
    result = outcome(target, write.format(visible=visible))

    assert hidden_row(source) == [(2, 0)], f"hidden row was written: {result}"


@pytest.mark.parametrize("write,after", BARE.values(), ids=BARE.keys())
def test_bare_write_reaches_what_its_policy_allows(collated_world, write, after):
    source, target, _ = collated_world
    result = outcome(target, write)

    assert result == ("ok", [(2,)])
    assert hidden_row(source) == after


@pytest.mark.parametrize("write", [write for write, _ in BARE.values()], ids=BARE.keys())
def test_bare_write_stops_at_its_policy(collated_tenant_world, write):
    source, target, _ = collated_tenant_world
    result = outcome(target, write)

    assert result == ("ok", [(1,)])
    assert hidden_row(source) == [(2, 0)]
