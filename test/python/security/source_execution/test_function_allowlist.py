"""ATTACK  target pushes a call the source answers differently: a built-in that reads source state, or a
        source function behind a target function's name (E4, E5)
BREACH  the call runs on the source without the owner allowing it
PASS    only PushableBuiltins and the names the owner adds to the whitelist cross; the rest run on the target
"""

import duckdb
import pytest

from conftest import bridge, new_connection
from security.world import POLICY, SETUP

FUNCTION = "tag_of"


@pytest.fixture
def world():
    """Both sides define `tag_of`; the source's records its calls. Yields (source, target, bridge_id, calls)."""
    calls = []

    def source_tag(x):
        calls.append(x)
        return x

    source, target = new_connection(), new_connection()
    source.execute(SETUP)
    source.create_function(FUNCTION, source_tag, ["INTEGER"], "INTEGER")
    target.create_function(FUNCTION, lambda x: x, ["INTEGER"], "INTEGER")
    bridge_id = bridge(source, target, {"users": {"select": POLICY}})
    yield source, target, bridge_id, calls
    source.close()
    target.close()


def runs_on_target(target, sql, name):
    plan = target.execute("EXPLAIN " + sql).fetchall()[0][1]
    return name in plan.split("CROSSING_READ")[0]


def tag_query(target):
    return target.execute(f"SELECT id FROM app.main.users WHERE {FUNCTION}(tenant) = 1 ORDER BY id").fetchall()


def test_default_builtin_is_pushed(world):
    _, target, _, _ = world
    assert not runs_on_target(target, "SELECT id FROM app.main.users WHERE abs(tenant) = 1", "abs")


def test_disallow_keeps_builtin_on_target(world):
    source, target, bridge_id, _ = world
    source.execute("SELECT bridge_set_policy(?, ':functions', 'whitelist', 'remove', 'abs')", [bridge_id])

    assert runs_on_target(target, "SELECT id FROM app.main.users WHERE abs(tenant) = 1", "abs")
    assert target.execute("SELECT id FROM app.main.users WHERE abs(tenant) = 1 ORDER BY id").fetchall() == [
        (1,),
        (2,),
    ]


def test_exact_whitelist_drops_the_builtins(world):
    source, target, bridge_id, _ = world
    source.execute("SELECT bridge_set_policy(?, ':functions', 'whitelist', 'set', ['abs'])", [bridge_id])

    assert not runs_on_target(target, "SELECT id FROM app.main.users WHERE abs(tenant) = 1", "abs")
    assert runs_on_target(target, "SELECT id FROM app.main.users WHERE sign(tenant) = 1", "sign")


def test_source_function_needs_allow(world):
    source, target, bridge_id, calls = world
    assert tag_query(target) == [(1,), (2,)]
    assert calls == []

    source.execute("SELECT bridge_set_policy(?, ':functions', 'whitelist', 'add', ?)", [bridge_id, FUNCTION])
    assert tag_query(target) == [(1,), (2,)]
    assert calls


def test_disallow_overrides_allow(world):
    source, target, bridge_id, calls = world
    source.execute("SELECT bridge_set_policy(?, ':functions', 'whitelist', 'add', ?)", [bridge_id, FUNCTION])
    source.execute("SELECT bridge_set_policy(?, ':functions', 'whitelist', 'remove', ?)", [bridge_id, FUNCTION])

    assert tag_query(target) == [(1,), (2,)]
    assert calls == []


@pytest.mark.parametrize("op", ["add", "remove", "set"])
def test_target_cannot_change_functions(world, op):
    _, target, bridge_id, _ = world
    with pytest.raises(duckdb.PermissionException):
        target.execute(
            "SELECT bridge_set_policy(?, ':functions', 'whitelist', ?, ['current_setting'])", [bridge_id, op]
        )


@pytest.mark.parametrize("op", ["add", "remove", "set"])
def test_refused_after_finalize(world, op):
    source, _, bridge_id, _ = world
    source.execute("SELECT bridge_finalize_policy(?)", [bridge_id])
    with pytest.raises(duckdb.PermissionException, match="finalized"):
        source.execute("SELECT bridge_set_policy(?, ':functions', 'whitelist', ?, ['abs'])", [bridge_id, op])


@pytest.mark.parametrize("name", [None, ""])
def test_function_name_is_required(world, name):
    source, _, bridge_id, _ = world
    with pytest.raises(duckdb.IOException):
        source.execute("SELECT bridge_set_policy(?, ':functions', 'whitelist', 'add', ?)", [bridge_id, name])
