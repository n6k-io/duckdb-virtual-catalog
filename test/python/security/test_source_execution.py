"""ATTACK  target calls a function the source defines and the target does not
BREACH  the source evaluates it -- arbitrary target-authored expressions run source-side
PASS    the target's binder refuses the name

`doBad` exists on the source and nowhere else, and announces itself when called, so a breach names
itself in the failure output instead of being inferred from a missing-function error.
"""

import pytest

from security.world import POLICY, SETUP

BREACH = "BREACH: doBad ran on the source"

#: Every expression position a query can put a function call in.
POSITIONS = {
    "projection": "SELECT doBad(id) FROM app.main.users",
    "where": "SELECT id FROM app.main.users WHERE doBad(id) > 0",
    "order_by": "SELECT id FROM app.main.users ORDER BY doBad(id)",
    "group_by": "SELECT doBad(id), count(*) FROM app.main.users GROUP BY 1",
    "having": "SELECT tenant FROM app.main.users GROUP BY tenant HAVING doBad(sum(id)) > 0",
    "aggregate_arg": "SELECT sum(doBad(id)) FROM app.main.users",
    "case": "SELECT CASE WHEN id > 1 THEN doBad(id) ELSE 0 END FROM app.main.users",
    "subquery": "SELECT id FROM app.main.users WHERE id IN (SELECT doBad(id) FROM app.main.users)",
}


@pytest.fixture
def source_only_udf(bridged):
    """A bridge whose source knows `doBad` and whose target does not. Yields (target, calls)."""
    source, target, handshake = bridged
    calls = []

    def do_bad(x):
        calls.append(x)
        raise RuntimeError(BREACH)

    source.execute(SETUP)
    source.create_function("doBad", do_bad, ["INTEGER"], "INTEGER")
    handshake(source, target, {"users": {"select": POLICY}})
    return target, calls


@pytest.mark.parametrize("sql", POSITIONS.values(), ids=POSITIONS.keys())
def test_target_function_never_executes_on_the_source(source_only_udf, sql):
    target, calls = source_only_udf

    with pytest.raises(Exception) as caught:
        target.execute(sql).fetchall()

    assert BREACH not in str(caught.value), "expression evaluated on the source"
    assert not calls, f"source ran doBad {len(calls)}x on {calls}"
    # The pass is specifically the target's binder refusing an unknown name. Without this the test
    # stays green on any unrelated failure, which is how a confinement test rots into a no-op.
    assert "does not exist" in str(caught.value), f"failed for another reason: {caught.value}"
