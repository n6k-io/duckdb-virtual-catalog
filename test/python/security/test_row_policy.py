"""ATTACK  target asks for exactly the rows the select policy excludes
BREACH  a tenant-2 id comes back
PASS    empty result

Fails if the policy is ORed into the source query rather than ANDed, or dropped in favour of the
pushed-down filter.
"""

import pytest

from security.world import HIDDEN

#: Each is a legal query a client may write. None may widen what the policy permits.
PREDICATES = [
    "tenant = 2",
    "tenant != 1",
    "true",
    "id = 4 OR true",
    "tenant = 1 OR tenant = 2",
    "NOT (tenant = 1)",
    "id IN (3, 4)",
    "id >= 1",
    "CASE WHEN tenant = 2 THEN true ELSE false END",
]

#: The same predicate wrapped in constructs that rewrite or re-plan the query around it.
SHAPES = [
    "SELECT id FROM app.main.users WHERE {p}",
    "SELECT id FROM (SELECT * FROM app.main.users) WHERE {p}",
    "WITH c AS (SELECT * FROM app.main.users WHERE {p}) SELECT id FROM c",
    "SELECT id FROM app.main.users WHERE {p} UNION ALL SELECT id FROM app.main.users WHERE {p}",
]


@pytest.mark.parametrize("shape", SHAPES, ids=["plain", "subquery", "cte", "union"])
@pytest.mark.parametrize("predicate", PREDICATES)
def test_policy_confines_every_predicate(attacker, predicate, shape):
    _, target, _ = attacker
    sql = shape.format(p=predicate)

    try:
        rows = target.execute(sql).fetchall()
    except Exception:  # noqa: BLE001 -- a refusal is the pass
        return

    reached = {row[0] for row in rows}
    assert not (reached & HIDDEN), f"{sql}\n  reached hidden rows {sorted(reached & HIDDEN)}"
