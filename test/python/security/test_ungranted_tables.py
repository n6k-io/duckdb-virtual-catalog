"""ATTACK  target names a source table that no grant mentions
BREACH  the canary value comes back
PASS    a refusal

Every bridged table is a catalog entry the grant map decides the existence of, so naming is the only
route left. The scan function these probes used to also try -- a plain table function taking a bridge
id, callable by anyone on the instance -- no longer exists.
"""

import pytest

from security.world import CANARY, UNGRANTED

ROUTES = {
    "qualified_name": "SELECT * FROM app.main.{u}",
    "schema_qualified": 'SELECT * FROM app."main".{u}',
    "quoted_name": 'SELECT * FROM app.main."{u}"',
    "in_subquery": "SELECT password FROM (SELECT * FROM app.main.{u})",
    "joined_to_granted": "SELECT s.password FROM app.main.{u} s, app.main.users",
    "in_cte": "WITH x AS (SELECT * FROM app.main.{u}) SELECT * FROM x",
    "scalar_subquery": "SELECT (SELECT password FROM app.main.{u} LIMIT 1)",
}


@pytest.mark.parametrize("sql", ROUTES.values(), ids=ROUTES.keys())
def test_ungranted_table_is_unreachable_by_any_route(attacker, sql):
    _, target, bridge_id = attacker

    try:
        rows = target.execute(sql.format(b=bridge_id, u=UNGRANTED)).fetchall()
    except Exception:  # noqa: BLE001 -- a refusal is the pass
        return

    assert CANARY not in str(rows), f"read an ungranted table -> {rows}"
