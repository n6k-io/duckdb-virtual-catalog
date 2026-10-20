"""ATTACK  target creates a view over a policy-restricted bridged table
BREACH  the view is created on the source, or reading it returns a hidden row
PASS    the view stays on the target and returns only visible rows
"""

import pytest

from security.world import HIDDEN, UNGRANTED

SHAPES = [
    "SELECT * FROM app.main.users",
    "SELECT id FROM app.main.users WHERE tenant = 2",
    "SELECT u.id FROM app.main.users u JOIN app.main.users v USING (id)",
]


@pytest.mark.parametrize("create_grant", [False, True], ids=["no_create", "create"])
@pytest.mark.parametrize("body", SHAPES, ids=["all", "hidden_filter", "self_join"])
def test_target_view_stays_on_target(attacker, body, create_grant):
    source, target, bridge_id = attacker
    if create_grant:
        source.execute("SELECT bridge_policy(?, 'main', 'create', 'true')", [bridge_id])

    target.execute(f"CREATE VIEW app.main.v AS {body}")

    reached = {row[0] for row in target.execute("SELECT * FROM app.main.v").fetchall()}
    assert not (reached & HIDDEN), f"view reached hidden rows {sorted(reached & HIDDEN)}"
    assert source.execute("SELECT view_name FROM duckdb_views() WHERE NOT internal").fetchall() == []


def test_target_view_cannot_name_ungranted_table(attacker):
    _, target, _ = attacker
    with pytest.raises(Exception, match="does not exist"):  # noqa: B017
        target.execute(f"CREATE VIEW app.main.v AS SELECT * FROM memory.main.{UNGRANTED}")
        target.execute("SELECT * FROM app.main.v").fetchall()
