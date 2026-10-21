"""ATTACK  target calls the upstream's scanner functions (`postgres_query`, `sqlite_scan`, ...),
        alone or inside a query over a bridged table, so the source would run them
BREACH  the canary comes back, or `secret` changes
PASS    a refusal, or a result that leaks nothing
"""

from security.upstream.world import leaked, secret_intact


def test_scanner_functions_do_not_reach_the_upstream(backend, exposed):
    upstream, target = exposed
    breaches = []
    for template in backend.scanner_calls:
        sql = template.format(
            users=upstream.target("users"),
            catalog=upstream.catalog,
            schema=upstream.schema,
        )
        try:
            rows = target.execute(sql).fetchall()
        except Exception:  # noqa: BLE001 -- a refusal is the pass
            rows = []
        if leaked(rows):
            breaches.append(f"{sql}\n    -> {leaked(rows)}")
        if not secret_intact(upstream):
            breaches.append(f"{sql}\n    -> secret changed")
            break

    assert not breaches, "\n".join(breaches)
