"""ATTACK  target names upstream objects no grant mentions: tables, views, functions, system catalogs
BREACH  the canary, or the name of an ungranted object, comes back
PASS    a refusal, or a result that leaks nothing
"""

from security.upstream.world import leaked

COMMON = [
    "SELECT * FROM app.{schema}.secret",
    "SELECT * FROM app.{schema}.secret_view",
    'SELECT * FROM app."{schema}"."secret"',
    "SELECT * FROM app.{schema}.users u, app.{schema}.secret s",
    "SELECT table_name FROM information_schema.tables",
    "SELECT table_name FROM duckdb_tables()",
    "SELECT view_name FROM duckdb_views()",
]


def test_ungranted_upstream_objects_are_unreachable(backend, exposed):
    upstream, target = exposed
    breaches = []
    for template in COMMON + backend.upstream_reads:
        sql = template.format(schema=upstream.schema)
        try:
            rows = target.execute(sql).fetchall()
        except Exception:  # noqa: BLE001 -- a refusal is the pass
            continue
        if leaked(rows):
            breaches.append(f"{sql}\n    -> {leaked(rows)}")

    assert not breaches, "\n".join(breaches)
