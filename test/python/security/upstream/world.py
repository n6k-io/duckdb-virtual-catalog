"""The upstream every attack here runs against: `users` granted select and nothing else, and the
canary in `secret`, which no grant names."""

CANARY = "hunter2"

#: Upstream objects no grant names. A returned cell equal to one of these leaks its existence.
UNGRANTED = {"secret", "secret_view", "leak"}


def leaked(rows):
    cells = {str(cell) for row in rows for cell in row}
    return sorted(cells & ({CANARY} | UNGRANTED))


def secret_intact(upstream):
    return upstream.con.execute("SELECT * FROM secret ORDER BY id").fetchall() == [(1, CANARY)]
