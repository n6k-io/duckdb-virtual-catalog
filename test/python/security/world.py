"""The one source database every attack in this suite runs against.

    users    granted select, policy `tenant = 1`     ids 1, 2 visible / 3, 4 hidden
    secret   named by no grant at all                never reachable, by any route

Every test in the package asserts against these three constants rather than restating them, so
what is meant to be reachable is stated once.
"""

SETUP = """
CREATE TABLE users(id INTEGER PRIMARY KEY, tenant INTEGER);
INSERT INTO users VALUES (1, 1), (2, 1), (3, 2), (4, 2);

CREATE TABLE secret(id INTEGER PRIMARY KEY, password VARCHAR);
INSERT INTO secret VALUES (1, 'hunter2');
"""

#: The select policy granted on `users`.
POLICY = "tenant = 1"

#: Rows of `users` the policy excludes. No query from the target may return these.
HIDDEN = {3, 4}

#: A table no grant mentions, and the value that proves it was read.
UNGRANTED = "secret"
CANARY = "hunter2"
