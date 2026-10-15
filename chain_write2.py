import sys
sys.path.insert(0, "/Users/sean/Documents/github.com/n6k-io/duckdb-virtual-catalog/test/python")
from conftest import bridge, new_connection, READWRITE
for sql in ["UPDATE app.main.users SET name = 'X'",
            "DELETE FROM app.main.users WHERE id = 3",
            "DELETE FROM app.main.users"]:
    a, b, c = new_connection(), new_connection(), new_connection()
    a.execute("CREATE TABLE users(id INTEGER PRIMARY KEY, name VARCHAR)")
    a.execute("INSERT INTO users VALUES (1, 'ada'), (2, 'grace'), (3, 'alan')")
    bridge(a, b, {"users": READWRITE})
    bridge(b, c, {"users": READWRITE}, pk_overrides={"users": "id"}, source_catalog="app")
    try:
        c.execute(sql); print("OK  ", sql, a.execute("select * from users order by id").fetchall())
    except Exception as e:
        print("FAIL", sql, str(e).splitlines()[0])
