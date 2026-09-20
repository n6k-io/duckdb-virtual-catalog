from conftest import READ, bridge


def test_count_star_two_instances(source, target):
    source.execute("CREATE TABLE memory.main.users(id INTEGER PRIMARY KEY, name VARCHAR)")
    source.execute("INSERT INTO memory.main.users VALUES (1,'ada'),(2,'grace')")
    bridge(source, target, {"users": READ})
    assert target.execute("SELECT count(*) FROM app.main.users").fetchone()[0] == 2
    assert target.execute("SELECT count(*) FROM app.main.users").fetchone()[0] == 2
    assert target.execute("SELECT count(*) FROM app.main.users WHERE id > 1").fetchone()[0] == 1
