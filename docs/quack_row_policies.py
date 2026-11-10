import duckdb

REPO = "https://storage.googleapis.com/n6k-duckdb-release"
CONFIG = {"allow_unsigned_extensions": "true"}


def run(con, sql, *params):
    print(f"> {sql.strip()}")
    try:
        print(con.execute(sql, list(params)).fetchall())
    except duckdb.Error as e:
        print(f"{type(e).__name__}: {e}")


# 1. DB1: the data
db1 = duckdb.connect(config=CONFIG)
db1.execute(f"INSTALL vcatbridge FROM '{REPO}'")
db1.execute("LOAD vcatbridge")
db1.execute("""
    CREATE TABLE users (id INTEGER PRIMARY KEY, tenant_id INTEGER, name VARCHAR);
    INSERT INTO users VALUES (1, 42, 'ada'), (2, 42, 'grace'), (3, 7, 'linus');
    CREATE TABLE secrets (k VARCHAR, v VARCHAR);
""")

# 2. DB1: the bridge and its policy
token = db1.execute("SELECT vcatbridge_register_source('b1', 'memory')").fetchone()[0]
db1.execute("SELECT vcatbridge_set_context('b1', 'tenant', 42)")
for verb in ["select", "insert", "update"]:
    db1.execute(f"SELECT vcatbridge_set_policy('b1', 'main.users', '{verb}', 'tenant_id = $tenant')")

# 3. DB2: what you hand to someone else
db2 = duckdb.connect("vcatbridge:", config=CONFIG)
db2.execute("SELECT vcatbridge_init('b1', $1)", [token])

run(db2, "SELECT * FROM users")
run(db2, "INSERT INTO users VALUES (5, 7, 'mallory')")
run(db2, "DELETE FROM users WHERE id = 2")
run(db2, "FROM secrets")

# 4. Serve DB2 over Quack
db2.execute("""
    SET allow_persistent_secrets = false;
    INSTALL quack;  LOAD quack;
    INSTALL httpfs; LOAD httpfs;
""")
db2.execute("CALL quack_serve('quack:localhost:9494', token = 'super_secret')")

# 5. Lock DB2 down
db2.execute("SELECT vcatbridge_lockdown('1GB', 2, ['quack_serve'])")

# 6. A client, as from any DuckDB
client = duckdb.connect()
client.execute("""
    INSTALL quack; LOAD quack;
    CREATE SECRET (TYPE quack, TOKEN 'super_secret');
    ATTACH 'quack:localhost:9494' AS app;
""")

run(client, "SELECT * FROM app.users")
run(client, "FROM quack_query('quack:localhost:9494', 'SELECT * FROM read_csv(''/etc/passwd'')')")

db2.execute("CALL quack_stop('quack:localhost:9494')")
