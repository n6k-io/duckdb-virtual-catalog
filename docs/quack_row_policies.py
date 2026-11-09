import duckdb

REPO = "https://storage.googleapis.com/n6k-duckdb-release"
CONFIG = {"allow_unsigned_extensions": "true"}


def run(con, sql, *params):
    print(f"> {sql.strip()}")
    try:
        print(con.execute(sql, list(params)).fetchall())
    except duckdb.Error as e:
        print(f"{type(e).__name__}: {e}")


# 1. SourceDB: the data
source_db = duckdb.connect(config=CONFIG)
source_db.execute(f"INSTALL vcatbridge FROM '{REPO}'")
source_db.execute("LOAD vcatbridge")
source_db.execute("""
    CREATE TABLE users (id INTEGER PRIMARY KEY, tenant_id INTEGER, name VARCHAR);
    INSERT INTO users VALUES (1, 42, 'ada'), (2, 42, 'grace'), (3, 7, 'linus');
    CREATE TABLE orders (user_id INTEGER, tenant_id INTEGER, amount DECIMAL(10, 2));
    INSERT INTO orders VALUES (1, 42, 10), (2, 42, 20), (3, 7, 30);
    CREATE TABLE secrets (k VARCHAR, v VARCHAR);
""")

# 2. SourceDB: the bridge and its policy
token = source_db.execute("SELECT vcatbridge_register_source('b1', 'memory')").fetchone()[0]
source_db.execute("SELECT vcatbridge_set_context('b1', 'tenant', 42)")
for verb in ["select", "insert", "update"]:
    source_db.execute(f"SELECT vcatbridge_set_policy('b1', 'main.users', '{verb}', 'tenant_id = $tenant')")
source_db.execute("SELECT vcatbridge_set_policy('b1', 'main.orders', 'select', 'tenant_id = $tenant')")

# 3. SharedDB: what you hand to someone else
shared_db = duckdb.connect("vcatbridge:", config=CONFIG)
shared_db.execute("SELECT vcatbridge_init('b1', $1)", [token])

run(shared_db, "SELECT * FROM users")
run(shared_db, "INSERT INTO users VALUES (5, 7, 'mallory')")
run(shared_db, "DELETE FROM users WHERE id = 2")
run(shared_db, "FROM secrets")
run(shared_db, "SELECT name, sum(amount) FROM users JOIN orders ON id = user_id GROUP BY name")
print(shared_db.execute("EXPLAIN SELECT name, sum(amount) FROM users JOIN orders ON id = user_id GROUP BY name").fetchone()[1])

# 4. Serve SharedDB over Quack
shared_db.execute("""
    SET allow_persistent_secrets = false;
    INSTALL quack;  LOAD quack;
    INSTALL httpfs; LOAD httpfs;
""")
shared_db.execute("CALL quack_serve('quack:localhost:9494', token = 'super_secret')")

# 5. Lock SharedDB down
shared_db.execute("SELECT vcatbridge_lockdown('1GB', 2, ['quack_serve'])")

# 6. A client, as from any DuckDB
client = duckdb.connect()
client.execute("""
    INSTALL quack; LOAD quack;
    CREATE SECRET (TYPE quack, TOKEN 'super_secret');
    ATTACH 'quack:localhost:9494' AS app;
""")

run(client, "SELECT * FROM app.users")
run(client, "FROM quack_query('quack:localhost:9494', 'SELECT * FROM read_csv(''/etc/passwd'')')")

shared_db.execute("CALL quack_stop('quack:localhost:9494')")
