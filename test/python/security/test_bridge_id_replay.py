"""ATTACK  an outside connection learns a bridge id and replays it
BREACH  the canary value comes back
PASS    there is nothing to lift, and nothing that would accept it

A bridged table used to be a view whose SQL named the bridge id in cleartext, wrapping a table
function that took that id from any caller. Reading the view told you how to read the source
directly, from a connection that never registered, never redeemed a token and never attached. Both
halves are gone: the entry is a catalog entry, and the function does not exist.
"""

from conftest import new_connection

CANARY = "attacker_should_not_see_this"


def test_no_view_exposes_the_bridge_id(bridged):
    source, target, handshake = bridged
    source.execute(
        f"CREATE TABLE customers(id INTEGER PRIMARY KEY, note VARCHAR);"
        f"INSERT INTO customers VALUES (1, '{CANARY}');"
    )
    handshake(source, target, {"customers": {"select": "true"}})

    # The bridged table is reachable...
    assert target.execute("SELECT note FROM app.main.customers").fetchall() == [(CANARY,)]

    # ...but not as a view, so no generated SQL carries the id.
    views = target.execute("SELECT sql FROM duckdb_views() WHERE view_name = 'customers'").fetchall()
    assert views == [], f"a bridged table is still a view, and its SQL may name the bridge id -> {views}"


def test_a_stolen_bridge_id_buys_nothing(bridged):
    source, target, handshake = bridged
    source.execute(
        f"CREATE TABLE customers(id INTEGER PRIMARY KEY, note VARCHAR);"
        f"INSERT INTO customers VALUES (1, '{CANARY}');"
    )
    # Hand the attacker the id outright -- the strongest version of the attack.
    stolen_id = handshake(source, target, {"customers": {"select": "true"}})

    attacker = new_connection()
    try:
        rows = attacker.execute(f"SELECT * FROM vcat_scan('{stolen_id}', 'customers')").fetchall()
    except Exception:  # noqa: BLE001 -- a refusal is the pass
        return
    finally:
        attacker.close()

    raise AssertionError(f"a connection outside the bridge read the source -> {rows}")
