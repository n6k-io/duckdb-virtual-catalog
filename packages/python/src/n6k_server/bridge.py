"""Bridge setup between DuckDB connections with permission-based access control."""

import logging
import threading
import uuid
from typing import Optional

import duckdb

from n6k_server.extension import load_virtual_catalog_bridge

log = logging.getLogger(__name__)

PERMISSION_VERBS = {
    "read": ("select",),
    "readwrite": ("select", "insert", "update", "delete"),
}


def bridge(
    source: duckdb.DuckDBPyConnection,
    target: duckdb.DuckDBPyConnection,
    name: str,
    *,
    source_catalog: str,
    permissions: dict[str, str],
    primary_keys: Optional[dict[str, tuple[str, ...]]] = None,
    lock: Optional[threading.Lock] = None,
) -> str:
    """Mirror tables from `source` into a new catalog `name` on `target`.

    One bridge is one attached catalog: `name` must not exist on `target` yet, and
    `DETACH name` (see `unbridge`) tears it down. Each granted source schema lands
    in the target schema of the same name.

    ## Args

    - `source`, `target` — DuckDB connections.
    - `name` — catalog created on the target.
    - `source_catalog` — where the source tables live.
    - `permissions` — `{'schema.table': 'read' | 'readwrite'}`.
    - `primary_keys` — `{'schema.table': (col, ...)}`; overrides PK auto-discovery.
    - `lock` — serialises source access if given.

    Returns the bridge id. Raises `duckdb.Error` for an empty, invalid, or
    unqualified `permissions` key.

    ## Example

    ```python
    import duckdb
    from n6k_server.bridge import bridge

    cfg = {"allow_unsigned_extensions": "true"}
    source = duckdb.connect(config=cfg)
    source.sql("CREATE TABLE users(id INTEGER PRIMARY KEY, name VARCHAR)")

    target = duckdb.connect(config=cfg)
    bridge(source, target, "app", source_catalog="memory", permissions={"main.users": "readwrite"})

    target.sql("SELECT * FROM app.main.users")
    ```
    """
    if not permissions:
        raise duckdb.InvalidInputException("bridge: permissions cannot be empty")
    for table, permission in permissions.items():
        if permission not in PERMISSION_VERBS:
            raise duckdb.InvalidInputException(
                f"bridge: invalid permission {permission!r} for {table!r}; expected 'read' or 'readwrite'"
            )
        if "." not in table:
            raise duckdb.InvalidInputException(f"bridge: permissions key {table!r} must be 'schema.table'")

    bridge_id = uuid.uuid4().hex

    load_virtual_catalog_bridge(source)
    load_virtual_catalog_bridge(target)

    if lock is not None:
        lock.acquire()
    try:
        token_result = source.sql(
            "SELECT bridge_register_source($1, $2)",
            params=[bridge_id, source_catalog],
        ).fetchone()
        if token_result is None:
            raise RuntimeError("bridge_register_source returned no result")
        token = token_result[0]
        # Keys first: discovery runs on the first grant, and a keyless table's update/delete
        # grant is refused before a later override could rescue it.
        for table, columns in (primary_keys or {}).items():
            source.sql(
                "SELECT bridge_primary_key($1, $2, $3)",
                params=[bridge_id, table, list(columns)],
            ).fetchone()
        for table, permission in permissions.items():
            for verb in PERMISSION_VERBS[permission]:
                if verb == "insert":
                    source.sql(
                        "SELECT bridge_policy($1, $2, $3, 'true', 'true')",
                        params=[bridge_id, table, verb],
                    ).fetchone()
                else:
                    source.sql(
                        "SELECT bridge_policy($1, $2, $3, 'true')",
                        params=[bridge_id, table, verb],
                    ).fetchone()
    finally:
        if lock is not None:
            lock.release()

    # ATTACH options fold to constants at bind time, so neither can be a parameter.
    quoted = name.replace('"', '""')
    target.execute(
        f"ATTACH '' AS \"{quoted}\" (TYPE virtual_catalog_bridge, ID '{bridge_id}', TOKEN '{token}')",
    )
    log.debug("bridge(%s): attached as %s", name, bridge_id)
    return bridge_id


def unbridge(target: duckdb.DuckDBPyConnection, name: str) -> None:
    quoted = name.replace('"', '""')
    target.execute(f'DETACH "{quoted}"')
