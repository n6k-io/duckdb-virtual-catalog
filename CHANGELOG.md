# 2026 Oct, 12

## virtual_catalog_bridge

- Bridges chain: a source catalog may itself be a `virtual_catalog_bridge`. Each hop applies its own policies and context; reads, writes, DDL and rollback pass through every hop.
- A native primary key is discovered through every hop. A key declared with `bridge_primary_key` covers one hop; declare it again on the next.
- Reads stream through every hop in autocommit. Inside an explicit transaction every hop, a single bridge included, holds its whole result in memory.
- The source catalog is pinned at registration by identity, not by name. A rename (`ALTER DATABASE ... SET ALIAS TO ...`) is followed; after a `DETACH` of the source every read, write, DDL or grant fails with `source catalog '...' was detached`, even if another database is attached under the same name.
- Registered sources cannot form a cycle. A source view, macro, `bridge_primary_key_query` or `bridge_primary_key_check` that names a bridge is not covered.
- `temp` and `system` are refused as a source catalog.
- A bridge whose source is on its own instance no longer keeps that instance alive; closing it is enough. Across instances `DETACH` is still required.
- Fixed: `UPDATE ... FROM` rows held on the target could write wrong values, update the wrong row, or update nothing.
- Fixed: a join the source runs, read by an expression only the target can compute, failed with `Failed to bind column reference` and invalidated the database.
- Fixed: reads never streamed; every result was materialized on the source first.

# 2026 Oct, 10

## virtual_catalog_bridge

- A grant can name a source view as well as a table. A view can only be granted `select`; `bridge_policy` refuses any other verb.

## virtual_catalog_provider

- A provider is scoped to the database it is attached in. The same catalog name in two databases no longer collides.
- Fixed: closing a database with a provider registered no longer keeps it open, so its file can be reopened.
