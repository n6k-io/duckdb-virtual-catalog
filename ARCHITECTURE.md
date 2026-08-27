# Architecture

## Extension layout

The repo builds one DuckDB extension:

- **virtual_catalog** — in-process extension. Bridges two DuckDB `DatabaseInstance` objects directly, no network involved. The source declares per-table permissions; the target gets restricted access. Also hosts providers and stream functions.

Arrow IPC is the data transport format for provider tables and stream functions.

## Setup Handshake

Three-step token-based flow between two DuckDB connections:

1. **Source** calls `vcat_register_source(bridge_id, source_catalog, source_schema, permissions, pk_overrides)` — validates tables exist on the source, discovers primary keys for writable tables, and returns a one-time nonce token. The pending registration expires after 30 seconds.

2. **Target** calls `vcat_setup_bridge(bridge_id, token, catalog_name, target_schema)` — validates the nonce (which detects mismatched extension binaries), consumes the pending source, registers the bridge globally, and injects a bridge schema into the target's catalog.

3. **Target** runs `ATTACH ':memory:' AS name (TYPE virtual_catalog)` — the storage extension resolves the bridge from the global registry.

Teardown is a fourth step, and not optional in a long-lived process: **Target** calls `vcat_unregister_bridge(catalog_name, target_schema)` — unbinds the bridge from the schema, then drops the registry entry. That entry holds a `shared_ptr<DatabaseInstance>` to the *source*, so until it runs, `DETACH` on either side frees nothing and the bridge id stays taken. It mirrors `vcat_unregister_provider` in both argument shape and ordering.

## Global State

- **Bridge Registry** — maps bridge IDs to bridge metadata. A `SharedRegistry<BridgeInfo>` (`src/virtual_catalog/include/shared_registry.hpp`), the one registry type shared with the provider registry; Meyer's singleton pattern ensures safe static initialization.
- **Provider Registry** — maps `catalog::schema` to the UDF set backing it. Same `SharedRegistry`, differing only in that re-registration replaces (a host swapping its UDFs) where a duplicate bridge id is refused.
- **Pending Sources** — temporary registrations awaiting target consumption, mutex-protected. Entries are purged after a 30-second TTL. A refused `vcat_setup_bridge` leaves the pending source intact, so the token can be reused once the id frees up.
- **Load Nonce** — a per-binary-load UUID that prevents token reuse across different extension installations.

## Catalog Architecture

The extension implements DuckDB's catalog API (Catalog → Schema → Table). The bridge injects a schema entry into an existing DuckDB catalog. This schema creates table entries for each permitted source table, each providing a scan function.

For writes, a phantom catalog intercepts DuckDB's plan-time hooks (`PlanInsert`, `PlanDelete`, `PlanUpdate`) to return custom physical operators that execute DML against the source connection.

## Reads

The bridge scan opens a connection to the source database, builds a SQL query with pushed-down column selection and WHERE filters, executes it, and streams results back as Arrow IPC. Filter pushdown translates DuckDB's filter set into SQL predicates. Projection pushdown selects only requested columns.

## Writes

**INSERT**: Data chunks are accumulated during the sink phase, then flushed to the source via DuckDB's Appender API on finalize.

**DELETE**: During a writable scan, primary key values are buffered in a shared PK buffer. The delete operator collects buffer indices during sink, then executes batched `DELETE ... WHERE pk IN (...)` (single-column PK) or per-row prepared statements (composite PK), all wrapped in a transaction.

**UPDATE**: Similar to delete — chunks with updated column values and PK buffer indices are accumulated, then `UPDATE ... SET col=$1 WHERE pk=$2` is executed per row via prepared statements, wrapped in a transaction.

Write permission is validated at plan time and re-checked at finalize (defense-in-depth). A shared helper handles the common DML setup: permission verification, source availability check, connection creation, and PK column lookup.

## PK Buffer

A shared buffer sits between scan and write operators. During a writable scan, PK column values are appended for each row. DELETE/UPDATE operators reference these by buffer index to construct their WHERE clauses. This avoids re-scanning the source to identify affected rows.

## Thread Safety

- Meyer's singletons for global registries ensure safe static initialization
- Each bridge has its own mutex protecting permission reads during concurrent operations
- All locking uses RAII `lock_guard` — no manual lock/unlock
- Schema and table maps in catalog entries have their own locks

## Build System

`CMakeLists.txt` at the repo root lists every source; `extension_config.cmake`
declares the single `virtual_catalog` extension. nanoarrow is vendored under
`third_party/nanoarrow/` and compiled straight into the extension.

CI builds across 8 architectures: Linux (amd64, arm64, musl), macOS (amd64,
arm64), Windows (amd64, mingw), and WASM (threads). wasm_mvp/wasm_eh are excluded —
only the shared-memory (coi) wasm variant is shipped.

## Dependencies

| Dependency | Purpose |
|------------|---------|
| DuckDB | Core database engine and extension APIs |
| yyjson | JSON serialization (DuckDB-internal, linked not vendored) |
| nanoarrow | Arrow IPC serialization/deserialization |
