# Build & Test

The USER runs builds — never run them yourself.

- **`make release`** builds the extension into
  `build/release/extension/virtual_catalog/virtual_catalog.duckdb_extension`.
- **`make test`** runs the sqllogictests under `test/sql/`.

Submodules (`duckdb`, `extension-ci-tools`) must be checked out before either works:
`git submodule update --init --recursive`.

## Shell rules
- Always run `cd` in its own Bash command, never chained with `&&`.
- Bash cwd persists between calls — track where you are before running the next command.

## Formatting & Tidy
```
make format-fix
make tidy-check
```
Runs `clang-format` and `clang-tidy` against `src/ test/`. Both need `black` and the
clang tools on PATH.

## Source layout

One extension, one source directory. `src/virtual_catalog/*.cpp` with headers in
`src/virtual_catalog/include/`; the root `CMakeLists.txt` lists every source and
`extension_config.cmake` declares the single extension. nanoarrow is vendored in
`third_party/nanoarrow/` and compiled in directly.

## SQL contract

The published surface is SQL, not C++. Consumers (notably the n6k repo) probe
`duckdb_functions()` for these and fall back to their own defaults when absent:

- `vcat_table_permissions(catalog, schema := …, "table" := …)` →
  `(schema, name, kind, writeable, editable, primary_key)`
- `vcat_table_describe(catalog, schema := …, "table" := …)` → one row describing that table
- `vcat_stream_functions()` →
  `(catalog, schema, entry_name, function_name, open_udf, next_udf, close_udf)`

Renaming or reshaping either breaks downstream consumers. Adding a `kind` value
does not — it is an open string set.

## DuckDB version

Pinned to v1.5.4 in `.github/workflows/MainDistributionPipeline.yml` and the `duckdb`
submodule; the two must agree.
