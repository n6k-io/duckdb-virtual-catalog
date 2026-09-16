# Build & Test

The USER runs builds — never run them yourself.

- User runs **`make release`** builds the extension into
  `build/release/extension/virtual_catalog/virtual_catalog.duckdb_extension`.
- You may run **`make test`** runs the sqllogictests under `test/sql/`. (needs make release)
- You may run **`make test-cpp`** run the c++ unit tests (does not need make release)
- **`make check-no-sql`** runs `scripts/check_no_sql.py`, which forbids SQL text
  construction in the bridge path. It is a required CI job.
- **`uv run pytest test/python/`** runs the Python-side tests

## Submodules
- `src/crossing` is the `duckdb-crossing` repo. Edit and commit crossing changes there, then bump the pointer here.

## Shell rules
- Always run `cd` in its own Bash command, never chained with `&&`.
- Bash cwd persists between calls — track where you are before running the next command.
- Dont filter outputs

## Formatting & Tidy
```
source .venv/bin/activate && make format-fix
source .venv/bin/activate && make tidy-check
```
Runs `clang-format` and `clang-tidy` against `src/ test/`. Both need `black` and the
clang tools on PATH.

