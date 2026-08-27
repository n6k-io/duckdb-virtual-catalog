# Testing this extension

Two harnesses, split by what each can reach.

## `test/sql` — sqllogictest

```bash
make test
```

Everything expressible on a single `DatabaseInstance` with no host language: the published
introspection contract, operation routing inside a `virtual_catalog` schema, and the bridge
handshake's validation and registry lifecycle.

Providers appear here only through SQL macros standing in for the host UDFs, which is enough to
reach the capability bits but not the Arrow payloads.

## `test/python` — pytest, via `uv`

```bash
uv run pytest
```

Needs `make release` first; the tests load
`build/release/extension/virtual_catalog/virtual_catalog.duckdb_extension` into the `duckdb` PyPI
package, whose version must match the pinned DuckDB (`duckdb==1.5.4`).

Covers what one instance and one language cannot:

| file | reaches |
|---|---|
| `test_bridge_read.py` | filter and projection pushdown across two `DatabaseInstance`s, checked against the source's own answer |
| `test_bridge_write.py` | INSERT/UPDATE/DELETE landing on the source, rowid path, composite and overridden keys |
| `test_provider.py` | the Arrow IPC round trip, provider DML, ALTER, cache invalidation |
| `test_stream_functions.py` | the open/next/close generator protocol actually executed |
| `test_lifetime.py` | source-instance pinning, registry churn, and clean process exit (subprocess) |
| `test_parity.py` | every probe in `parity.py` run against native, bridge and provider |

## Parity

`parity.py` holds one list of ~67 SQL probes and runs each against a native DuckDB table, a bridged
table and a provider table over identical data. Native is the oracle: a probe reaches `parity` when
the rows *and* the resulting table state match.

Every divergence is recorded in `LEDGER` with a reason, and the suite fails in both directions — a
probe that regresses, and a probe listed as a known gap that starts working. The second is the
point: it is how a gap closing gets noticed rather than quietly drifting out of the docs.

```bash
uv run python test/python/parity_report.py            # the matrix
uv run python test/python/parity_report.py --ledger   # regenerate the LEDGER literal
```

`provider_stub.py` is a host-side provider written against the real contract: it answers with
exactly the columns it was asked for, in order, and applies exactly the filters it was handed.
That last part is deliberate — `filter_json.hpp` notes that DuckDB does not re-apply filters
pushed into an Arrow scan, so a provider that ignores them silently returns wrong rows.

### Two things that bite every Python host

- Register `open` and `next` with `null_handling="special"`. `open` is always called with a NULL
  fourth argument, and `next` returns NULL to signal exhaustion; under DuckDB's default NULL
  handling neither works.
- Fetch the result of the setup calls. `con.execute("SELECT vcat_create_stream_function(...)")`
  without a `.fetchall()` never runs the function, so the registration silently does not happen.
