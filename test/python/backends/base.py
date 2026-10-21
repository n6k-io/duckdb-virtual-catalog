import pathlib
import uuid
from dataclasses import dataclass

import duckdb
import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
SEEDS = pathlib.Path(__file__).resolve().parent / "seeds"
BUILT_EXTENSIONS = REPO_ROOT / "build/release/extension"
BRIDGE_EXTENSION = BUILT_EXTENSIONS / "virtual_catalog_bridge/virtual_catalog_bridge.duckdb_extension"
PROVIDER_EXTENSION = BUILT_EXTENSIONS / "virtual_catalog_provider/virtual_catalog_provider.duckdb_extension"


def new_connection(*, bridge=True, provider=False):
    """A connection with its own DatabaseInstance and the locally built extensions loaded.

    The two are separate binaries; load only what the test needs, or both where it
    exercises them side by side in separate catalogs.
    """
    wanted = ([BRIDGE_EXTENSION] if bridge else []) + ([PROVIDER_EXTENSION] if provider else [])
    for extension in wanted:
        if not extension.exists():
            pytest.skip(f"extension not built: {extension} -- run `make release`")
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    for extension in wanted:
        con.execute(f"LOAD '{extension}'")
    return con


def unique_id(prefix="b"):
    """Bridge ids and provider probes are process-wide, and pytest reuses one process."""
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def load(con, extension):
    try:
        con.execute(f"INSTALL {extension}; LOAD {extension}")
    except duckdb.Error as error:
        pytest.skip(f"{extension} extension unavailable: {error}")


@dataclass
class Source:
    """A source connection whose default catalog and schema are the attached upstream, so bare
    table names resolve there."""

    con: duckdb.DuckDBPyConnection
    catalog: str
    schema: str

    def target(self, table, catalog="app"):
        """The bridge serves source schemas under their own names."""
        return f"{catalog}.{self.schema}.{table}"


class Backend:
    """An upstream seeded natively from `seeds/<name>.sql`, then attached through its scanner (native:
    seeded in place, no scanner)."""

    name: str

    #: nodeid substring -> reason. Applied as strict xfail, so a gap that closes fails the run.
    gaps: dict[str, str] = {}

    #: Statements a target may try in order to reach the upstream around the bridge, by naming the
    #: source's own catalog. Formatted with `users` (the bridged table), `catalog` and `schema`.
    scanner_calls: list[str] = []

    #: Target-side reads of upstream objects no grant names. Formatted with `schema`.
    upstream_reads: list[str] = []

    @property
    def seed(self):
        return (SEEDS / f"{self.name}.sql").read_text()

    def unavailable(self):
        """A skip reason, or None."""
        return None

    def attach(self, con, location):
        """Seed the upstream, attach it to `con`, and return its (catalog, schema)."""
        raise NotImplementedError

    def open(self, location):
        con = new_connection()
        try:
            catalog, schema = self.attach(con, location)
            con.execute(f"USE {catalog}.{schema}")
        except BaseException:
            con.close()
            raise
        return Source(con, catalog, schema)

    def close(self, source):
        source.con.close()
