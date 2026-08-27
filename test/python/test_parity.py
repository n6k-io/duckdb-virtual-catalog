"""Parity with a native DuckDB table, probe by probe.

Fails in both directions. A probe that regresses fails because it no longer matches native; a probe
listed in LEDGER that starts working fails too, so a gap closing gets noticed and the ledger (and
the docs it backs) stay honest.
"""

import pytest

from parity import BACKENDS, LEDGER, PROBES, NativeBackend, classify, run_probe


@pytest.fixture(scope="module")
def native_outcomes():
    outcomes = {}
    for probe in PROBES:
        backend = NativeBackend()
        try:
            outcomes[probe.name] = run_probe(backend, probe)
        finally:
            backend.close()
    return outcomes


@pytest.mark.parametrize("backend_name", sorted(BACKENDS))
@pytest.mark.parametrize("probe", PROBES, ids=lambda p: p.name)
def test_parity_with_a_native_table(probe, backend_name, native_outcomes):
    native = native_outcomes[probe.name]
    backend = BACKENDS[backend_name]()
    try:
        other = run_probe(backend, probe)
    finally:
        backend.close()

    actual = classify(native, other)
    expected, reason = LEDGER.get((probe.name, backend_name), ("parity", ""))

    if actual == expected:
        return

    if expected == "parity":
        pytest.fail(
            f"{probe.name} on {backend_name}: expected parity with a native table, got {actual}.\n"
            f"  native : {native}\n"
            f"  {backend_name:<7}: {other}\n"
            f"If this divergence is intended, add it to LEDGER in parity.py with a reason."
        )

    pytest.fail(
        f"{probe.name} on {backend_name}: LEDGER says {expected} ({reason}), but it is now {actual}.\n"
        f"  native : {native}\n"
        f"  {backend_name:<7}: {other}\n"
        f"If the gap closed, remove the LEDGER entry and update the docs."
    )


def test_ledger_has_no_stale_entries():
    known = {p.name for p in PROBES}
    unknown = sorted(name for name, _ in LEDGER if name not in known)
    assert not unknown, f"LEDGER references probes that no longer exist: {unknown}"


def test_every_ledger_entry_has_a_reason():
    missing = sorted(key for key, (_, reason) in LEDGER.items() if not reason)
    assert not missing, f"LEDGER entries without a reason: {missing}"
