"""Print the parity matrix, and the LEDGER literal that pins it.

    uv run python test/python/parity_report.py            # markdown matrix
    uv run python test/python/parity_report.py --ledger   # paste-able LEDGER for parity.py
"""

import sys

from parity import BACKENDS, PROBES, NativeBackend, classify, run_probe

SYMBOL = {"parity": "✅", "unsupported": "❌", "differs": "⚠️", "accepts-invalid": "💥"}


def collect():
    results = {}
    for probe in PROBES:
        native_backend = NativeBackend()
        try:
            native = run_probe(native_backend, probe)
        finally:
            native_backend.close()
        for name, factory in BACKENDS.items():
            backend = factory()
            try:
                other = run_probe(backend, probe)
            finally:
                backend.close()
            results[(probe.name, name)] = (classify(native, other), native, other)
    return results


def main():
    results = collect()
    if "--ledger" in sys.argv:
        print("LEDGER: dict[tuple[str, str], tuple[str, str]] = {")
        for (probe, backend), (status, _native, other) in results.items():
            if status == "parity":
                continue
            reason = other.error if status == "unsupported" else "result differs from native"
            reason = reason.replace('"', "'")
            print(f'    ("{probe}", "{backend}"): ("{status}", "{reason}"),')
        print("}")
        return

    print("| probe | category | bridge | provider |")
    print("|---|---|---|---|")
    for probe in PROBES:
        cells = []
        for backend in BACKENDS:
            status = results[(probe.name, backend)][0]
            cells.append(f"{SYMBOL[status]} {status}")
        print(f"| `{probe.name}` | {probe.category} | {cells[0]} | {cells[1]} |")

    print()
    totals = {}
    for (_, backend), (status, _n, _o) in results.items():
        totals.setdefault(backend, {}).setdefault(status, 0)
        totals[backend][status] += 1
    for backend, counts in totals.items():
        print(f"{backend}: " + ", ".join(f"{k} {v}" for k, v in sorted(counts.items())))


if __name__ == "__main__":
    main()
