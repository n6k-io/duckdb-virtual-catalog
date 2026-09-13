"""Throughput and memory baseline for the bridge read path.

Not a pytest test -- run it directly:  uv run python test/python/bench_bridge.py

The bridge's cost model is invisible to the correctness suite, so this exists to make the
before/after of a transport change measurable rather than argued about.

Two things are being separated, and neither is visible at a single table size: the fixed cost of
opening a scan (a fresh source Connection, a full bind/plan of the pushdown relation, the streaming
buffer) and the per-row cost of moving chunks. Hence the row sweep and the per-probe linear fit --
the intercept is the first, the slope is the second, and a ratio at one row count blends them into a
number you cannot act on.

The summary is therefore ordered by absolute overhead, not by bridge/native. A ratio over a native
baseline of a few hundred microseconds says more about how fast DuckDB is on that shape than about
the bridge, and it puts the cheapest probes at the top of the table.

Threads are a second, separate axis rather than a cross with the row sweep: crossing them would
multiply the runtime by the length of the ladder, and thread scaling is only a question at a size
that has work to divide. The ladder therefore runs at one fixed row count, sets threads on the
*target* only -- the source instance is held fixed, so what is measured is whether the target can
parallelise work above the crossing scan -- and fits serial + parallel/N per side. `par_keep` is the
bridge's parallel fraction over native's: 1.0 keeps all of native's scaling, 0.0 none of it.

Two caveats on that number. At target threads=1 the bridge still has its pump std::thread outside
DuckDB's pool, so it is really 1+1 and its parallel fraction reads slightly low. And the ladder skips
the memory pass, which would otherwise double its cost for a figure the row sweep already reports.

Memory comes from duckdb_memory() sampled on a second cursor while the probe runs, taken separately
on the source and target instances. That is the only figure that attributes a byte to a side; RSS
cannot, and ru_maxrss additionally only ever rises, so it appears here as a per-side high-water note
rather than as a per-probe measurement. Each side runs in its own subprocess so neither floors the
other.
"""

import argparse
import csv
import json
import math
import pathlib
import random
import resource
import subprocess
import sys
import threading
import time

import numpy as np
from scipy import optimize, stats

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

SIDES = ("native", "bridge")

#: Bounds on the row counts drawn each iteration. The low end is 1 so that setup_ms -- the cost of
#: opening a scan, which is what the intercept is -- is measured near zero rows rather than
#: extrapolated back from a size that already carries real per-row cost.
ROW_RANGE = (1, 4_000_000)

#: Always run, on top of the drawn points. The two ends carry most of the slope's leverage
#: (its variance goes as 1/Sxx) and the midpoint gives the linearity check something to fail on.
ANCHOR_ROWS = (1, 2_000_000, 4_000_000)

#: Drawn points, fixed before the sweep starts rather than decided by watching it. Stopping when
#: an interval happens to look narrow is optional stopping: it ends the run preferentially on
#: draws where noise shrank the interval, so the reported CI comes out too narrow and the estimate
#: biased. A sample size chosen in advance is what makes the 95% mean 95%. Slope error falls as
#: 1/sqrt(N), so quadrupling POINTS halves the intervals.
POINTS = 24

#: Below this the fitted line does not describe the points, so the two terms are not a model.
FIT_R2_FLOOR = 0.95

#: Below this, the difference between the two sides is timer jitter and scheduling, not transport.
NOISE_MS = 2.0

MIN_POINTS = 3

THREAD_LADDER = (1, 2, 3, 4, 6, 8)

THREAD_ROWS = 4_000_000

#: Correctness is checked once, small, sorted -- not inside the timing loop, where comparing two
#: fully materialised multi-million-row results costs more than the probe and is order-dependent.
VERIFY_ROWS = 5_000

SETUP = """
CREATE TABLE wide AS
SELECT
    i::INTEGER                          AS id,
    'user_' || (i % 5000)               AS name,
    'city_' || (i % 97)                 AS city,
    ((i * 7) % 1000)::INTEGER           AS score,
    (i % 1000) / 7.0                    AS ratio,
    DATE '2020-01-01' + ((i % 2000)::INTEGER) AS joined,
    'note-' || i || '-' || repeat('x', 40) AS note
FROM range({rows}) t(i);
"""

#: `drain` consumes the result in bounded batches instead of materialising it: the probe then
#: measures the transport rather than the cost of building a few million Python tuples.
PROBES = [
    {"name": "agg_all_cols", "sql": "SELECT count(*), sum(score), max(name), max(note) FROM {t}"},
    {"name": "agg_one_col", "sql": "SELECT sum(score) FROM {t}"},
    {"name": "strings", "sql": "SELECT count(DISTINCT note) FROM {t}"},
    {"name": "filtered", "sql": "SELECT count(*) FROM {t} WHERE score > 500 AND city = 'city_42'"},
    {"name": "join", "sql": "SELECT count(*) FROM {t} a JOIN {t} b USING (id)"},
    {"name": "order_by", "sql": "SELECT id FROM {t} ORDER BY score, id LIMIT 100"},
    {"name": "limit_10", "sql": "SELECT * FROM {t} LIMIT 10"},
    {"name": "fetch_all", "sql": "SELECT * FROM {t}", "drain": True},
]

NATIVE_TABLE = "wide"
BRIDGE_TABLE = "app.main.wide"


def peak_rss_mb():
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return usage / (1024 * 1024) if sys.platform == "darwin" else usage / 1024


def buffer_manager_mb(cursor):
    total = cursor.execute("SELECT sum(memory_usage_bytes) FROM duckdb_memory()").fetchone()[0]
    return (total or 0) / (1024 * 1024)


class MemorySampler:
    """Peak buffer-manager use per instance across the life of one probe.

    duckdb_memory() is an instantaneous read, so the interesting value -- what the scan held mid
    flight -- is gone by the time the query returns and has to be polled for. Each cursor is a
    separate ClientContext on the same instance, so polling does not contend with the probe's own
    connection.
    """

    def __init__(self, cursors, interval=0.001):
        self.cursors = cursors
        self.interval = interval
        self.baseline = {name: buffer_manager_mb(cur) for name, cur in cursors.items()}
        self.peak = dict(self.baseline)
        self._stop = threading.Event()
        self._thread = None

    def _sample(self):
        for name, cur in self.cursors.items():
            self.peak[name] = max(self.peak[name], buffer_manager_mb(cur))

    def _loop(self):
        while not self._stop.is_set():
            self._sample()
            self._stop.wait(self.interval)

    def __enter__(self):
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join()
        self._sample()

    def delta(self, name):
        return self.peak[name] - self.baseline[name]


def run_sql(con, sql, drain=False):
    cursor = con.execute(sql)
    if drain:
        rows = 0
        while True:
            batch = cursor.fetchmany(8192)
            if not batch:
                return rows
            rows += len(batch)
    return cursor.fetchall()


def build_side(side, rows, threads=None):
    """Returns (probe connection, {instance name: sampling cursor})."""
    from conftest import READ, bridge, new_connection

    source = new_connection()
    source.execute(SETUP.format(rows=rows))
    if side == "native":
        if threads:
            source.execute(f"SET threads={threads}")
        return source, {"source": source.cursor()}

    target = new_connection()
    bridge(source, target, {"wide": READ})
    if threads:
        target.execute(f"SET threads={threads}")
    return target, {"source": source.cursor(), "target": target.cursor()}


def run_side(side, rows, wanted, threads=None):
    """Child process: build one side, run the named probes against it, print JSON on stdout.

    One timed run per probe. Repeating at a fixed row count would buy the fit nothing -- it is the
    same x -- and taking a min over repeats would bias the point low and hide the very scatter the
    confidence interval is computed from. The sweep spends that time on another row count instead.
    """
    con, cursors = build_side(side, rows, threads)
    table = NATIVE_TABLE if side == "native" else BRIDGE_TABLE

    results = {}
    for probe in PROBES:
        if probe["name"] not in wanted:
            continue
        sql = probe["sql"].format(t=table)
        drain = probe.get("drain", False)

        rss_before = peak_rss_mb()
        start = time.perf_counter()
        run_sql(con, sql, drain)
        elapsed_ms = (time.perf_counter() - start) * 1000.0

        if threads:
            memory = {name: 0.0 for name in cursors}
        else:
            # Memory on its own pass: the sampler issues a query per poll per instance, which would
            # otherwise land inside the number above.
            sampler = MemorySampler(cursors)
            with sampler:
                run_sql(con, sql, drain)
            memory = {name: sampler.delta(name) for name in cursors}

        results[probe["name"]] = {
            "ms": elapsed_ms,
            "rss_hw_delta_mb": peak_rss_mb() - rss_before,
            "mem_mb": memory,
        }

    print("RESULT" + json.dumps({"probes": results, "rss_hw_mb": peak_rss_mb()}))


def run_verify():
    """Child process: both sides at VERIFY_ROWS, sorted compare.

    A transport change that quietly drops rows would otherwise read as a speedup.
    """
    from conftest import READ, bridge, new_connection

    source = new_connection()
    target = new_connection()
    source.execute(SETUP.format(rows=VERIFY_ROWS))
    bridge(source, target, {"wide": READ})

    for probe in PROBES:
        # Materialised regardless of `drain`: the point here is the rows, not the timing.
        native = sorted(map(repr, run_sql(source, probe["sql"].format(t=NATIVE_TABLE))))
        bridged = sorted(map(repr, run_sql(target, probe["sql"].format(t=BRIDGE_TABLE))))
        if native != bridged:
            raise SystemExit(f"{probe['name']}: bridge result differs from source oracle")

    print("RESULTok")


def spawn(extra):
    proc = subprocess.run(
        [sys.executable, __file__, *extra],
        capture_output=True,
        text=True,
        cwd=pathlib.Path(__file__).resolve().parents[2],
    )
    line = next((ln for ln in proc.stdout.splitlines() if ln.startswith("RESULT")), None)
    if line is None:
        tail = proc.stderr.strip().splitlines()
        raise SystemExit(f"child failed ({' '.join(extra)}): {tail[-1] if tail else '?'}")
    payload = line[len("RESULT") :]
    return json.loads(payload) if payload != "ok" else "ok"


def _cost_model(mrows, fixed_ms, per_mrow_ms):
    return fixed_ms + per_mrow_ms * mrows


class Parameter:
    """One fitted term: a bounded estimate, plus inference taken from the unbounded fit.

    The estimate is bounded at zero because neither a per-scan nor a per-row cost can be negative,
    and unbounded least squares returns one whenever the term is buried in noise. Inference cannot
    come from that same fit: a parameter sitting on its bound has no normal sampling distribution,
    so its stderr and p-value would be fiction. Hence two fits -- `pinned` marks the parameters
    where they disagree, which is exactly the set whose true value is indistinguishable from zero.
    """

    def __init__(self, bounded, free, stderr, ci, p_value):
        self.value = bounded
        self.free = free
        self.stderr = stderr
        self.ci = ci
        self.p_value = p_value
        self.pinned = free < 0.0

    def render(self, width):
        if self.pinned:
            body = f"~0 (p={self.p_value:.2f})"
        elif math.isfinite(self.ci):
            body = f"{self.value:.2f} ± {self.ci:.2f}"
        else:
            body = f"{self.value:.2f} ± ?"
        return f"{body:>{width}}"


def _fit_linear(xs, ys):
    dof = len(xs) - 2

    free, cov = optimize.curve_fit(_cost_model, xs, ys, p0=(0.0, 1.0))
    bounded, _ = optimize.curve_fit(_cost_model, xs, ys, p0=(0.0, 1.0), bounds=([0.0, 0.0], [np.inf, np.inf]))

    if dof > 0 and np.all(np.isfinite(cov)):
        stderr = np.sqrt(np.diag(cov))
        with np.errstate(divide="ignore", invalid="ignore"):
            t_stat = np.where(stderr > 0, free / stderr, np.inf)
        ci = stats.t.ppf(0.975, dof) * stderr
        p_values = 2.0 * stats.t.sf(np.abs(t_stat), dof)
    else:
        stderr = ci = np.full(2, np.inf)
        p_values = np.full(2, 1.0)

    # Scored against the free fit: R^2 is defined for least squares, and a fit clamped at a bound
    # is not it -- residuals against the clamped line can exceed the mean line's and go negative.
    residual = ys - _cost_model(xs, *free)
    ss_tot = float(np.sum((ys - ys.mean()) ** 2))
    r2 = 1.0 - float(np.sum(residual**2)) / ss_tot if ss_tot else 1.0

    params = [Parameter(bounded[i], free[i], stderr[i], ci[i], p_values[i]) for i in (0, 1)]
    return params[0], params[1], r2


def fit_cost(points):
    """(rows, ms) pairs -> (fixed Parameter, per-Mrow Parameter, R^2).

    Two parameters, so three points is the minimum that leaves any degrees of freedom for a CI.
    """
    xs = np.array([r / 1e6 for r, _ in points], dtype=float)
    ys = np.array([ms for _, ms in points], dtype=float)
    return _fit_linear(xs, ys)


def fit_threads(points):
    """(threads, ms) pairs -> (serial Parameter, parallel Parameter, R^2).

    Amdahl is linear in 1/N, so the same two-parameter machinery fits it unchanged.
    """
    xs = np.array([1.0 / n for n, _ in points], dtype=float)
    ys = np.array([ms for _, ms in points], dtype=float)
    return _fit_linear(xs, ys)


def parallel_fraction(points):
    """Share of the fitted time that scales with threads, or None when the fit does not pin it down."""
    serial, parallel, _ = fit_threads(points)
    if parallel.pinned or parallel.value <= 0 or not math.isfinite(parallel.ci):
        return None
    total = serial.value + parallel.value
    return parallel.value / total if total > 0 else None


def par_keep(threads_collected, name):
    """How much of native's thread scaling the bridge keeps. None when native barely scales."""
    ladder = sorted(threads_collected)
    sides = {side: [(n, threads_collected[n][side]["probes"][name]["ms"]) for n in ladder] for side in SIDES}
    native = parallel_fraction(sides["native"])
    bridge = parallel_fraction(sides["bridge"])
    if native is None or bridge is None or native <= 0:
        return None
    return bridge / native


def probe_rows(collected, name):
    """Row counts this probe actually ran at. Probes leave the sweep as they converge."""
    return [r for r, sides in sorted(collected.items()) if name in sides["bridge"]["probes"]]


def side_points(collected, name, side):
    return [(r, collected[r][side]["probes"][name]["ms"]) for r in probe_rows(collected, name)]


def overhead_points(collected, name):
    """The bridge's cost with native already subtracted, per row count.

    Differencing before the fit rather than fitting each side and subtracting: the CI on a
    difference of two independent fits is not available from either of them.
    """
    return [
        (r, collected[r]["bridge"]["probes"][name]["ms"] - collected[r]["native"]["probes"][name]["ms"])
        for r in probe_rows(collected, name)
    ]


def slope_ratio(collected, name):
    """Per-row cost of the bridge against native, or None when native has no per-row cost to divide.

    The constant is already out of both sides, which is what makes this readable: the old
    bridge_ms/native_ms put `limit_10` at 38x purely because DuckDB answers it in 0.1ms, and no
    amount of transport work would ever have moved that number.
    """
    native = fit_cost(side_points(collected, name, "native"))[1]
    bridge = fit_cost(side_points(collected, name, "bridge"))[1]
    # The divisor has to be pinned down, not merely non-zero: `limit_10`'s native slope fits to a
    # hundredth of a ms/Mrow with a CI several times that, and dividing by it printed 105x.
    if native.pinned or native.value <= 0 or not math.isfinite(native.ci) or native.ci >= native.value:
        return None
    return bridge.value / native.value


def probe_state(collected, name):
    """Everything known about one probe. Every probe runs at every row count, so these compare."""
    fixed, per_mrow, r2 = fit_cost(overhead_points(collected, name))
    seen = probe_rows(collected, name)
    largest = max(seen)
    return {
        "name": name,
        "fixed": fixed,
        "per_mrow": per_mrow,
        "slower": slope_ratio(collected, name),
        "fit_r2": r2,
        "largest": largest,
        "src_mem_mb": max(collected[r]["bridge"]["probes"][name]["mem_mb"]["source"] for r in seen),
        "tgt_mem_mb": max(collected[r]["bridge"]["probes"][name]["mem_mb"]["target"] for r in seen),
        "native_ms": collected[largest]["native"]["probes"][name]["ms"],
        "bridge_ms": collected[largest]["bridge"]["probes"][name]["ms"],
    }


def summarise(collected):
    """One row per probe, ordered by per-row cost -- what dominates as the table grows."""
    states = [probe_state(collected, probe["name"]) for probe in PROBES]
    return sorted(states, key=lambda r: r["per_mrow"].value, reverse=True)


def report_summary(collected, low, high, threads_collected=None):
    header = f"{'probe':<14}{'setup_ms':>18}{'ms_per_Mrow':>17}{'slower':>10}"
    if threads_collected:
        header += f"{'par_keep':>11}"
    print(f"\nbridge overhead over native -- fitted over random row counts in {low:,}..{high:,}\n")
    print(header)
    print("-" * len(header))
    for row in summarise(collected):
        name = f"{row['name']}{'?' if row['fit_r2'] < FIT_R2_FLOOR else ''}"
        slower = f"{row['slower']:.2f}x" if row["slower"] is not None else "--"
        line = f"{name:<14}{row['fixed'].render(18)}{row['per_mrow'].render(17)}{slower:>10}"
        if threads_collected:
            keep = par_keep(threads_collected, row["name"])
            line += f"{keep:>11.2f}" if keep is not None else f"{'--':>11}"
        print(line)


def report_threads(threads_collected, rows):
    ladder = sorted(threads_collected)
    for probe in PROBES:
        name = probe["name"]
        print(f"\n{name}   @ {rows:,} rows")
        print(f"  {'threads':>7} {'native_ms':>11} {'bridge_ms':>11}")
        for threads in ladder:
            native = threads_collected[threads]["native"]["probes"][name]["ms"]
            bridge = threads_collected[threads]["bridge"]["probes"][name]["ms"]
            print(f"  {threads:>7} {native:>11.2f} {bridge:>11.2f}")
        for side in SIDES:
            points = [(n, threads_collected[n][side]["probes"][name]["ms"]) for n in ladder]
            serial, parallel, r2 = fit_threads(points)
            fraction = parallel_fraction(points)
            shown = f"{fraction:.3f}" if fraction is not None else "--"
            print(
                f"  fit {side:<7} serial {serial.render(14)}   parallel {parallel.render(14)}"
                f"   p {shown:>6}   R^2 {r2:>6.3f}"
            )
        keep = par_keep(threads_collected, name)
        print(f"  par_keep {keep:.2f}" if keep is not None else "  par_keep --")


def measure(rows, wanted, threads=None):
    extra = ["--threads", str(threads)] if threads else []
    return {side: spawn(["--side", side, "--rows", str(rows), "--probes", ",".join(wanted), *extra]) for side in SIDES}


def run_thread_ladder(rows, ladder):
    """Every probe at every thread count, at one fixed row count.

    Not crossed with the row sweep: that would multiply its runtime by the length of the ladder, and
    thread scaling is only a question where there is enough work to divide.
    """
    names = [probe["name"] for probe in PROBES]
    collected = {}
    for index, threads in enumerate(ladder, 1):
        collected[threads] = measure(rows, names, threads)
        print(f"  [{index:>2}/{len(ladder)}] {threads:>2} threads", flush=True)
    return collected


def plan_rows(low, high, points, rng):
    """The anchors plus `points` sqrt-uniform draws over [low, high], deduplicated and sorted.

    sqrt-uniform -- uniform in sqrt(rows), squared back. Neither obvious choice covers the range:
    uniform over 1..4M puts 75% of its draws above 1M and 0.2% below 10k, leaving the intercept
    extrapolated across two million rows of nothing; log-uniform inverts it, spending half the
    sweep under 2k rows where every timing is sub-millisecond noise. sqrt lands between, at 50%
    above 1M and 15% below 100k.
    """
    chosen = {r for r in ANCHOR_ROWS if low <= r <= high}
    root_low, root_high = math.sqrt(low), math.sqrt(high)
    drawn = set()
    while len(drawn) < points:
        rows = int(round(rng.uniform(root_low, root_high) ** 2))
        rows = min(max(rows, low), high)
        if rows not in chosen and rows not in drawn:
            drawn.add(rows)
    return sorted(chosen | drawn)


def fit_progress(collected):
    """Weakest R^2 across the probes so far, for the progress line.

    Watching this is safe only because the sweep does not act on it: the row counts were fixed
    before the first timing, so nothing seen mid-run can end it early and skew the intervals.
    """
    if len(collected) < 3:
        return "R^2 --"
    scored = [(fit_cost(overhead_points(collected, probe["name"]))[2], probe["name"]) for probe in PROBES]
    r2, name = min(scored)
    return f"R^2 {r2:>5.2f} worst ({name})"


def run_sweep(rows_plan):
    """Every probe at every row count. One fixed design, decided before any timing was taken."""
    names = [probe["name"] for probe in PROBES]
    collected = {}
    for index, rows in enumerate(rows_plan, 1):
        collected[rows] = measure(rows, names)
        print(f"  [{index:>2}/{len(rows_plan)}] {rows:>9,} rows   {fit_progress(collected)}", flush=True)
    return collected


def write_csv(path, collected):
    """Every timing, one row each, no derived numbers. The fit is reproducible from this alone."""
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(("rows", "probe", "side", "ms"))
        for rows in sorted(collected):
            for side in SIDES:
                for name, result in collected[rows][side]["probes"].items():
                    writer.writerow((rows, name, side, f"{result['ms']:.6f}"))


def report_detail(collected):
    for probe in PROBES:
        name = probe["name"]
        seen = probe_rows(collected, name)
        print(f"\n{name}")
        print(f"  {probe['sql'].format(t='<table>')}")
        print(
            f"  {'rows':>11} {'native_ms':>10} {'bridge_ms':>10} {'overhead':>9} "
            f"{'Mrow/s':>8} {'src_mem':>8} {'tgt_mem':>8} {'rss_hw':>7}"
        )
        for rows in seen:
            native = collected[rows]["native"]["probes"][name]
            bridged = collected[rows]["bridge"]["probes"][name]
            overhead = bridged["ms"] - native["ms"]
            throughput = rows / 1e6 / (bridged["ms"] / 1000.0)
            noise = " noise" if max(native["ms"], bridged["ms"]) < NOISE_MS else ""
            print(
                f"  {rows:>11,} {native['ms']:>10.2f} {bridged['ms']:>10.2f} {overhead:>+9.2f} "
                f"{throughput:>8.1f} {bridged['mem_mb']['source']:>8.1f} "
                f"{bridged['mem_mb']['target']:>8.1f} {bridged['rss_hw_delta_mb']:>7.0f}{noise}"
            )

        if len(seen) < MIN_POINTS:
            continue
        series = {side: side_points(collected, name, side) for side in SIDES}
        series["overhead"] = overhead_points(collected, name)
        for label, points in series.items():
            fixed, per_million, r2 = fit_cost(points)
            print(
                f"  fit {label:<9} {fixed.render(16)} ms + {per_million.render(16)} ms/Mrow"
                f"   R^2 {r2:>5.3f}   p {fixed.p_value:>5.3f} / {per_million.p_value:<5.3f}"
            )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--range",
        dest="row_range",
        type=lambda s: tuple(int(x) for x in s.split(",")),
        default=ROW_RANGE,
        help="low,high row counts to draw the sweep from (default: %(default)s)",
    )
    ap.add_argument(
        "--rows",
        type=lambda s: tuple(int(x) for x in s.split(",")),
        help="explicit comma-separated row counts; skips the draw",
    )
    ap.add_argument("--points", type=int, default=POINTS, help="row counts to draw (default: %(default)s)")
    ap.add_argument("--seed", type=int, help="fix the row-count draw so a run is reproducible")
    ap.add_argument("--json", type=pathlib.Path)
    ap.add_argument("--csv", type=pathlib.Path, help="every raw timing as rows,probe,side,ms")
    ap.add_argument("--skip-verify", action="store_true")
    ap.add_argument("--verbose", action="store_true", help="per-row-count detail under each probe")
    ap.add_argument("--skip-threads", action="store_true", help="skip the thread-scaling ladder")
    ap.add_argument(
        "--thread-ladder",
        type=lambda s: tuple(int(x) for x in s.split(",")),
        default=THREAD_LADDER,
        help="target thread counts to sweep (default: %(default)s)",
    )
    ap.add_argument(
        "--thread-rows",
        type=int,
        default=THREAD_ROWS,
        help="row count the thread ladder runs at (default: %(default)s)",
    )
    ap.add_argument("--side", choices=SIDES, help=argparse.SUPPRESS)
    ap.add_argument("--probes", type=lambda s: s.split(","), help=argparse.SUPPRESS)
    ap.add_argument("--verify", action="store_true", help=argparse.SUPPRESS)
    ap.add_argument("--threads", type=int, help=argparse.SUPPRESS)
    args = ap.parse_args()

    if args.verify:
        run_verify()
        return
    if args.side:
        run_side(args.side, args.rows[0], args.probes, args.threads)
        return

    if not args.skip_verify:
        print(f"verifying parity at {VERIFY_ROWS:,} rows ... ", end="", flush=True)
        spawn(["--verify"])
        print("ok")

    if args.rows:
        rows_plan = sorted(set(args.rows))
    else:
        low, high = min(args.row_range), max(args.row_range)
        rows_plan = plan_rows(low, high, args.points, random.Random(args.seed))
    low, high = min(rows_plan), max(rows_plan)

    print(f"{len(rows_plan)} row counts in {low:,}..{high:,}, fixed before the first timing")
    collected = run_sweep(rows_plan)

    threads_collected = None
    if not args.skip_threads:
        ladder = sorted(set(args.thread_ladder))
        print(f"\nthread ladder {ladder} at {args.thread_rows:,} rows")
        threads_collected = run_thread_ladder(args.thread_rows, ladder)

    report_summary(collected, low, high, threads_collected)
    if args.verbose:
        report_detail(collected)
        if threads_collected:
            report_threads(threads_collected, args.thread_rows)

    if args.json:
        payload = {"sweep": {str(r): v for r, v in collected.items()}}
        if threads_collected:
            payload["threads"] = {str(n): v for n, v in threads_collected.items()}
            payload["thread_rows"] = args.thread_rows
        args.json.write_text(json.dumps(payload, indent=2))
        print(f"\nwrote {args.json}")
    if args.csv:
        write_csv(args.csv, collected)
        print(f"wrote {args.csv}")


if __name__ == "__main__":
    main()
