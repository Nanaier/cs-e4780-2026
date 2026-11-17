"""Figures and tables for the performance section.

Reads the run JSON files that `trading.bench` wrote and that were copied into analysis/performance/results/,
and writes PNG figures and CSV tables into analysis/performance/figures/.

    results/throughput-*/   default CRC32 partitioning, p = 1, 2, 4, 3 runs each (+ python.json)
    results/balanced-*/     same with the balanced partition map (partition-map-08.csv)
    results/ft-alo-*/       10 s checkpoints, at-least-once sinks, p = 2, 4
    results/ft-eo-*/        10 s checkpoints, exactly-once sinks, p = 2, 4
    results/exchange-*/     exchange subsets (nl, nl-fr), p = 4

Usage: python analysis/performance/figures.py   (needs matplotlib: pip install -r analysis/performance/requirements.txt)
"""

import csv
import json
import statistics
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

HERE = Path(__file__).parent
RESULTS = HERE / "results"
FIGURES = HERE / "figures"
BLUE, ORANGE, GREEN, GREY = "#2f6db5", "#e07b24", "#3a9a5b", "#888888"


def load(pattern):
    """All Flink run files under results/<pattern>/, as dicts."""
    return [json.loads(f.read_text()) for d in sorted(RESULTS.glob(pattern)) for f in sorted(d.glob("flink-p*-r*.json"))]


def by_p(runs, key):
    """{p: [values]} for a throughput key, e.g. records_per_s_source."""
    out = {}
    for r in runs:
        out.setdefault(r["p"], []).append(r["throughput"][key])
    return dict(sorted(out.items()))


def med_range(values):
    m = statistics.median(values)
    return m, m - min(values), max(values) - m


def errorbar(ax, xs, groups, color, label, marker="o"):
    """Median with min-max whiskers, plus every run as a small dot."""
    meds, lo, hi = zip(*(med_range(g) for g in groups))
    ax.errorbar(xs, meds, yerr=[lo, hi], color=color, marker=marker, capsize=4, lw=1.6, label=label)
    for x, g in zip(xs, groups):
        ax.scatter([x] * len(g), g, color="black", s=8, zorder=3)
    return meds


def throughput_and_speedup(default, balanced, python_rate):
    """Throughput vs cores, and measured speedup vs the bound set by the busiest source reader."""
    FIGURES.mkdir(exist_ok=True)
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(10, 3.8))
    series = [("default (CRC32)", default, BLUE)] + ([("balanced map", balanced, GREEN)] if balanced else [])
    rows = []
    for label, runs, color in series:
        rates = by_p(runs, "records_per_s_source")
        ps = list(rates)
        meds = errorbar(a1, ps, [[v / 1e6 for v in rates[p]] for p in ps], color, label)
        base = meds[0]
        # Two bounds on the speedup: the busiest Kafka reader (source subtask) and the busiest keyed subtask.
        # A stage whose busiest subtask gets share s of the ticks can run at most 1/s times the 1-core rate.
        bound, kbound = {}, {}
        for r in runs:
            bound.setdefault(r["p"], []).append(1 / r["source_split"]["busiest_share"])
            kbound.setdefault(r["p"], []).append(1 / r["keyed_split"]["busiest_share"])
        a2.plot(ps, [m / base for m in meds], color=color, marker="o", label=f"measured, {label}")
        a2.plot(ps, [statistics.median(bound[p]) for p in ps], color=color, ls="--", marker="x",
                label=f"bound: busiest Kafka reader, {label}")
        a2.plot(ps, [statistics.median(kbound[p]) for p in ps], color=color, ls=":", marker="+",
                label=f"bound: busiest keyed subtask, {label}")
        for p, m in zip(ps, meds):
            rows.append([label, p, round(m * 1e6), round(m / base, 2), round(statistics.median(bound[p]), 2),
                         round(statistics.median(kbound[p]), 2),
                         round(statistics.median(r["cpu_percent_mean"]["taskmanager"] / p for r in runs if r["p"] == p), 1)])
    if python_rate:
        a1.axhline(python_rate / 1e6, color=GREY, ls=":", label="Python engine (1 process)")
    a1.set(xlabel="TaskManager cores (= parallelism)", ylabel="throughput (M ticks/s)", title="Throughput, full week",
           xticks=[1, 2, 4], ylim=(0, None))
    a1.legend(fontsize=8)
    a2.plot([1, 4], [1, 4], color=GREY, lw=0.8, label="linear")
    a2.set(xlabel="TaskManager cores", ylabel="speedup vs 1 core", title="Speedup = min(source bound, keyed bound)",
           xticks=[1, 2, 4], ylim=(0, 4.3))
    a2.legend(fontsize=7)
    for a in (a1, a2):
        a.grid(alpha=0.3)
    fig.suptitle("Ryzen 7 9800X3D, Docker in WSL2 (6 vCPUs): TaskManager on vCPUs 0..p-1, Kafka + JobManager on 4-5",
                 fontsize=9)
    fig.tight_layout()
    fig.savefig(FIGURES / "throughput_speedup.png", dpi=200)
    with open(FIGURES / "throughput_speedup.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["partitioning", "p", "median_ticks_per_s_source", "speedup", "bound_busiest_source_reader",
                    "bound_busiest_keyed_subtask",
                    "taskmanager_cpu_percent_per_core"])
        w.writerows(rows)


def fault_tolerance(none, alo, eo):
    """Throughput cost of 10 s checkpoints with at-least-once and exactly-once Kafka sinks."""
    fig, ax = plt.subplots(figsize=(6, 3.6))
    configs = [("none", none, GREY), ("at-least-once", alo, ORANGE), ("exactly-once", eo, BLUE)]
    width = 0.25
    rows = []
    for i, (label, runs, color) in enumerate(configs):
        rates = by_p([r for r in runs if r["p"] in (2, 4)], "records_per_s_source")
        xs = [j + (i - 1) * width for j in range(len(rates))]
        meds = [statistics.median(v) / 1e6 for v in rates.values()]
        ax.bar(xs, meds, width, color=color, label=label)
        for x, v in zip(xs, rates.values()):
            ax.scatter([x] * len(v), [u / 1e6 for u in v], color="black", s=8, zorder=3)
        rows += [[label, p, round(statistics.median(v))] for p, v in rates.items()]
    ax.set_xticks(range(2), ["p = 2", "p = 4"])
    ax.set(ylabel="throughput (M ticks/s)", title="Cost of fault tolerance (10 s checkpoints)", ylim=(0, None))
    ax.legend(fontsize=8)
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(FIGURES / "fault_tolerance_cost.png", dpi=200)
    with open(FIGURES / "fault_tolerance_cost.csv", "w", newline="") as fh:
        csv.writer(fh).writerows([["delivery", "p", "median_ticks_per_s_source"]] + rows)


def exchange_scaling(default):
    """Amsterdam, Amsterdam + Paris and all three exchanges at p = 4: job time and throughput vs input size."""
    subsets = [("NL", load("exchange-nl-2*")), ("NL+FR", load("exchange-nl-fr-*")),
               ("NL+FR+ETR", [r for r in default if r["p"] == 4])]
    subsets = [(name, runs) for name, runs in subsets if runs]
    if len(subsets) < 2:
        return
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(10, 3.6))
    rows = []
    for i, (name, runs) in enumerate(subsets):
        ticks = runs[0]["records"] / 1e6
        secs = [r["job_ms"] / 1000 for r in runs]
        rates = [r["throughput"]["records_per_s_job"] / 1e6 for r in runs]
        a1.scatter([ticks] * len(secs), secs, color="black", s=10, zorder=3)
        a1.errorbar([ticks], [statistics.median(secs)], yerr=[[statistics.median(secs) - min(secs)],
                    [max(secs) - statistics.median(secs)]], color=BLUE, marker="o", capsize=4)
        a1.annotate(name, (ticks, statistics.median(secs)), textcoords="offset points", xytext=(6, -12), fontsize=8)
        a2.bar(i, statistics.median(rates), color=BLUE)
        a2.scatter([i] * len(rates), rates, color="black", s=10, zorder=3)
        rows.append([name, runs[0]["records"], round(statistics.median(secs), 2), round(statistics.median(rates) * 1e6)])
    a1.set(xlabel="ticks in the input (M)", ylabel="job time (s), start to finish", title="Job time vs input size, p = 4",
           xlim=(0, None), ylim=(0, None))
    a2.set_xticks(range(len(subsets)), [n for n, _ in subsets])
    a2.set(ylabel="throughput (M ticks/s, whole job)", title="Throughput by exchange subset, p = 4", ylim=(0, None))
    for a in (a1, a2):
        a.grid(alpha=0.3)
    fig.text(0.5, 0.01, "Small inputs finish in a few seconds, so the job's fixed start-up time lowers their throughput.",
             ha="center", fontsize=8)
    fig.tight_layout(rect=(0, 0.05, 1, 1))
    fig.savefig(FIGURES / "exchange_scaling.png", dpi=200)
    with open(FIGURES / "exchange_scaling.csv", "w", newline="") as fh:
        csv.writer(fh).writerows([["exchanges", "ticks", "median_job_s", "median_ticks_per_s_job"]] + rows)


def latency_vs_speed():
    """Latency on real data (08.11) at several replay speeds, from the moment each window could first close."""
    runs = []
    for f in RESULTS.glob("latency-*/*/latency.json"):
        speed = int(f.parent.name.split("-x")[1].split("-")[0])
        runs.append((speed, json.loads(f.read_text())))
    if not runs:
        return
    runs.sort(key=lambda t: t[0])
    speeds = [s for s, _ in runs]
    fig, ax = plt.subplots(figsize=(6.5, 3.8))
    for key, label, color in [("latency_ms_from_closable", "window closable -> result", BLUE),
                              ("latency_ms_engine_stamp", "inside the engine", ORANGE)]:
        for q, ls in (("p50", "-"), ("p99", "--")):
            ax.plot(speeds, [r[key][q] for _, r in runs], color=color, ls=ls, marker="o", label=f"{label}, {q}")
    ax.set_xscale("log")
    ax.set_xticks(speeds, [f"{s}x" for s in speeds])
    ax.set(xlabel="replay speed (log scale; 1x = real time)", ylabel="latency (ms)", ylim=(0, None),
           title="Latency vs load, real data (08.11), Flink p = 4")
    ax.legend(fontsize=7, loc="center right")
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(FIGURES / "latency_vs_speed.png", dpi=200)
    with open(FIGURES / "latency_vs_speed.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["speed", "rows", "closable_p50", "closable_p95", "closable_p99", "closable_max",
                    "hot_p50", "tail_p50", "engine_p50", "engine_p99"])
        for s, r in runs:
            c = r["latency_ms_from_closable"]
            w.writerow([s, r["counts"]["rows"], c["p50"], c["p95"], c["p99"], c["max"],
                        r["latency_ms_from_closable_hot"]["p50"], r["latency_ms_from_closable_tail"]["p50"],
                        r["latency_ms_engine_stamp"]["p50"], r["latency_ms_engine_stamp"]["p99"]])


def main():
    default = load("throughput-*")
    balanced = load("balanced-*")
    py = [json.loads(f.read_text()) for f in RESULTS.glob("throughput-*/python.json")]
    throughput_and_speedup(default, balanced, next(iter(py[0]["throughput"].values())) if py else None)
    if load("ft-alo-*") and load("ft-eo-*"):
        fault_tolerance(default, load("ft-alo-*"), load("ft-eo-*"))
    exchange_scaling(default)
    latency_vs_speed()
    print("figures in", FIGURES)


if __name__ == "__main__":
    main()
