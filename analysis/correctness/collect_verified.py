"""Collect every verified run into one correctness table.

Sources (all committed to the repo, so no data download is needed):
  * analysis/performance/results/**.json  - bench.py runs with --verify (Kafka source, Python engine,
    checkpoints, TaskManager kill, exchange subsets, subscriber filtering), by Person 2
  * analysis/performance/results/correctness-*/*.txt and analysis/correctness/results/*.txt
    - diff summaries of file-mode runs ("ema: left=... mismatched=0 ...")

Writes analysis/correctness/results/correctness.csv (one row per verified run) and
correctness_table.tex (the grouped table for the report).

    python analysis/correctness/collect_verified.py [--performance DIR] [--extra DIR ...]
"""
import argparse
import csv
import json
import re
from collections import OrderedDict
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]

SCENARIOS = [  # (directory prefix, label, order)
    ("throughput", "Kafka source, default partitioner", 2),
    ("balanced", "Kafka source, balanced partition map", 3),
    ("ft-alo", "10\\,s checkpoints, at-least-once sinks", 4),
    ("ft-eo", "10\\,s checkpoints, exactly-once sinks", 5),
    ("recovery-alo", "TaskManager killed at 41\\,\\%, at-least-once + dedupe", 6),
    ("recovery-eo", "TaskManager killed at 41\\,\\%, exactly-once", 7),
    ("exchange-nl-fr", "Kafka source, Amsterdam + Paris only", 8),
    ("exchange-nl", "Kafka source, Amsterdam only", 8),
    ("subscribers", "Subscriber filter: all symbols, 1--10{,}000 subscribers", 9),
]
DIFF_LINE = re.compile(r"(ema|advisories)[^:]*: left=(\d+) right=(\d+) matched=(\d+) mismatched=(\d+) only_left=(\d+) only_right=(\d+)")
RUN_LINE = re.compile(r"^\s*([\w-]+)\s+p=(\d+)\s+exit=(\d+)")


def input_of(reference):
    ref = str(reference or "")
    if "/subsets/nl-fr/" in ref:
        return "week, NL+FR"
    if "/subsets/nl/" in ref:
        return "week, NL"
    if "scaling" in ref or "day08" in ref:
        return "08.11"
    if "synthetic" in ref:
        return "synthetic"
    return "week"


def scenario_of(path):
    top = path.parts[0] if path.parts else ""
    for prefix, label, order in SCENARIOS:
        if top.startswith(prefix):
            return label, order
    return top, 99


def from_json(results_dir):
    rows = []
    for path in sorted(results_dir.rglob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (ValueError, UnicodeDecodeError):
            continue
        verify = data.get("verify") if isinstance(data, dict) else None
        if not isinstance(verify, dict) or "ema" not in verify:
            continue
        rel = path.relative_to(results_dir)
        label, order = scenario_of(rel)
        engine = data.get("engine", "?")
        if engine == "python":
            label, order = "Python engine, Kafka source", 1
        dup = (verify.get("collect") or {}).get("duplicates_dropped") or {}
        subs = None
        sub_file = path.parent / "subscribers.json"
        if sub_file.exists():
            subs = json.loads(sub_file.read_text(encoding="utf-8")).get("subscribers")
        rows.append({
            "scenario": label, "order": order, "engine": engine, "source": "kafka", "p": data.get("p"),
            "input": input_of(verify.get("reference")), "subscribers": subs if subs is not None else "",
            "ema_rows": verify["ema"]["left"], "advisories": verify["advisories"]["left"],
            "mismatched": verify["ema"]["mismatched"] + verify["advisories"]["mismatched"],
            "missing": verify["ema"]["only_right"] + verify["advisories"]["only_right"],
            "extra": verify["ema"]["only_left"] + verify["advisories"]["only_left"],
            "duplicates_dropped": dup.get("ema", 0) + dup.get("advisories", 0),
            "identical": bool(verify.get("identical")), "file": str(Path(results_dir.name, rel)).replace("\\", "/"),
        })
    return rows


def from_txt(path, input_name="week"):
    """Parse a log of `<engine> p=<n> exit=<code>` lines, each followed by the two diff lines."""
    rows, current = [], None
    for line in path.read_text(encoding="utf-8").splitlines():
        run = RUN_LINE.match(line)
        if run:
            engine = run.group(1)
            current = {
                "scenario": {"flink-file": "File source (Flink)",
                             "flink-file-ew": "File source (Flink), watermark per tick",
                             "python-file": "Python engine, file source"}.get(engine, engine),
                "order": {"flink-file": 0, "flink-file-ew": 0}.get(engine, 1), "engine": engine.split("-")[0],
                "source": "file", "p": int(run.group(2)), "input": input_name, "subscribers": "",
                "ema_rows": 0, "advisories": 0, "mismatched": 0, "missing": 0, "extra": 0,
                "duplicates_dropped": 0, "identical": run.group(3) == "0", "file": path.name,
            }
            rows.append(current)
            continue
        diff = DIFF_LINE.search(line)
        if diff and current is not None:
            kind, left, _right, _matched, mism, only_l, only_r = diff.groups()
            current["ema_rows" if kind == "ema" else "advisories"] = int(left)
            current["mismatched"] += int(mism)
            current["extra"] += int(only_l)
            current["missing"] += int(only_r)
            current["identical"] = current["identical"] and int(mism) == int(only_l) == int(only_r) == 0
    return rows


def grouped(rows):
    groups = OrderedDict()
    for r in sorted(rows, key=lambda r: (r["order"], r["input"] != "week", r["scenario"], r["p"] or 0)):
        g = groups.setdefault((r["scenario"], r["input"]), {"p": set(), "runs": 0, "ok": 0, "ema": set(), "adv": set(), "dups": 0})
        g["p"].add(r["p"])
        g["runs"] += 1
        g["ok"] += r["identical"]
        g["ema"].add(r["ema_rows"])
        g["adv"].add(r["advisories"])
        g["dups"] += r["duplicates_dropped"]
    return groups


def plain(label):
    return label.replace("\\,", " ").replace("\\%", "%").replace("{,}", ",").replace("--", "-")


def fmt_set(values):
    values = sorted(v for v in values if v is not None)
    if len(values) == 1:
        return f"{values[0]:,}".replace(",", "{,}")
    return f"{values[0]:,}--{values[-1]:,}".replace(",", "{,}")


def latex(groups, total_runs, total_ok):
    lines = [
        "% Generated by analysis/correctness/collect_verified.py -- do not edit by hand.",
        "\\begin{table}[t]",
        "  \\caption{Every verified run, compared row by row with the batch reference (same input, same 300\\,ms bound). "
        f"{total_ok} of {total_runs} runs are identical: no mismatched, missing or extra EMA rows or advisories.}}",
        "  \\label{tab:correctness}",
        "  \\small",
        "  \\begin{tabular}{@{}llrrrc@{}}",
        "    \\toprule",
        "    Configuration & $p$ & Input & EMA rows & Adv. & Runs \\\\",
        "    \\midrule",
    ]
    for (scenario, input_name), g in groups.items():
        ps = ",".join(str(p) for p in sorted(p for p in g["p"] if p is not None))
        ok = f"{g['ok']}/{g['runs']}"
        lines.append(f"    {scenario} & {ps} & {input_name} & {fmt_set(g['ema'])} & {fmt_set(g['adv'])} & {ok} \\\\")
    lines += ["    \\bottomrule", "  \\end{tabular}", "\\end{table}", ""]
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--performance", default=str(ROOT / "analysis/performance/results"),
                        help="bench.py result folders (Person 2's branch)")
    parser.add_argument("--extra", nargs="*", default=[str(HERE / "results")],
                        help="folders with *.txt diff logs of file-mode runs")
    parser.add_argument("--out", default=str(HERE / "results"))
    args = parser.parse_args(argv)

    perf = Path(args.performance)
    rows = []
    if perf.exists():
        rows += from_json(perf)
        for txt in sorted(perf.glob("correctness-*/*.txt")):
            rows += from_txt(txt)
    else:
        print(f"note: {perf} not found (performance branch not merged yet?); using local logs only")
    for folder in args.extra:
        for txt in sorted(Path(folder).glob("*.txt")):
            rows += from_txt(txt, "synthetic" if "synthetic" in txt.name else "week")
    if not rows:
        raise SystemExit("no verified runs found")

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    fields = ["scenario", "engine", "source", "p", "input", "subscribers", "ema_rows", "advisories",
              "mismatched", "missing", "extra", "duplicates_dropped", "identical", "file"]
    rows.sort(key=lambda r: (r["order"], r["input"] != "week", r["scenario"], r["p"] or 0, str(r["subscribers"])))
    with (out / "correctness.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows({**r, "scenario": plain(r["scenario"])} for r in rows)
    week_rows = [r for r in rows if r["input"] != "synthetic"]
    total_ok = sum(r["identical"] for r in week_rows)
    (out / "correctness_table.tex").write_text(latex(grouped(week_rows), len(week_rows), total_ok), encoding="utf-8")
    compared = sum(r["ema_rows"] + r["advisories"] for r in week_rows)
    print(f"{len(rows)} verified runs ({len(week_rows)} on real data, {total_ok} identical), "
          f"{compared:,} result rows compared; wrote {out / 'correctness.csv'} and correctness_table.tex")
    return 0 if total_ok == len(week_rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
