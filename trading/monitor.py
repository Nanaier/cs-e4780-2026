"""The dashboard: one screen in the style of a market data terminal (also built by `python -m trading.viz`).

Every window follows a playhead that replays the data hour by hour; real-week statistics from analysis/dataset
(trades per minute, exchange split, symbol ranks) are shown next to whatever results folder it is built from.

    python -m trading.monitor --results out/week/reference-300 --metadata data/week/metadata.parquet
"""
import argparse
import csv
import json
import time
from collections import defaultdict
from pathlib import Path

TEMPLATE = Path(__file__).with_name("monitor.html")
DATASET = Path(__file__).resolve().parents[1] / "analysis" / "dataset" / "results"
WARMUP_WINDOWS = 230  # (1 - 2/101)^230 < 1 %: from here on EMA100 no longer remembers EMA_0 = 0
EXCHANGES = ["FR", "NL", "ETR"]  # fixed order, so an exchange keeps its colour whatever the data
DATASET_TABLES = ["f1_row_types_summary", "f1_row_types_by_day", "f2_by_exchange", "f2_by_sectype",
                  "f3_long_tail_summary", "f4_rate_summary", "f4_window_summary", "f4_peak_seconds",
                  "f5_out_of_order_lag", "f8_metadata_coverage", "f8_currencies"]


def load_dataset(folder):
    """Facts about the real week from analysis/dataset (Person 1), whatever results the dashboard shows."""
    folder = Path(folder) if folder else None
    if not folder or not (folder / "f4_rate_per_minute.csv").exists():
        return None
    tables = {name: read_csv(folder / f"{name}.csv") for name in DATASET_TABLES if (folder / f"{name}.csv").exists()}
    rate = [[int(r["minute_ms"]) // 1000, int(r["trades"]), int(r["peak_trades_per_s"])]
            for r in read_csv(folder / "f4_rate_per_minute.csv")]
    ranks = {}
    if (folder / "f3_trades_per_symbol.csv").exists():
        ranks = {r["symbol"]: [int(r["rank"]), int(r["trades"])] for r in read_csv(folder / "f3_trades_per_symbol.csv")}
    return {"tables": tables, "rate": rate, "ranks": ranks}


def read_csv(path):
    with Path(path).open(newline="") as fh:
        return list(csv.DictReader(fh))


def ema_rows(path, *names):
    """Stream (symbol, window_end_ms, *names) from ema.csv without keeping it in memory (the week has 1.9M rows)."""
    with Path(path).open(newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        cols = [header.index(n) for n in ("symbol", "window_end_ms", *names)]
        for row in reader:
            yield [row[c] for c in cols]


def first_window(advisory):
    return float(advisory["prev_ema38"]) == 0.0 and float(advisory["prev_ema100"]) == 0.0


def exchange_of(symbol):
    return symbol.rsplit(".", 1)[1] if "." in symbol else None


def load_metadata(path):
    if not path or not Path(path).exists():
        return {}
    import duckdb
    columns = duckdb.sql(f"SELECT * FROM read_parquet('{path}') LIMIT 0").columns
    currency = "currency" if "currency" in columns else "NULL"  # older metadata files have no currency column
    rows = duckdb.sql(
        f"SELECT symbol, isin, related_indices, sec_type, exchange, {currency} FROM read_parquet('{path}')").fetchall()
    return {symbol: {"isin": isin, "index": index, "sec_type": sec_type, "exchange": exchange, "currency": cur}
            for symbol, isin, index, sec_type, exchange, cur in rows}


def overview(per_symbol, advisories, regime, hours, metadata, by_hour):
    """Market-wide numbers: every symbol, not only the ones with a chart."""
    exch = {s: metadata.get(s, {}).get("exchange") or exchange_of(s) for s in per_symbol}
    present = set(exch.values()) - {None}
    exchanges = ([x for x in EXCHANGES if x in present] + sorted(present - set(EXCHANGES)))[:3]
    xi = {x: i for i, x in enumerate(exchanges)}
    xsym = [0] * len(exchanges)
    position = {h: k for k, h in enumerate(sorted(hours))}
    scatter = []
    for s, info in per_symbol.items():
        i = xi.get(exch[s])
        if i is not None:
            xsym[i] += 1
            windows = [v for h, n in sorted(by_hour[s].items()) for v in (position[h], n)]
            scatter.append([s, i, info["rows"], info["buy"] + info["sell"], windows])

    feed, cells = [], defaultdict(lambda: [0, 0])  # (day, hour) -> [buy, sell]
    for a in advisories:
        if first_window(a):
            continue
        t = int(a["window_end_ms"]) // 1000
        sell = a["advisory"] != "BUY"
        feed.append([a["symbol"], t, int(sell), xi.get(exch.get(a["symbol"]), -1),
                     round(float(a["ema38"]), 4), round(float(a["ema100"]), 4)])
        own = t - 1  # the window's own hour, not the next one
        cells[(time.strftime("%Y-%m-%d", time.gmtime(own)), own // 3600 % 24)][sell] += 1
    feed.sort(key=lambda f: (f[1], f[0]))
    days = sorted({d for d, _ in cells})
    hour_of_day = [h for _, h in cells] or [0]
    return {
        "symbols": len(per_symbol), "exchanges": exchanges, "xsymbols": xsym,
        "warmup": WARMUP_WINDOWS, "min_settled": max(5, min(50, len(per_symbol) // 20)),
        "t0": min(hours) if hours else 0, "t1": max(hours) + 3600 if hours else 0,
        "hours": sorted(hours),
        "regime": [[t, up, n] for t, (up, n) in sorted(regime.items())],
        "heat": {"days": days, "h0": min(hour_of_day), "h1": max(hour_of_day),
                 "cells": [[days.index(d), h, b, s] for (d, h), (b, s) in sorted(cells.items())]},
        "scatter": scatter,
        "feed": feed,
    }


def choose(per_symbol, max_symbols, include):
    """Pick the most active symbols, round-robin over exchanges so every exchange is represented."""
    ranked = sorted(per_symbol, key=lambda s: (-(per_symbol[s]["buy"] + per_symbol[s]["sell"]), -per_symbol[s]["rows"], s))
    queues = defaultdict(list)
    for s in ranked:
        queues[exchange_of(s)].append(s)
    chosen = [s for s in include if s in per_symbol]
    order = sorted(queues, key=lambda x: (-len(queues[x]), str(x)))
    while len(chosen) < max_symbols and any(queues.values()):
        for x in order:
            while queues[x] and queues[x][0] in chosen:
                queues[x].pop(0)
            if queues[x] and len(chosen) < max_symbols:
                chosen.append(queues[x].pop(0))
    return set(chosen)


def main(argv=None, default_name="monitor.html"):
    parser = argparse.ArgumentParser(description="Build the dashboard (one HTML page) from ema.csv and advisories.csv")
    parser.add_argument("--results", default="out/stream")
    parser.add_argument("--metadata", default=None, help="metadata.parquet from the prep step")
    parser.add_argument("--max-symbols", type=int, default=60)
    parser.add_argument("--include", default="", help="comma-separated symbols that are always shown")
    parser.add_argument("--dataset", default=str(DATASET),
                        help="analysis/dataset/results folder with the real-week statistics ('' to leave them out)")
    parser.add_argument("--out", default=None)
    args = parser.parse_args(argv)

    results = Path(args.results)
    ema_path = results / "ema.csv"
    advisories = read_csv(results / "advisories.csv")
    metrics_path = results / "metrics.json"
    metrics = json.loads(metrics_path.read_text()) if metrics_path.exists() else None
    metadata = load_metadata(args.metadata)

    per_symbol = defaultdict(lambda: {"buy": 0, "sell": 0, "first": 0, "last": None, "last_t": None, "rows": 0,
                                      "end": None, "trend": None})
    ema_total = 0
    hours = set()
    by_hour = defaultdict(lambda: defaultdict(int))
    regime = defaultdict(lambda: [0, 0])  # hour -> [in an uptrend, settled symbol-windows]
    for symbol, end, ema38, ema100 in ema_rows(ema_path, "ema38", "ema100"):  # pass 1: counts and current trend
        ema_total += 1
        info = per_symbol[symbol]
        hour = (int(end) - 1) // 3_600_000 * 3600
        hours.add(hour)
        by_hour[symbol][hour] += 1
        if info["rows"] >= WARMUP_WINDOWS:  # the symbol's EMA100 has settled, so its trend is real
            r = regime[hour]
            r[0] += float(ema38) > float(ema100)
            r[1] += 1
        info["rows"] += 1
        end = int(end)
        if info["end"] is None or end >= info["end"]:
            info["end"] = end
            info["trend"] = "up" if float(ema38) > float(ema100) else "down"
    advisory_isin = {a["symbol"]: a["isin"] for a in advisories if a.get("isin")}  # --enrich output
    advisory_currency = {a["symbol"]: a["currency"] for a in advisories if a.get("currency")}
    for a in advisories:
        info = per_symbol[a["symbol"]]
        if first_window(a):
            info["first"] += 1
            continue
        info["buy" if a["advisory"] == "BUY" else "sell"] += 1
        t = int(a["window_end_ms"]) // 1000
        if info["last_t"] is None or t >= info["last_t"]:
            info["last"], info["last_t"] = a["advisory"], t
    include = [s.strip() for s in args.include.split(",") if s.strip()]
    chosen = choose(per_symbol, args.max_symbols, include)

    series = defaultdict(lambda: {"t": [], "c": [], "a": [], "b": []})
    for symbol, end, close, ema38, ema100 in ema_rows(ema_path, "close", "ema38", "ema100"):  # pass 2: chosen only
        if symbol in chosen:
            s = series[symbol]
            s["t"].append(int(end) // 1000)
            s["c"].append(round(float(close), 6))
            s["a"].append(round(float(ema38), 6))
            s["b"].append(round(float(ema100), 6))
    for s in series.values():  # the chart needs ascending time; a multi-subtask engine may write out of order
        if any(a > b for a, b in zip(s["t"], s["t"][1:])):
            order = sorted(range(len(s["t"])), key=s["t"].__getitem__)
            for k in s:
                s[k] = [s[k][i] for i in order]
    shown_advisories = []
    for a in advisories:
        if a["symbol"] in chosen:
            shown_advisories.append({"s": a["symbol"], "t": int(a["window_end_ms"]) // 1000, "k": a["advisory"],
                                     "a": float(a["ema38"]), "b": float(a["ema100"]), "w": first_window(a)})
    shown_advisories.sort(key=lambda a: (a["s"], a["t"]))  # chart markers must be in ascending time

    symbols = []
    for s in sorted(chosen):
        meta = metadata.get(s, {})
        info = per_symbol[s]
        symbols.append({"s": s, "isin": meta.get("isin") or advisory_isin.get(s), "index": meta.get("index"),
                        "cur": meta.get("currency") or advisory_currency.get(s),
                        "x": meta.get("exchange") or exchange_of(s), "k": meta.get("sec_type"),
                        "buy": info["buy"], "sell": info["sell"], "first": info["first"], "rows": info["rows"],
                        "trend": info["trend"], "last": info["last"], "last_t": info["last_t"]})
    data = {
        "overview": overview(per_symbol, advisories, regime, hours, metadata, by_hour),
        "symbols": symbols,
        "series": series,
        "advisories": shown_advisories,
        "totals": {"ema": ema_total, "first": sum(first_window(a) for a in advisories)},
        "metrics": metrics,
        "source": str(results).replace("\\", "/"),
        "real": load_dataset(args.dataset),
    }
    page = TEMPLATE.read_text(encoding="utf-8")
    out = Path(args.out) if args.out else results / default_name
    out.write_text(page.replace("__DATA__", json.dumps(data, separators=(",", ":")).replace("</", "<\\/")), encoding="utf-8")
    print(f"wrote {out} ({len(symbols)} symbols)")


if __name__ == "__main__":
    main()
