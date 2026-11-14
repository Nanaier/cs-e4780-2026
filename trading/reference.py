import argparse
import json
import time
from pathlib import Path

import duckdb

from trading.core import quarantine_sql
from trading.files import ResultWriter, load_subscriptions, tick_files

WINDOW_MS = 300_000


def window_closes(con, files, window_ms, late_bound_ms=None, quarantine_ms=None):
    file_list = "[" + ", ".join(f"'{f}'" for f in files) + "]"
    admitted = "TRUE" if quarantine_ms is None else f"NOT {quarantine_sql(quarantine_ms)}"
    closed_until = "NULL"
    if late_bound_ms is not None:
        watermark = f"(max(event_ts) OVER (ORDER BY seq ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) - {late_bound_ms})"
        closed_until = f"{watermark} - {watermark} % {window_ms}"
    return con.execute(f"""
        WITH arrived AS (
            SELECT symbol, last, event_ts, seq, event_ts - event_ts % {window_ms} AS window_start,
                   {closed_until} AS closed_until
            FROM read_parquet({file_list})
            WHERE {admitted}
        ),
        t AS (
            SELECT * FROM arrived WHERE closed_until IS NULL OR window_start >= closed_until
        ),
        ranked AS (
            SELECT symbol, window_start, last,
                   row_number() OVER (PARTITION BY symbol, window_start ORDER BY event_ts DESC, seq DESC) AS rn
            FROM t
        )
        SELECT symbol, window_start, last FROM ranked WHERE rn = 1 ORDER BY symbol, window_start
    """)


def evaluate_symbol(symbol, windows, last_window, window_ms, empty_window, suppress_first):
    a38 = 2.0 / (1.0 + 38)
    a100 = 2.0 / (1.0 + 100)
    ema38 = ema100 = 0.0
    evaluated = False
    rows, advisories = [], []
    if empty_window == "repeat":
        filled = []
        by_start = dict(windows)
        close = None
        for start in range(windows[0][0], last_window + window_ms, window_ms):
            close = by_start.get(start, close)
            filled.append((start, close))
        windows = filled
    for start, close in windows:
        prev38, prev100 = ema38, ema100
        ema38 = close * a38 + prev38 * (1.0 - a38)
        ema100 = close * a100 + prev100 * (1.0 - a100)
        end = start + window_ms
        rows.append((symbol, end, close, ema38, ema100))
        kind = None
        if ema38 > ema100 and prev38 <= prev100:
            kind = "BUY"
        elif ema38 < ema100 and prev38 >= prev100:
            kind = "SELL"
        if kind and (evaluated or not suppress_first):
            advisories.append((symbol, end, kind, ema38, ema100, prev38, prev100))
        evaluated = True
    return rows, advisories


def main(argv=None):
    parser = argparse.ArgumentParser(description="Batch reference implementation of Query 1 and Query 2")
    parser.add_argument("--ticks", required=True, help="tick parquet file or directory")
    parser.add_argument("--out", default="out/reference")
    parser.add_argument("--subscriptions", default="all")
    parser.add_argument("--empty-window", choices=["skip", "repeat"], default="skip")
    parser.add_argument("--suppress-first", action="store_true")
    parser.add_argument("--late-bound-ms", type=int, default=None,
                        help="drop events a stream with this out-of-orderness bound would drop (default: keep all)")
    parser.add_argument("--quarantine-ms", type=int, default=None,
                        help="remove ticks whose sys_ts is more than N ms after event_ts before anything else")
    args = parser.parse_args(argv)

    started = time.perf_counter()
    files = tick_files(args.ticks)
    subscribed = load_subscriptions(args.subscriptions)
    con = duckdb.connect()
    held = "FALSE" if args.quarantine_ms is None else quarantine_sql(args.quarantine_ms)
    last_window, quarantined = con.execute(
        f"SELECT max(event_ts - event_ts % ?) FILTER (WHERE NOT {held}), count(*) FILTER (WHERE {held}) FROM read_parquet(?)",
        [WINDOW_MS, [str(f) for f in files]]
    ).fetchone()
    cursor = window_closes(con, files, WINDOW_MS, args.late_bound_ms, args.quarantine_ms)

    all_rows, all_advisories = [], []
    counts = {"symbols": 0, "ema_rows": 0, "advisories": 0}
    if args.quarantine_ms is not None:
        counts["quarantined"] = quarantined
    current, windows = None, []

    def finish():
        if current is None:
            return
        rows, advisories = evaluate_symbol(current, windows, last_window, WINDOW_MS, args.empty_window, args.suppress_first)
        counts["symbols"] += 1
        counts["ema_rows"] += len(rows)
        counts["advisories"] += len(advisories)
        if subscribed is None or current in subscribed:
            all_rows.extend(rows)
            all_advisories.extend(advisories)

    while True:
        chunk = cursor.fetchmany(100_000)
        if not chunk:
            break
        for symbol, start, close in chunk:
            if symbol != current:
                finish()
                current, windows = symbol, []
            windows.append((start, close))
    finish()

    writer = ResultWriter(args.out)
    for symbol, end, close, ema38, ema100 in sorted(all_rows, key=lambda r: (r[1], r[0])):
        writer.write_ema(symbol, end, close, ema38, ema100)
    for advisory in sorted(all_advisories, key=lambda r: (r[1], r[0])):
        writer.write_advisory(*advisory)
    writer.close()
    counts["seconds"] = round(time.perf_counter() - started, 3)
    Path(args.out, "metrics.json").write_text(json.dumps(counts, indent=2))
    print(json.dumps(counts, indent=2))


if __name__ == "__main__":
    main()
