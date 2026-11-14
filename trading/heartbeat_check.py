import argparse
import json
from collections import Counter
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from trading.core import WINDOW_MS
from trading.files import local_time, tick_files


def window_start(values):
    return pc.multiply(pc.divide(values, WINDOW_MS), WINDOW_MS)


def carry_max(values, carried):
    run = pc.cumulative_max(values)
    return run if carried is None else pc.max_element_wise(run, pa.scalar(carried, pa.int64()))


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Replay order check of the heartbeat rule H = feed clock - margin: ticks behind the heartbeat "
                    "watermark H - bound, taking for each tick the largest H the replay can send before it")
    parser.add_argument("--ticks", required=True, help="tick parquet file or directory")
    parser.add_argument("--bound-ms", type=int, default=300)
    parser.add_argument("--margins", type=int, nargs="+", default=[1000])
    parser.add_argument("--quarantine-ms", type=int, default=None, help="leave out ticks the replay would quarantine")
    parser.add_argument("--show", type=int, default=20)
    parser.add_argument("--out", help="write the JSON here")
    args = parser.parse_args(argv)

    clock = newest = None
    counts = {"ticks": 0, "quarantined": 0}
    without = Counter()
    late = {m: Counter() for m in args.margins}
    groups = {m: Counter() for m in args.margins}
    window_late = {m: [] for m in args.margins}
    for path in tick_files(args.ticks):
        for batch in pq.ParquetFile(path).iter_batches(
                batch_size=1_000_000, columns=["seq", "symbol", "exchange", "event_ts", "sys_ts"]):
            event = batch.column("event_ts")
            arrival = carry_max(pc.coalesce(batch.column("sys_ts"), event), clock)
            seen = carry_max(event, newest)
            before = pa.concat_arrays([pa.array([-(1 << 62) if newest is None else newest], pa.int64()), seen.slice(0, len(seen) - 1)])
            clock, newest = arrival[-1].as_py(), seen[-1].as_py()
            table = pa.table({"seq": batch.column("seq"), "symbol": batch.column("symbol"),
                              "exchange": batch.column("exchange"), "event_ts": event, "clock": arrival, "before": before})
            counts["ticks"] += len(table)
            if args.quarantine_ms is not None:
                held = pc.fill_null(pc.greater(pc.subtract(batch.column("sys_ts"), event), args.quarantine_ms), False)
                counts["quarantined"] += pc.sum(held).as_py() or 0
                table = table.filter(pc.invert(held))
            event, start = table["event_ts"], window_start(table["event_ts"])
            tick_watermark = pc.subtract(table["before"], args.bound_ms)
            behind_ticks = pc.less(event, tick_watermark)
            without["event_late"] += pc.sum(behind_ticks).as_py() or 0
            without["window_late"] += pc.sum(pc.less(start, window_start(tick_watermark))).as_py() or 0
            for m in args.margins:
                watermark = pc.subtract(table["clock"], m + args.bound_ms)
                behind = pc.less(event, watermark)
                late[m]["event_late"] += pc.sum(behind).as_py() or 0
                late[m]["event_late_only_with_heartbeats"] += pc.sum(pc.and_(behind, pc.invert(behind_ticks))).as_py() or 0
                for row in table.filter(behind).select(["exchange", "event_ts"]).to_pylist():
                    groups[m][f"{row['exchange']} {local_time(row['event_ts'])[:16]}"] += 1
                closed = pc.less(start, window_start(watermark))
                late[m]["window_late"] += pc.sum(closed).as_py() or 0
                window_late[m] += table.filter(closed).slice(0, args.show).to_pylist()

    result = {
        "params": {"ticks": args.ticks, "bound_ms": args.bound_ms, "quarantine_ms": args.quarantine_ms},
        **counts,
        "without_heartbeats": dict(without),
        "margins": {str(m): {**late[m], "event_late_by_exchange_minute": dict(groups[m].most_common(args.show)),
                             "window_late_examples": window_late[m][:args.show]} for m in args.margins},
    }
    text = json.dumps(result, indent=2)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(text)
    print(text)


if __name__ == "__main__":
    main()
