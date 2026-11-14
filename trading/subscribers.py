import argparse
import itertools
import json
import random
from pathlib import Path

import duckdb
from confluent_kafka import Producer

from trading import topics
from trading.files import tick_files


def tick_counts(ticks):
    files = [str(f) for f in tick_files(ticks)]
    return duckdb.connect().execute(
        "SELECT symbol, count(*) FROM read_parquet(?) GROUP BY symbol ORDER BY symbol", [files]).fetchall()


def generate(counts, subscribers, per_subscriber, mode, seed, prefix):
    symbols = [s for s, _ in counts]
    if per_subscriber > len(symbols):
        raise SystemExit(f"only {len(symbols)} symbols, cannot pick {per_subscriber} per subscriber")
    weights = list(itertools.accumulate(n for _, n in counts))
    rng = random.Random(seed)
    width = len(str(subscribers))
    plan = {}
    for i in range(1, subscribers + 1):
        if mode == "uniform":
            chosen = set(rng.sample(symbols, per_subscriber))
        else:
            chosen = set()
            while len(chosen) < per_subscriber:
                chosen.update(rng.choices(symbols, cum_weights=weights, k=per_subscriber - len(chosen)))
        plan[f"{prefix}{i:0{width}d}"] = sorted(chosen)
    return plan


def main(argv=None):
    parser = argparse.ArgumentParser(description="Generate N subscribers with K symbols each into a compacted topic")
    parser.add_argument("--ticks", required=True, help="tick parquet file or directory: the symbol universe and popularity")
    parser.add_argument("--subscribers", "-n", type=int, required=True)
    parser.add_argument("--symbols", "-k", type=int, default=20, help="symbols per subscriber")
    parser.add_argument("--mode", choices=["popular", "uniform"], default="popular",
                        help="popular: weighted by tick count; uniform: every symbol equally likely")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--prefix", default="sub")
    parser.add_argument("--action", choices=["subscribe", "unsubscribe", "tombstone"], default="subscribe")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="subscriptions")
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--symbols-out", help="write the union of subscribed symbols here, one per line")
    parser.add_argument("--dry-run", action="store_true", help="do not produce, only print the summary")
    args = parser.parse_args(argv)

    counts = tick_counts(args.ticks)
    plan = generate(counts, args.subscribers, args.symbols, args.mode, args.seed, args.prefix)
    union = sorted({s for chosen in plan.values() for s in chosen})
    ticks = dict(counts)
    if args.symbols_out:
        Path(args.symbols_out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.symbols_out).write_text("".join(f"{s}\n" for s in union))
    if not args.dry_run:
        topics.create(args.bootstrap, [args.topic], args.partitions, compact=True)
        producer = Producer({"bootstrap.servers": args.bootstrap, "linger.ms": 5, "acks": "all"})
        value = None if args.action == "tombstone" else args.action
        for subscriber, chosen in plan.items():
            for symbol in chosen:
                while True:
                    try:
                        producer.produce(args.topic, key=f"{subscriber}|{symbol}", value=value)
                        break
                    except BufferError:
                        producer.poll(0.05)
        producer.flush()
    print(json.dumps({
        "topic": None if args.dry_run else args.topic,
        "action": args.action,
        "mode": args.mode,
        "subscribers": len(plan),
        "symbols_per_subscriber": args.symbols,
        "pairs": sum(len(c) for c in plan.values()),
        "symbols_in_data": len(counts),
        "union_symbols": len(union),
        "union_tick_share": round(sum(ticks[s] for s in union) / sum(ticks.values()), 4),
    }, indent=2))


if __name__ == "__main__":
    main()
