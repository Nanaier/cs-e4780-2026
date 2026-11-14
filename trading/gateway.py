import argparse
import json
import time
import uuid
from collections import defaultdict
from pathlib import Path

from confluent_kafka import OFFSET_BEGINNING, OFFSET_END, Consumer, TopicPartition

from trading.files import percentiles
from trading.topics import read_table

POINTS = (50, 95, 99, 99.9)


def apply(index, key, value):
    subscriber, bar, symbol = key.partition("|")
    if not bar:
        return
    if value == "subscribe":
        index.setdefault(symbol, set()).add(subscriber)
        return
    subscribers = index.get(symbol)
    if subscribers is not None:
        subscribers.discard(subscriber)
        if not subscribers:
            del index[symbol]


def partitions(consumer, topic, offset):
    meta = consumer.list_topics(topic, timeout=10).topics.get(topic)
    if meta is None or meta.error is not None:
        raise SystemExit(f"topic {topic} not found")
    return [TopicPartition(topic, p, offset if isinstance(offset, int) else offset[p]) for p in meta.partitions]


def main(argv=None):
    parser = argparse.ArgumentParser(description="Fan results out to subscribers: inverted index symbol -> subscribers")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--ema-topic", default="ema")
    parser.add_argument("--advisories-topic", default="advisories")
    parser.add_argument("--subscriptions-topic", default="subscriptions")
    parser.add_argument("--from-start", action="store_true", help="read the result topics from the beginning")
    parser.add_argument("--idle-exit-s", type=float, default=10.0, help="stop this long after the last result")
    parser.add_argument("--max-wait-s", type=float, default=3600.0, help="stop if no result arrives within this time")
    parser.add_argument("--sample", type=int, default=0, help="append the deliveries of the first N subscribers to files")
    parser.add_argument("--sample-dir", default="out/gateway")
    parser.add_argument("--out", help="write the metrics JSON here")
    args = parser.parse_args(argv)

    table, ends = read_table(args.bootstrap, args.subscriptions_topic)
    index = {}
    for key, value in table.items():
        apply(index, key, value)
    subscribers = {s for subs in index.values() for s in subs}
    initial = {"subscribers": len(subscribers), "subscriptions": sum(len(s) for s in index.values()),
               "symbols_subscribed": len(index)}
    sample = set(sorted(subscribers)[:args.sample])
    files = {}
    if sample:
        Path(args.sample_dir).mkdir(parents=True, exist_ok=True)
        files = {s: Path(args.sample_dir, f"{s}.csv").open("w") for s in sample}

    consumer = Consumer({"bootstrap.servers": args.bootstrap, "group.id": f"gateway-{uuid.uuid4().hex[:8]}",
                         "enable.auto.commit": False})
    start = OFFSET_BEGINNING if args.from_start else OFFSET_END
    consumer.assign(partitions(consumer, args.ema_topic, start) + partitions(consumer, args.advisories_topic, start)
                    + partitions(consumer, args.subscriptions_topic, ends))

    delivered = defaultdict(int)
    counts = {"ema": 0, "advisories": 0, "delivered_rows": 0, "deliveries": 0, "subscription_updates": 0}
    delays = []
    busy = 0.0
    first = last = seen = None
    started = time.time()
    try:
        while True:
            messages = consumer.consume(num_messages=10_000, timeout=0.5)
            if not messages:
                now = time.time()
                if (seen and now - seen > args.idle_exit_s) or (not seen and now - started > args.max_wait_s):
                    break
                continue
            batch_started = time.perf_counter()
            for message in messages:
                if message.error():
                    continue
                topic = message.topic()
                if topic == args.subscriptions_topic:
                    value = message.value()
                    apply(index, message.key().decode(), None if value is None else value.decode())
                    counts["subscription_updates"] += 1
                    continue
                line = message.value().decode()
                seen = time.time()
                counts["ema" if topic == args.ema_topic else "advisories"] += 1
                subs = index.get(line[:line.index(",")])
                if not subs:
                    continue
                for s in subs:
                    delivered[s] += 1
                for s in sample.intersection(subs):
                    files[s].write(f"{topic},{line}\n")
                counts["deliveries"] += len(subs)
                counts["delivered_rows"] += 1
                now = time.time()
                delays.append(int(now * 1000) - int(line[line.rindex(",") + 1:]))
                first = first or now
                last = now
            busy += time.perf_counter() - batch_started
    finally:
        consumer.close()
        for fh in files.values():
            fh.close()

    span = (last - first) if first else 0.0
    metrics = {
        "subscriptions_topic": args.subscriptions_topic,
        **initial,
        "counts": counts,
        "delivery_span_s": round(span, 3),
        "busy_s": round(busy, 3),
        "deliveries_per_s": round(counts["deliveries"] / span) if span else None,
        "deliveries_per_busy_s": round(counts["deliveries"] / busy) if busy else None,
        "deliveries_per_subscriber": percentiles([delivered.get(s, 0) for s in subscribers | set(delivered)], POINTS),
        "delay_ms_emit_to_delivery": percentiles(delays, POINTS),
        "sample": sorted(sample),
    }
    text = json.dumps(metrics, indent=2)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(text)
    print(text)


if __name__ == "__main__":
    main()
