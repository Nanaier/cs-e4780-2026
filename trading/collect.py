import argparse
import json
import time
import uuid
from pathlib import Path

from confluent_kafka import Consumer

from trading.files import ResultWriter, percentiles


def main(argv=None):
    parser = argparse.ArgumentParser(description="Read the ema and advisories topics into CSV files and measure latency")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--ema-topic", default="ema")
    parser.add_argument("--advisories-topic", default="advisories")
    parser.add_argument("--out", default="out/collected")
    parser.add_argument("--from-start", action="store_true", help="read the topics from the beginning")
    parser.add_argument("--idle-exit-s", type=float, default=10.0)
    parser.add_argument("--isolation", choices=["read_committed", "read_uncommitted"], default="read_committed",
                        help="isolation.level; read_committed is librdkafka's default")
    parser.add_argument("--dedupe", action="store_true",
                        help="keep the first row per (topic, symbol, window_end) and count the dropped duplicates")
    parser.add_argument("--enriched", action="store_true", help="advisory lines carry isin,currency (engines' --enrich)")
    args = parser.parse_args(argv)

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": f"collector-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest" if args.from_start else "latest",
        "enable.auto.commit": False,
        "isolation.level": args.isolation,
    })
    consumer.subscribe([args.ema_topic, args.advisories_topic])
    writer = ResultWriter(args.out, args.enriched)
    advisory_end = 10 if args.enriched else 8
    end_to_end, in_job = [], []
    counts = {"ema": 0, "advisories": 0}
    kept = {"ema": {}, "advisories": {}}
    duplicates = {"ema": 0, "advisories": 0}
    differing = {"ema": 0, "advisories": 0}
    examples = []
    last_message = None
    started = time.time()
    try:
        while True:
            messages = consumer.consume(num_messages=10_000, timeout=0.5)
            now_ms = int(time.time() * 1000)
            if not messages:
                idle_since = last_message or started
                if time.time() - idle_since > args.idle_exit_s:
                    break
                continue
            last_message = time.time()
            for message in messages:
                if message.error():
                    continue
                f = message.value().decode().split(",")
                kind = "ema" if message.topic() == args.ema_topic else "advisories"
                if args.dedupe:
                    key, values = f"{f[0]},{f[1]}", ",".join(f[3:6] if kind == "ema" else f[3:advisory_end])
                    first = kept[kind].setdefault(key, values)
                    if first is not values:
                        duplicates[kind] += 1
                        if first != values:
                            differing[kind] += 1
                            if len(examples) < 5:
                                examples.append({"topic": kind, "key": key, "kept": first, "dropped": values})
                        continue
                if kind == "ema":
                    newest_ingest_ms, emit_ms = int(f[-2]), int(f[-1])
                    end_to_end.append(now_ms - newest_ingest_ms)
                    in_job.append(emit_ms - newest_ingest_ms)
                    counts["ema"] += 1
                    writer.write_ema(f[0], int(f[1]), float(f[3]), float(f[4]), float(f[5]))
                else:
                    counts["advisories"] += 1
                    writer.write_advisory(f[0], int(f[1]), f[3], float(f[4]), float(f[5]), float(f[6]), float(f[7]),
                                          f[8:10] if args.enriched else None)
    finally:
        consumer.close()
        writer.close()
    metrics = {
        "isolation": args.isolation,
        "counts": counts,
        "latency_ms_ingest_to_collected": percentiles(end_to_end),
        "latency_ms_ingest_to_emitted": percentiles(in_job),
    }
    if args.dedupe:
        metrics.update({"duplicates_dropped": duplicates, "duplicates_differing": differing,
                        "differing_examples": examples})
    Path(args.out, "metrics.json").write_text(json.dumps(metrics, indent=2))
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
