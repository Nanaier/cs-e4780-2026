import argparse
import csv
import json
import signal
import time
import uuid
from collections import Counter, defaultdict
from pathlib import Path

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, TopicPartition

from trading.core import HEARTBEAT, METADATA
from trading.files import percentiles

WINDOW_MS = 300_000
POINTS = (50, 95, 99, 99.9)
HEARTBEAT_KEY = HEARTBEAT.encode()
METADATA_KEY = METADATA.encode()


def read_topic(bootstrap, topic, handle, isolation="read_committed"):
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"latency-{uuid.uuid4().hex[:8]}",
        "enable.auto.commit": False,
        "enable.partition.eof": True,
        "isolation.level": isolation,
    })
    meta = consumer.list_topics(topic, timeout=10).topics.get(topic)
    if meta is None or meta.error is not None:
        consumer.close()
        raise SystemExit(f"topic {topic} not found")
    ends = {}
    for p in sorted(meta.partitions):
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        if high > low:
            ends[p] = high
    pending = set(ends)
    consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in pending])
    while pending:
        for message in consumer.consume(10_000, 1.0):
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    pending.discard(message.partition())
                continue
            if message.partition() in pending:
                handle(message.partition(), message.value())
                if message.offset() + 1 >= ends[message.partition()]:
                    pending.discard(message.partition())
    consumer.close()
    return sorted(ends), sorted(set(meta.partitions) - set(ends))


def follow(bootstrap, topic, isolation, path):
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"latency-follow-{uuid.uuid4().hex[:8]}",
        "enable.auto.commit": False,
        "enable.partition.eof": True,
        "isolation.level": isolation,
        "fetch.wait.max.ms": 10,
    })
    meta = consumer.list_topics(topic, timeout=10).topics.get(topic)
    if meta is None or meta.error is not None:
        consumer.close()
        raise SystemExit(f"topic {topic} not found")
    partitions = set(meta.partitions)
    consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in partitions])
    stopping = []
    signal.signal(signal.SIGTERM, lambda *_: stopping.append(True))
    at_end = set()
    with Path(path).open("w", newline="") as fh:
        out = csv.writer(fh)
        out.writerow(["symbol", "window_end_ms", "received_ms"])
        while not (stopping and at_end == partitions):
            messages = consumer.consume(10_000, 0.005)
            received_ms = int(time.time() * 1000)
            for message in messages:
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        at_end.add(message.partition())
                    continue
                at_end.discard(message.partition())
                fields = message.value().split(b",", 2)
                out.writerow([fields[0].decode(), int(fields[1]), received_ms])
    consumer.close()


def load_received(path):
    received = {}
    with Path(path).open() as fh:
        for row in csv.DictReader(fh):
            key = (row["symbol"], int(row["window_end_ms"]))
            received[key] = min(received.get(key, 1 << 62), int(row["received_ms"]))
    return received


class Closable:
    def __init__(self, bound_ms):
        self.bound_ms = bound_ms
        self.top = {}
        self.first = {}
        self.crossing = defaultdict(dict)
        self.ticks_per_symbol = Counter()
        self.heartbeats = 0

    def tick(self, partition, value):
        fields = value.split(b",")
        if fields[1] == METADATA_KEY:
            return
        if fields[0] == HEARTBEAT_KEY:
            self.heartbeats += 1
        else:
            self.ticks_per_symbol[fields[0]] += 1
        event_ts, ingest_ms = int(fields[3]), int(fields[5])
        top = self.top.get(partition)
        if top is not None and event_ts <= top:
            return
        high = (event_ts - self.bound_ms) // WINDOW_MS
        if top is None:
            self.first[partition] = (high, ingest_ms)
        else:
            crossing = self.crossing[partition]
            for k in range((top - self.bound_ms) // WINDOW_MS + 1, high + 1):
                crossing[k] = ingest_ms
        self.top[partition] = event_ts

    def at(self, partitions, window_end):
        k = window_end // WINDOW_MS
        times = []
        for p in partitions:
            first_k, first_ingest = self.first[p]
            if k <= first_k:
                times.append(first_ingest)
            elif k in self.crossing[p]:
                times.append(self.crossing[p][k])
            else:
                return None
        return max(times)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Latency from the moment each window could first close: the first tick or heartbeat past "
                    "window end + bound on every partition of the ticks topic")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--ticks-topic")
    parser.add_argument("--results-topic", required=True, help="EMA topic written by either engine")
    parser.add_argument("--bound-ms", type=int, help="the engine's out-of-orderness bound")
    parser.add_argument("--hot", type=int, default=100, help="symbols with the most ticks counted as hot")
    parser.add_argument("--out", help="write the metrics JSON here")
    parser.add_argument("--isolation", choices=["read_committed", "read_uncommitted"], default="read_committed",
                        help="isolation.level for the results topic; read_committed is librdkafka's default")
    parser.add_argument("--follow", action="store_true",
                        help="only follow the results topic live and write each row's receive time to --received "
                             "until SIGTERM, then read to the end and exit")
    parser.add_argument("--received", help="receive times written by --follow; adds latency to the receive time")
    args = parser.parse_args(argv)
    if args.follow:
        if not args.received:
            parser.error("--follow needs --received")
        follow(args.bootstrap, args.results_topic, args.isolation, args.received)
        return
    if args.ticks_topic is None or args.bound_ms is None:
        parser.error("--ticks-topic and --bound-ms are required")
    received = load_received(args.received) if args.received else None

    closable = Closable(args.bound_ms)
    partitions, empty = read_topic(args.bootstrap, args.ticks_topic, closable.tick)
    hot = {s.decode() for s, _ in closable.ticks_per_symbol.most_common(args.hot)}

    cache = {}
    overall, hot_rows, tail_rows, reported, to_received = [], [], [], [], []
    emits = defaultdict(list)
    counts = {"rows": 0, "never_closable": 0, "emitted_before_closable": 0}

    def result(_, value):
        fields = value.decode().split(",")
        symbol, window_end, stamp, emit = fields[0], int(fields[1]), int(fields[-2]), int(fields[-1])
        counts["rows"] += 1
        reported.append(emit - stamp)
        if window_end not in cache:
            cache[window_end] = closable.at(partitions, window_end)
        start = cache[window_end]
        if start is None:
            counts["never_closable"] += 1
            return
        latency = emit - start
        if latency < 0:
            counts["emitted_before_closable"] += 1
        overall.append(latency)
        if received is not None:
            seen = received.get((symbol, window_end))
            if seen is None:
                counts["not_received"] = counts.get("not_received", 0) + 1
            else:
                to_received.append(seen - start)
        (hot_rows if symbol in hot else tail_rows).append(latency)
        emits[window_end].append(emit)

    read_topic(args.bootstrap, args.results_topic, result, args.isolation)
    windows = [(max(e) - cache[w], max(e) - min(e)) for w, e in emits.items()]
    metrics = {
        "params": {"ticks_topic": args.ticks_topic, "results_topic": args.results_topic,
                   "bound_ms": args.bound_ms, "hot_symbols": args.hot, "isolation": args.isolation},
        "ticks": sum(closable.ticks_per_symbol.values()),
        "partitions": len(partitions),
        "empty_partitions": empty,
        "counts": counts,
        "windows": len(windows),
        "latency_ms_from_closable": percentiles(overall, POINTS),
        "latency_ms_from_closable_hot": percentiles(hot_rows, POINTS),
        "latency_ms_from_closable_tail": percentiles(tail_rows, POINTS),
        "window_complete_ms": percentiles([w[0] for w in windows], POINTS),
        "window_emit_spread_ms": percentiles([w[1] for w in windows], POINTS),
        "latency_ms_engine_stamp": percentiles(reported, POINTS),
    }
    if received is not None:
        metrics["latency_ms_from_closable_received"] = percentiles(to_received, POINTS)
    if closable.heartbeats:
        metrics["heartbeats"] = closable.heartbeats
    text = json.dumps(metrics, indent=2)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(text)
    print(text)


if __name__ == "__main__":
    main()
