import argparse
import json
import sys
import time
from pathlib import Path

from trading.core import HEARTBEAT, METADATA, EmaCrossoverOperator, InstrumentVersions, Params, quarantined
from trading.files import TICK_COLUMNS, ResultWriter, iter_ticks, load_subscriptions, local_time, percentiles, tick_files


class KafkaResults:
    def __init__(self, bootstrap, ema_topic, advisories_topic, instruments=None, bound_ms=0):
        from confluent_kafka import Producer

        self.producer = Producer({
            "bootstrap.servers": bootstrap,
            "linger.ms": 5,
            "compression.type": "lz4",
            "acks": "1",
            "queue.buffering.max.messages": 1_000_000,
        })
        self.ema_topic = ema_topic
        self.advisories_topic = advisories_topic
        self.instruments = instruments
        self.bound_ms = bound_ms
        self.newest_ingest_ms = 0

    def _send(self, topic, symbol, fields):
        line = ",".join([symbol, *fields, str(self.newest_ingest_ms), str(int(time.time() * 1000))])
        while True:
            try:
                self.producer.produce(topic, key=symbol, value=line)
                return
            except BufferError:
                self.producer.poll(0.05)

    def write_ema(self, symbol, window_end, close, ema38, ema100):
        self._send(self.ema_topic, symbol, [str(window_end), local_time(window_end), repr(close), repr(ema38), repr(ema100)])

    def write_advisory(self, symbol, window_end, kind, ema38, ema100, prev38, prev100):
        fields = [str(window_end), local_time(window_end), kind, repr(ema38), repr(ema100), repr(prev38), repr(prev100)]
        if self.instruments is not None:
            fields += self.instruments.as_of(symbol, window_end + self.bound_ms)
        self._send(self.advisories_topic, symbol, fields)

    def poll(self):
        self.producer.poll(0)

    def close(self):
        self.producer.flush()


def build_operator(args, writer, latencies, now):
    def on_ema(row, trigger):
        writer.write_ema(row.symbol, row.window_end, row.close, row.ema38, row.ema100)
        if trigger is not None:
            latencies.append(now() - trigger)

    def on_advisory(adv, trigger):
        writer.write_advisory(adv.symbol, adv.window_end, adv.kind, adv.ema38, adv.ema100, adv.prev_ema38, adv.prev_ema100)

    params = Params(
        out_of_orderness_ms=args.out_of_orderness_ms,
        empty_window=args.empty_window,
        suppress_first=args.suppress_first,
    )
    return EmaCrossoverOperator(params, load_subscriptions(args.subscriptions), on_ema, on_advisory)


def run_files(args, op):
    files = tick_files(args.ticks)
    if not files:
        sys.exit(f"no tick files under {args.ticks}")
    clock = time.perf_counter_ns
    if args.quarantine_ms is None:
        for symbol, last, event_ts, seq in iter_ticks(files, args.batch_size):
            op.process(symbol, last, event_ts, seq, clock())
        op.flush(None)
        return None
    held = 0
    for symbol, last, event_ts, seq, sys_ts in iter_ticks(files, args.batch_size, TICK_COLUMNS + ("sys_ts",)):
        if quarantined(event_ts, sys_ts, args.quarantine_ms):
            held += 1
            continue
        op.process(symbol, last, event_ts, seq, clock())
    op.flush(None)
    return held


def partition_watermark(partition_max, partition_seen, assigned, idle_ms, now_ms):
    active = [partition_max[p] for p in assigned
              if p in partition_max and now_ms - partition_seen[p] <= idle_ms]
    waiting = [p for p in assigned if p not in partition_max]
    if not active or (waiting and now_ms - min(partition_seen.values(), default=now_ms) <= idle_ms):
        return None
    return min(active)


def run_kafka(args, op, results):
    from confluent_kafka import Consumer

    consumer = Consumer({
        "bootstrap.servers": args.kafka,
        "group.id": args.group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([args.topic])
    partition_max, partition_seen = {}, {}
    first_message = last_message = None
    try:
        while True:
            messages = consumer.consume(num_messages=10_000, timeout=0.5)
            now_ms = int(time.time() * 1000)
            if not messages:
                if last_message is not None and time.time() - last_message > args.idle_exit_s:
                    break
                continue
            last_message = time.time()
            first_message = first_message or last_message
            trigger = None
            for message in messages:
                if message.error():
                    continue
                fields = message.value().decode().split(",")
                if fields[1] == METADATA:
                    if results.instruments is not None:
                        results.instruments.add(fields[0], int(fields[3]), fields[6], fields[7])
                    continue
                symbol, _, last, event_ts, seq, ingest_ms = fields
                event_ts = int(event_ts)
                p = message.partition()
                if event_ts > partition_max.get(p, -1):
                    partition_max[p] = event_ts
                partition_seen[p] = now_ms
                if symbol == HEARTBEAT:
                    continue
                op.ingest(symbol, float(last), event_ts, int(seq))
                ingest_ms = int(ingest_ms)
                trigger = ingest_ms if trigger is None else max(trigger, ingest_ms)
            if trigger is not None:
                results.newest_ingest_ms = max(results.newest_ingest_ms, trigger)
            assigned = [tp.partition for tp in consumer.assignment()]
            watermark = partition_watermark(partition_max, partition_seen, assigned, args.idle_partition_ms, now_ms)
            if watermark is not None:
                op.advance(watermark - args.out_of_orderness_ms, trigger)
            results.poll()
        op.flush(None)
    finally:
        consumer.close()
    return (last_message - first_message) if first_message else 0.0


def main(argv=None):
    parser = argparse.ArgumentParser(description="Streaming EMA38/EMA100 crossover engine (Python prototype)")
    parser.add_argument("--ticks", help="tick parquet file or directory (file replay mode)")
    parser.add_argument("--kafka", help="bootstrap servers, e.g. localhost:9092 (Kafka mode)")
    parser.add_argument("--topic", default="ticks")
    parser.add_argument("--group", default="engine-python")
    parser.add_argument("--idle-exit-s", type=float, default=10.0)
    parser.add_argument("--idle-partition-ms", type=int, default=2_000,
                        help="a partition silent this long stops holding back the watermark")
    parser.add_argument("--ema-topic", default="ema", help="Kafka mode: topic for EMA rows")
    parser.add_argument("--advisories-topic", default="advisories", help="Kafka mode: topic for advisories")
    parser.add_argument("--enrich", action="store_true",
                        help="Kafka mode: add isin,currency to advisories from the in-band metadata rows, "
                             "as known when the window could first close")
    parser.add_argument("--out", default="out/stream")
    parser.add_argument("--subscriptions", default="all", help="file with one symbol per line, or 'all'")
    parser.add_argument("--out-of-orderness-ms", type=int, default=0)
    parser.add_argument("--empty-window", choices=["skip", "repeat"], default="skip")
    parser.add_argument("--suppress-first", action="store_true", help="no advisory from a symbol's first window")
    parser.add_argument("--batch-size", type=int, default=65_536)
    parser.add_argument("--quarantine-ms", type=int, default=None,
                        help="file mode: hold back ticks whose sys_ts is more than N ms after event_ts")
    args = parser.parse_args(argv)
    if bool(args.ticks) == bool(args.kafka):
        parser.error("give exactly one of --ticks or --kafka")
    if args.enrich and not args.kafka:
        parser.error("--enrich needs --kafka")

    Path(args.out).mkdir(parents=True, exist_ok=True)
    if args.kafka:
        instruments = InstrumentVersions() if args.enrich else None
        writer = KafkaResults(args.kafka, args.ema_topic, args.advisories_topic, instruments, args.out_of_orderness_ms)
    else:
        writer = ResultWriter(args.out)
    latencies = []
    if args.kafka:
        now = lambda: int(time.time() * 1000)
        unit = "ms, Kafka ingest to result"
    else:
        now = time.perf_counter_ns
        unit = "ns, read to result"
    op = build_operator(args, writer, latencies, now)

    started = time.perf_counter()
    held = None
    if args.kafka:
        elapsed = run_kafka(args, op, writer)
    else:
        held = run_files(args, op)
        elapsed = time.perf_counter() - started
    writer.close()

    if not args.kafka:
        latencies = [v / 1_000_000 for v in latencies]
    metrics = {
        "source": args.kafka or args.ticks,
        "params": {k: getattr(args, k) for k in ("out_of_orderness_ms", "empty_window", "suppress_first", "subscriptions")},
        "counts": op.counts,
        "symbols": len(op.state),
        "wall_seconds": round(elapsed, 3),
        "events_per_second": round(op.counts["events"] / elapsed) if elapsed else None,
        "latency_unit": unit.replace("ns", "ms"),
        "latency_ms": percentiles(latencies),
    }
    if held is not None:
        metrics["quarantined"] = held
    Path(args.out, "metrics.json").write_text(json.dumps(metrics, indent=2))
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
