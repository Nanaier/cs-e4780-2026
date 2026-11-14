import argparse
import time

import pyarrow.parquet as pq
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from trading.core import HEARTBEAT, quarantined
from trading.files import default_metadata, metadata_values, tick_files


def ensure_topic(bootstrap, topic, partitions, config=None):
    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic in admin.list_topics(timeout=10).topics:
        return
    new = NewTopic(topic, num_partitions=partitions, replication_factor=1, config=config or {})
    for future in admin.create_topics([new]).values():
        future.result()


def produce(producer, topic, key, value, partition=None):
    while True:
        try:
            if partition is None:
                producer.produce(topic, key=key, value=value)
            else:
                producer.produce(topic, key=key, value=value, partition=partition)
            return
        except BufferError:
            producer.poll(0.05)


def load_partition_map(path, partitions):
    mapping = {}
    for line in open(path):
        symbol, partition = line.strip().split(",")
        if int(partition) >= partitions:
            raise SystemExit(f"{path}: partition {partition} for {symbol} exceeds {partitions} partitions")
        mapping[symbol] = int(partition)
    return mapping


class Heartbeat:
    def __init__(self, producer, topic, every_ms, margin_ms):
        self.producer = producer
        self.topic = topic
        self.every_ms = every_ms
        self.margin_ms = margin_ms
        deadline = time.time() + 30
        while not (meta := producer.list_topics(topic, timeout=10).topics.get(topic)) or meta.error or not meta.partitions:
            if time.time() > deadline:
                raise SystemExit(f"no partitions for {topic}")
            time.sleep(0.2)
        self.partitions = sorted(meta.partitions)
        self.clock = None
        self.sent = 0

    def emit(self, clock):
        value = f"{HEARTBEAT},H,0.0,{clock - self.margin_ms},0,{int(time.time() * 1000)}"
        for p in self.partitions:
            produce(self.producer, self.topic, HEARTBEAT, value, p)
        self.clock = clock
        self.sent += 1

    def advance(self, clock):
        if self.clock is None or clock - self.clock >= self.every_ms:
            self.emit(clock)

    def until(self, end, base_clock, base_virtual_ms, started, speed):
        while self.clock is not None and self.clock + self.every_ms <= end:
            clock = self.clock + self.every_ms
            if speed > 0:
                wait = started + (base_virtual_ms + clock - base_clock) / 1000 / speed - time.time()
                if wait > 0:
                    time.sleep(wait)
            self.emit(clock)


def publish_instruments(producer, topic, metadata):
    sent = 0
    for symbol, value in metadata_values(metadata):
        produce(producer, topic, symbol, value)
        sent += 1
    producer.flush()
    return sent


def main(argv=None):
    parser = argparse.ArgumentParser(description="Replay slimmed ticks into Kafka, keyed by symbol")
    parser.add_argument("--ticks", required=True, help="tick parquet file or directory")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="ticks")
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--speed", type=float, default=0.0, help="0 = as fast as possible; 60 = one hour of trading per minute")
    parser.add_argument("--max-gap-ms", type=int, default=60_000, help="compress quiet periods longer than this")
    parser.add_argument("--limit", type=int, default=0, help="stop after this many events (0 = all)")
    parser.add_argument("--quarantine-ms", type=int, default=None,
                        help="route ticks whose sys_ts is more than N ms after event_ts to --quarantine-topic")
    parser.add_argument("--quarantine-topic", default="quarantine")
    parser.add_argument("--instruments-topic", help="publish metadata rows keyed by symbol here before the ticks")
    parser.add_argument("--metadata", help="metadata parquet (default: metadata.parquet next to the ticks directory)")
    parser.add_argument("--partition-map", help="CSV symbol,partition; symbols not listed keep the CRC32 partitioner")
    parser.add_argument("--heartbeat-ms", type=int, default=0,
                        help="send a heartbeat to every partition each time the feed clock (largest sys_ts so far) "
                             "has advanced N ms, also while sleeping over quiet periods (0 = off)")
    parser.add_argument("--heartbeat-margin-ms", type=int, default=1000, help="heartbeat event time = feed clock - this")
    parser.add_argument("--heartbeat-tail-ms", type=int, default=0,
                        help="after the last tick, keep the feed clock and the heartbeats running this long")
    args = parser.parse_args(argv)
    if args.heartbeat_tail_ms and not args.heartbeat_ms:
        parser.error("--heartbeat-tail-ms needs --heartbeat-ms")
    partition_of = load_partition_map(args.partition_map, args.partitions) if args.partition_map else {}

    ensure_topic(args.bootstrap, args.topic, args.partitions)
    if args.quarantine_ms is not None:
        ensure_topic(args.bootstrap, args.quarantine_topic, args.partitions)
    if args.instruments_topic:
        ensure_topic(args.bootstrap, args.instruments_topic, args.partitions, {"cleanup.policy": "compact"})
    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "linger.ms": 5,
        "batch.size": 262_144,
        "compression.type": "lz4",
        "acks": "1",
        "queue.buffering.max.messages": 1_000_000,
    })

    if args.instruments_topic:
        metadata = args.metadata or default_metadata(args.ticks)
        print(f"instruments: {publish_instruments(producer, args.instruments_topic, metadata)} rows from {metadata} "
              f"to {args.instruments_topic}", flush=True)

    beat = Heartbeat(producer, args.topic, args.heartbeat_ms, args.heartbeat_margin_ms) if args.heartbeat_ms else None
    sent = held = 0
    started = time.time()
    virtual_ms = 0.0
    previous_ts = first_ts = None
    for path in tick_files(args.ticks):
        for batch in pq.ParquetFile(path).iter_batches(batch_size=65_536, columns=["symbol", "sec_type", "last", "event_ts", "seq", "sys_ts"]):
            cols = batch.to_pydict()
            for symbol, sec_type, last, event_ts, seq, sys_ts in zip(
                    cols["symbol"], cols["sec_type"], cols["last"], cols["event_ts"], cols["seq"], cols["sys_ts"]):
                arrival = sys_ts if sys_ts is not None else event_ts
                if args.speed > 0:
                    if previous_ts is not None and arrival > previous_ts:
                        step = min(arrival - previous_ts, args.max_gap_ms)
                        if beat:
                            beat.until(previous_ts + step - 1, previous_ts, virtual_ms, started, args.speed)
                        virtual_ms += step
                    wait = started + virtual_ms / 1000 / args.speed - time.time()
                    if wait > 0:
                        time.sleep(wait)
                if previous_ts is None:
                    previous_ts = first_ts = arrival
                else:
                    previous_ts = max(previous_ts, arrival)
                value = f"{symbol},{sec_type},{last!r},{event_ts},{seq},{int(time.time() * 1000)}"
                if args.quarantine_ms is not None and quarantined(event_ts, sys_ts, args.quarantine_ms):
                    produce(producer, args.quarantine_topic, symbol, f"{value},{sys_ts}")
                    held += 1
                else:
                    produce(producer, args.topic, symbol, value, partition_of.get(symbol))
                if beat:
                    beat.advance(previous_ts)
                sent += 1
                if sent % 100_000 == 0:
                    producer.poll(0)
                    print(f"{sent} events, {sent / (time.time() - started):,.0f}/s", flush=True)
                if args.limit and sent >= args.limit:
                    break
            if args.limit and sent >= args.limit:
                break
    if beat and args.heartbeat_tail_ms and previous_ts is not None:
        beat.until(previous_ts + args.heartbeat_tail_ms, previous_ts, virtual_ms, started, args.speed)
    producer.flush()
    elapsed = time.time() - started
    if args.speed > 0 and first_ts is not None:
        print(f"paced: feed {first_ts} at wall {started * 1000:.0f}, {args.speed}x, max gap {args.max_gap_ms} ms")
    if beat:
        print(f"heartbeats: {beat.sent} to each of {len(beat.partitions)} partitions, "
              f"last event time {beat.clock - beat.margin_ms}")
    if args.quarantine_ms is not None:
        print(f"routed: {args.topic}={sent - held} {args.quarantine_topic}={held}")
    print(f"done: {sent} events in {elapsed:.1f} s ({sent / elapsed:,.0f}/s)")


if __name__ == "__main__":
    main()
