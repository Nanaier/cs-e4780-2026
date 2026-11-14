import argparse
import time
import uuid

from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic


def create(bootstrap, topics, partitions, retention_ms=None, compact=False):
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=10).topics
    config = {} if retention_ms is None else {"retention.ms": str(retention_ms)}
    if compact:
        config["cleanup.policy"] = "compact"
    new = [NewTopic(t, num_partitions=partitions, replication_factor=1, config=config) for t in topics if t not in existing]
    for future in (admin.create_topics(new).values() if new else []):
        future.result()
    deadline = time.time() + 30
    while new and time.time() < deadline:
        meta = admin.list_topics(timeout=10).topics
        if all(t.topic in meta and meta[t.topic].partitions
               and all(p.leader >= 0 for p in meta[t.topic].partitions.values()) for t in new):
            break
        time.sleep(0.2)


def delete(bootstrap, topics):
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=10).topics
    doomed = [t for t in topics if t in existing]
    for future in (admin.delete_topics(doomed).values() if doomed else []):
        future.result()


def count(bootstrap, topic, isolation="read_committed"):
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "topic-count", "isolation.level": isolation})
    meta = consumer.list_topics(topic, timeout=10).topics.get(topic)
    if meta is None or meta.error is not None:
        consumer.close()
        return None
    total = 0
    for p in meta.partitions:
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        total += high - low
    consumer.close()
    return total


def read_table(bootstrap, topic):
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": f"table-{uuid.uuid4().hex[:8]}",
                         "enable.auto.commit": False})
    meta = consumer.list_topics(topic, timeout=10).topics.get(topic)
    if meta is None or meta.error is not None:
        consumer.close()
        raise SystemExit(f"topic {topic} not found")
    ends, pending = {}, set()
    for p in meta.partitions:
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        ends[p] = high
        if high > low:
            pending.add(p)
    table = {}
    consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in pending])
    while pending:
        for message in consumer.consume(10_000, 1.0):
            if message.error() or message.partition() not in pending:
                continue
            if message.key() is not None:
                key = message.key().decode()
                if message.value() is None:
                    table.pop(key, None)
                else:
                    table[key] = message.value().decode()
            if message.offset() + 1 >= ends[message.partition()]:
                pending.discard(message.partition())
    consumer.close()
    return table, ends


def main(argv=None):
    parser = argparse.ArgumentParser(description="Create, delete or count Kafka topics")
    parser.add_argument("action", choices=["create", "delete", "count"])
    parser.add_argument("topics", nargs="+")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--retention-ms", type=int, default=None, help="-1 keeps messages until the topic is deleted")
    parser.add_argument("--compact", action="store_true", help="cleanup.policy=compact: keep the latest value per key")
    parser.add_argument("--isolation", choices=["read_committed", "read_uncommitted"], default="read_committed",
                        help="count: read_committed ends at the last stable offset, read_uncommitted at the high watermark")
    args = parser.parse_args(argv)
    if args.action == "create":
        create(args.bootstrap, args.topics, args.partitions, args.retention_ms, args.compact)
    elif args.action == "delete":
        delete(args.bootstrap, args.topics)
    else:
        for topic in args.topics:
            print(f"{topic} {count(args.bootstrap, topic, args.isolation)}")


if __name__ == "__main__":
    main()
