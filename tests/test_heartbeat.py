import time
import unittest
from types import SimpleNamespace

from trading.latency import Closable
from trading.replay import Heartbeat


class FakeProducer:
    def __init__(self, partitions):
        self.partitions = partitions
        self.sent = []

    def list_topics(self, topic, timeout):
        return SimpleNamespace(topics={topic: SimpleNamespace(error=None, partitions=dict.fromkeys(range(self.partitions)))})

    def produce(self, topic, key, value, partition=None):
        self.sent.append((partition, value))

    def poll(self, timeout):
        pass


def event_times(producer):
    return [int(value.split(",")[3]) for _, value in producer.sent]


class HeartbeatTests(unittest.TestCase):
    def test_every_partition_gets_feed_clock_minus_margin(self):
        producer = FakeProducer(3)
        beat = Heartbeat(producer, "t", 1_000, 1_000)
        beat.advance(5_000)
        self.assertEqual([p for p, _ in producer.sent], [0, 1, 2])
        self.assertTrue(all(v.startswith("__heartbeat__,H,0.0,4000,0,") for _, v in producer.sent))

    def test_sent_when_clock_advanced_by_interval(self):
        producer = FakeProducer(1)
        beat = Heartbeat(producer, "t", 1_000, 1_000)
        for clock in (5_000, 5_500, 5_999, 6_000, 6_200, 8_000):
            beat.advance(clock)
        self.assertEqual(event_times(producer), [4_000, 5_000, 7_000])

    def test_quiet_period_stays_below_next_arrival(self):
        producer = FakeProducer(1)
        beat = Heartbeat(producer, "t", 1_000, 1_000)
        beat.advance(5_000)
        beat.until(9_000 - 1, 5_000, 0.0, time.time(), 0)
        self.assertEqual(event_times(producer), [4_000, 5_000, 6_000, 7_000])


class ClosableTests(unittest.TestCase):
    def test_heartbeat_closes_window_but_is_not_a_tick(self):
        closable = Closable(300)
        closable.tick(0, b"X,E,1.0,1000,1,10")
        closable.tick(0, b"__heartbeat__,H,0.0,300400,0,20")
        self.assertEqual(closable.at([0], 300_000), 20)
        self.assertEqual(dict(closable.ticks_per_symbol), {b"X": 1})
        self.assertEqual(closable.heartbeats, 1)


if __name__ == "__main__":
    unittest.main()
