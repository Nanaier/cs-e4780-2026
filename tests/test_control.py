import unittest

from trading.core import quarantined
from trading.gateway import apply
from trading.subscribers import generate

HOUR = 3_600_000


class QuarantineTests(unittest.TestCase):
    def test_event_stamped_far_before_arrival_is_quarantined(self):
        self.assertTrue(quarantined(1_000, 1_000 + HOUR + 1, HOUR))

    def test_event_stamped_after_arrival_is_not_quarantined(self):
        self.assertFalse(quarantined(1_000 + 8 * HOUR, 1_000, HOUR))

    def test_whole_second_system_time_is_not_quarantined(self):
        self.assertFalse(quarantined(1_999, 1_000, HOUR))
        self.assertFalse(quarantined(1_000, 2_000, HOUR))


class SubscriptionTests(unittest.TestCase):
    def test_index_follows_subscribe_unsubscribe_and_tombstone(self):
        index = {}
        apply(index, "a|X", "subscribe")
        apply(index, "b|X", "subscribe")
        apply(index, "a|Y", "subscribe")
        apply(index, "a|X", "unsubscribe")
        apply(index, "a|Y", None)
        self.assertEqual(index, {"X": {"b"}})

    def test_generated_subscribers_have_distinct_symbols(self):
        counts = [("A", 1000), ("B", 10), ("C", 10), ("D", 1)]
        for mode in ("popular", "uniform"):
            plan = generate(counts, 50, 3, mode, 7, "s")
            self.assertEqual(len(plan), 50)
            self.assertTrue(all(len(set(chosen)) == 3 for chosen in plan.values()))

    def test_popular_mode_prefers_symbols_with_more_ticks(self):
        counts = [("A", 1000), ("B", 1), ("C", 1), ("D", 1)]
        plan = generate(counts, 200, 1, "popular", 7, "s")
        self.assertGreater(sum(chosen == ["A"] for chosen in plan.values()), 150)


if __name__ == "__main__":
    unittest.main()
