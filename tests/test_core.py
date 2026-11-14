import unittest

from trading.core import EmaCrossoverOperator, Params, alpha

W = 300_000
DAY = 1_636_329_600_000


def run(events, **params):
    emas, advisories = [], []
    op = EmaCrossoverOperator(
        Params(**params),
        on_ema=lambda row, trigger: emas.append(row),
        on_advisory=lambda adv, trigger: advisories.append(adv),
    )
    for seq, (symbol, price, ts) in enumerate(events):
        op.process(symbol, price, ts, seq)
    op.flush()
    return op, emas, advisories


class EmaTests(unittest.TestCase):
    def test_ema_follows_formula_over_three_windows(self):
        _, emas, _ = run([("X", 10.0, DAY + 1_000), ("X", 12.0, DAY + W + 1_000), ("X", 8.0, DAY + 2 * W + 1_000)])
        a, b = alpha(38), alpha(100)
        e38 = e100 = 0.0
        for row, close in zip(emas, [10.0, 12.0, 8.0]):
            e38 = close * a + e38 * (1 - a)
            e100 = close * b + e100 * (1 - b)
            self.assertEqual(row.ema38, e38)
            self.assertEqual(row.ema100, e100)
        self.assertEqual([r.window_end for r in emas], [DAY + W, DAY + 2 * W, DAY + 3 * W])

    def test_close_is_last_event_by_event_time(self):
        _, emas, _ = run([("X", 1.0, DAY + 5_000), ("X", 2.0, DAY + 9_000), ("X", 3.0, DAY + 7_000)], out_of_orderness_ms=10_000)
        self.assertEqual(emas[0].close, 2.0)

    def test_window_closes_when_next_window_starts(self):
        closed = []
        op = EmaCrossoverOperator(Params(), on_ema=lambda row, trigger: closed.append(row.window_end))
        op.process("X", 10.0, DAY + 1_000, 0)
        self.assertEqual(closed, [])
        op.process("Y", 11.0, DAY + W, 1)
        self.assertEqual(closed, [DAY + W])

    def test_first_window_always_signals_buy(self):
        op, _, advisories = run([("X", 50.0, DAY + 1_000)])
        self.assertEqual([a.kind for a in advisories], ["BUY"])
        self.assertEqual(op.counts["first_window_signals"], 1)

    def test_suppress_first_hides_first_window_signal(self):
        _, _, advisories = run([("X", 50.0, DAY + 1_000)], suppress_first=True)
        self.assertEqual(advisories, [])

    def test_falling_price_produces_sell_after_buy(self):
        events = [("X", 100.0, DAY + i * W + 1) for i in range(60)]
        events += [("X", 50.0, DAY + (60 + i) * W + 1) for i in range(60)]
        _, _, advisories = run(events)
        self.assertEqual([a.kind for a in advisories], ["BUY", "SELL"])

    def test_late_event_is_dropped_and_counted(self):
        op, emas, _ = run([("X", 10.0, DAY + W + 1_000), ("X", 99.0, DAY + 1_000)])
        self.assertEqual(op.counts["late_events"], 1)
        self.assertEqual([r.close for r in emas], [10.0])

    def test_out_of_order_within_bound_is_kept(self):
        op, emas, _ = run([("X", 10.0, DAY + W + 1_000), ("X", 99.0, DAY + 1_000)], out_of_orderness_ms=5_000)
        self.assertEqual(op.counts["late_events"], 0)
        self.assertEqual([r.close for r in emas], [99.0, 10.0])

    def test_skip_mode_ignores_empty_windows(self):
        _, emas, _ = run([("X", 10.0, DAY + 1), ("X", 20.0, DAY + 3 * W + 1)])
        self.assertEqual(len(emas), 2)

    def test_repeat_mode_fills_empty_windows_with_last_close(self):
        _, emas, _ = run([("X", 10.0, DAY + 1), ("X", 20.0, DAY + 3 * W + 1)], empty_window="repeat")
        self.assertEqual([r.close for r in emas], [10.0, 10.0, 10.0, 20.0])

    def test_subscriptions_limit_output_but_not_state(self):
        emitted = []
        op = EmaCrossoverOperator(Params(), subscribed={"Y"}, on_ema=lambda row, trigger: emitted.append(row.symbol))
        op.process("X", 1.0, DAY + 1, 0)
        op.process("Y", 2.0, DAY + 2, 1)
        op.flush()
        self.assertEqual(emitted, ["Y"])
        self.assertEqual(set(op.state), {"X", "Y"})


if __name__ == "__main__":
    unittest.main()
