import csv
import json
import tempfile
import unittest
from pathlib import Path

from trading import viz

EMA = ["symbol", "window_end_ms", "window_end", "close", "ema38", "ema100"]
ADV = ["symbol", "window_end_ms", "window_end", "advisory", "ema38", "ema100", "prev_ema38", "prev_ema100"]


def write(path, header, rows):
    with path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)


def page_data(html):
    start = html.index('<script type="application/json" id="data">') + len('<script type="application/json" id="data">')
    return json.loads(html[start:html.index("</script>", start)])


class DashboardTest(unittest.TestCase):
    def build(self, ema, advisories, *args, adv_header=ADV):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            write(folder / "ema.csv", EMA, ema)
            write(folder / "advisories.csv", adv_header, advisories)
            viz.main(["--results", str(folder), *args])
            return page_data((folder / "dashboard.html").read_text(encoding="utf-8"))

    def test_first_window_signal_is_separate_from_real_crossovers(self):
        ema = [["A.FR", 300000, "", 10, 0.5, 0.2], ["A.FR", 600000, "", 9, 0.4, 0.45], ["B.NL", 300000, "", 5, 0.2, 0.1]]
        adv = [["A.FR", 300000, "", "BUY", 0.5, 0.2, 0, 0], ["A.FR", 600000, "", "SELL", 0.4, 0.45, 0.5, 0.2],
               ["B.NL", 300000, "", "BUY", 0.2, 0.1, 0, 0]]
        data = self.build(ema, adv)
        self.assertEqual(data["totals"], {"ema": 3, "first": 2})
        a = next(s for s in data["symbols"] if s["s"] == "A.FR")
        self.assertEqual((a["buy"], a["sell"], a["first"], a["last"], a["trend"], a["x"]), (0, 1, 1, "SELL", "down", "FR"))
        self.assertEqual([x["w"] for x in data["advisories"] if x["s"] == "A.FR"], [True, False])

    def test_scatter_counts_each_symbols_windows_per_replay_hour(self):
        ema = [["A.FR", 300000, "", 1, 1, 1], ["A.FR", 3600000, "", 1, 1, 1], ["A.FR", 3900000, "", 1, 1, 1],
               ["B.NL", 3900000, "", 1, 1, 1]]
        data = self.build(ema, [])
        self.assertEqual(data["overview"]["hours"], [0, 3600])
        self.assertEqual({p[0]: p[4] for p in data["overview"]["scatter"]}, {"A.FR": [0, 2, 1, 1], "B.NL": [1, 1]})

    def test_every_exchange_is_represented_when_symbols_are_capped(self):
        ema = [[f"S{i}.FR", 300000, "", 1, 1, 1] for i in range(5)] + [["T.ETR", 300000, "", 1, 1, 1]]
        data = self.build(ema, [], "--max-symbols", "2")
        self.assertEqual(sorted(s["x"] for s in data["symbols"]), ["ETR", "FR"])

    def test_rows_written_out_of_order_are_sorted_for_the_chart(self):
        ema = [["A.FR", 900000, "", 3, 3, 1], ["A.FR", 300000, "", 1, 1, 1], ["A.FR", 600000, "", 2, 2, 1]]
        adv = [["A.FR", 900000, "", "BUY", 3, 1, 0.5, 1], ["A.FR", 300000, "", "BUY", 1, 0.5, 0, 0]]
        data = self.build(ema, adv)
        self.assertEqual(data["series"]["A.FR"]["t"], [300, 600, 900])
        self.assertEqual(data["series"]["A.FR"]["c"], [1, 2, 3])
        self.assertEqual([a["t"] for a in data["advisories"]], [300, 900])
        self.assertEqual(data["symbols"][0]["trend"], "up")

    def test_include_and_isin_from_enriched_advisories(self):
        ema = [[f"S{i}.FR", 300000, "", 1, 1, 1] for i in range(5)]
        adv = [["S4.FR", 300000, "", "BUY", 1, 0.5, 0, 0, "FR0000000004", "EUR"]]
        data = self.build(ema, adv, "--max-symbols", "1", "--include", "S4.FR", adv_header=ADV + ["isin", "currency"])
        self.assertEqual([(s["s"], s["isin"]) for s in data["symbols"]], [("S4.FR", "FR0000000004")])


if __name__ == "__main__":
    unittest.main()
