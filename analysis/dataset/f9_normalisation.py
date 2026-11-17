from common import load_stats, pct, wall_clock, write_csv


def main():
    rows = []
    totals = [0, 0, 0, 0]
    for day, stats in load_stats():
        trades = stats["price_events"]
        values = [trades, stats["trading_date_fallback"], stats["trading_time_fallback"], stats["price_bad_timestamp"]]
        totals = [a + b for a, b in zip(totals, values)]
        rows.append([day, trades, values[1], pct(values[1], trades), values[2], values[3],
                     wall_clock(stats["first_event_ms"]).isoformat(sep=" ", timespec="milliseconds"),
                     wall_clock(stats["last_event_ms"]).isoformat(sep=" ", timespec="milliseconds")])
    rows.append(["week", totals[0], totals[1], pct(totals[1], totals[0]), totals[2], totals[3], "", ""])
    header = ["day", "trades", "no_trading_date", "no_trading_date_pct", "no_trading_time", "unparsable_timestamp",
              "first_event", "last_event"]
    write_csv("f9_normalisation", header, rows)
    for row in rows:
        print(*row)


if __name__ == "__main__":
    main()
