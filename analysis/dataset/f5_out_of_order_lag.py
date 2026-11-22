from common import load_stats, pct, write_csv


def main():
    rows = []
    for day, stats in load_stats():
        p50, p90, p99, p999, worst = stats["out_of_order_lag_ms_p50_p90_p99_p999_max"]
        late = stats["out_of_order_events"]
        rows.append([day, stats["price_events"], late, pct(late, stats["price_events"]), p50, p90, p99, p999, worst])
    header = ["day", "trades", "behind_newest", "behind_newest_pct", "lag_p50_ms", "lag_p90_ms", "lag_p99_ms",
              "lag_p999_ms", "lag_max_ms"]
    write_csv("f5_out_of_order_lag", header, rows)
    for row in rows:
        print(*row)


if __name__ == "__main__":
    main()
