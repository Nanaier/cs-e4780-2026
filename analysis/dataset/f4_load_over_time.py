import statistics

import matplotlib.dates as mdates

from common import EMA_300, SEC_COLOR, TICKS, connect, figure, query_csv, save, wall_clock, write_csv

GAP_MS = 10 * 60 * 1000


def with_gaps(points):
    xs, ys, previous = [], [], None
    for t, value in points:
        if previous is not None and t - previous > GAP_MS:
            xs.append(wall_clock(previous + 1))
            ys.append(float("nan"))
        xs.append(wall_clock(t))
        ys.append(value)
        previous = t
    return xs, ys


def main():
    con = connect()
    con.sql(f"CREATE TEMP TABLE per_second AS SELECT sys_ts // 1000 AS second, count(*) AS trades FROM {TICKS} "
            "WHERE sys_ts IS NOT NULL GROUP BY 1")
    _, summary = query_csv(con, "f4_rate_summary", f"""
        SELECT (SELECT count(*) FROM {TICKS} WHERE sys_ts IS NULL) AS trades_without_sys_ts,
               count(*) AS seconds_with_trades,
               quantile_disc(trades, 0.5) AS p50_trades_per_s, quantile_disc(trades, 0.99) AS p99_trades_per_s,
               max(trades) AS peak_trades_per_s, arg_max(make_timestamp(second * 1000000), trades) AS peak_at
        FROM per_second""")
    print(*summary[0])
    _, peaks = query_csv(con, "f4_peak_seconds", f"""
        WITH top AS (SELECT second, trades FROM per_second ORDER BY trades DESC LIMIT 10)
        SELECT make_timestamp(top.second * 1000000) AS second, top.trades,
               count(*) FILTER (WHERE t.exchange = 'FR') AS fr, count(*) FILTER (WHERE t.exchange = 'NL') AS nl,
               count(*) FILTER (WHERE t.exchange = 'ETR') AS etr
        FROM top JOIN {TICKS} t ON t.sys_ts // 1000 = top.second GROUP BY ALL ORDER BY top.trades DESC""")
    for row in peaks[:3]:
        print(*row)
    _, minutes = query_csv(con, "f4_rate_per_minute", """
        SELECT second // 60 * 60000 AS minute_ms, make_timestamp(second // 60 * 60000000) AS minute,
               sum(trades) AS trades, max(trades) AS peak_trades_per_s
        FROM per_second GROUP BY 1, 2 ORDER BY 1""")

    _, windows = query_csv(con, "f4_symbols_per_window", f"""
        SELECT window_end_ms, make_timestamp(window_end_ms * 1000) AS window_end, count(*) AS symbols_closing
        FROM read_csv('{EMA_300}', header = true) GROUP BY 1, 2 ORDER BY 1""")
    counts = [w[2] for w in windows]
    by_day = {}
    for w in windows:
        day = str(w[1])[:10]
        if w[2] > by_day.get(day, (None, 0))[1]:
            by_day[day] = (str(w[1])[11:16], w[2])
    window_summary = [["windows", len(counts)], ["median_symbols_closing", statistics.median(counts)],
                      ["max_symbols_closing", max(counts)]]
    window_summary += [[f"busiest_window_{day}", f"{end} {n}"] for day, (end, n) in sorted(by_day.items())]
    write_csv("f4_window_summary", ["measure", "value"], window_summary)
    for row in window_summary:
        print(*row)

    fig, (top, bottom) = figure(height=3.4, nrows=2, sharex=True)
    xs, ys = with_gaps([(m[0], m[3]) for m in minutes])
    top.plot(xs, ys, color=SEC_COLOR["I"], linewidth=0.6)
    top.set_ylabel("Peak trades/s\nper minute")
    xs, ys = with_gaps([(w[0], w[2]) for w in windows])
    bottom.plot(xs, ys, color=SEC_COLOR["I"], linewidth=0.6)
    bottom.set_ylabel("Symbols closing\nper 5-min window")
    bottom.xaxis.set_major_locator(mdates.DayLocator())
    bottom.xaxis.set_major_formatter(mdates.DateFormatter(""))
    bottom.xaxis.set_minor_locator(mdates.HourLocator(byhour=[12]))
    bottom.xaxis.set_minor_formatter(mdates.DateFormatter("%a"))
    bottom.tick_params(axis="x", which="minor", length=0)
    bottom.set_xlabel("Arrival time (system time), 08.11-12.11.2021")
    fig.tight_layout()
    save(fig, "f4_load_over_time")


if __name__ == "__main__":
    main()
