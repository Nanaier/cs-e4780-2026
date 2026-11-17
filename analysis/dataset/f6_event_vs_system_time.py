from common import SEC_COLOR, TICKS, connect, figure, query_csv, save

HOUR_MS = 3_600_000


def main():
    con = connect()
    _, summary = query_csv(con, "f6_summary", f"""
        SELECT count(*) AS trades,
               round(100.0 * count(*) FILTER (WHERE event_ts - sys_ts >= -1000 AND event_ts - sys_ts < 1000) / count(*), 3)
                   AS within_1s_pct,
               round(100.0 * count(*) FILTER (WHERE sys_ts < event_ts) / count(*), 3) AS sys_earlier_pct,
               count(*) FILTER (WHERE sys_ts - event_ts > {HOUR_MS}) AS stamped_over_1h_before_arrival,
               count(*) FILTER (WHERE event_ts - sys_ts > {HOUR_MS}) AS stamped_over_1h_after_system_time
        FROM {TICKS}""")
    print(*summary[0])
    _, histogram = query_csv(con, "f6_event_minus_sys_histogram", f"""
        SELECT floor((event_ts - sys_ts) / 1000.0)::BIGINT AS seconds, count(*) AS trades
        FROM {TICKS} GROUP BY 1 ORDER BY 1""")
    _, quarantine = query_csv(con, "f6_quarantine_set", f"""
        SELECT exchange, sec_type, count(*) AS trades, count(DISTINCT symbol) AS symbols,
               min(make_timestamp(event_ts * 1000)) AS stamped_from, max(make_timestamp(event_ts * 1000)) AS stamped_to,
               min(make_timestamp(sys_ts * 1000)) AS arrived_from, max(make_timestamp(sys_ts * 1000)) AS arrived_to,
               round(min(sys_ts - event_ts) / {HOUR_MS}, 2) AS min_hours_old, round(max(sys_ts - event_ts) / {HOUR_MS}, 2) AS max_hours_old
        FROM {TICKS} WHERE sys_ts - event_ts > {HOUR_MS} GROUP BY ALL ORDER BY trades DESC""")
    for row in quarantine:
        print(*row)
    _, reverse = query_csv(con, "f6_reverse_case", f"""
        SELECT exchange, sec_type, count(*) AS trades, count(DISTINCT symbol) AS symbols,
               min(make_timestamp(event_ts * 1000)) AS stamped_from, max(make_timestamp(event_ts * 1000)) AS stamped_to,
               round(min(event_ts - sys_ts) / {HOUR_MS}, 2) AS min_hours_after_sys, round(max(event_ts - sys_ts) / {HOUR_MS}, 2) AS max_hours_after_sys
        FROM {TICKS} WHERE event_ts - sys_ts > {HOUR_MS} GROUP BY ALL ORDER BY trades DESC""")
    for row in reverse:
        print(*row)
    _, threshold = query_csv(con, "f6_threshold_range", f"""
        SELECT max(sys_ts - event_ts) FILTER (WHERE sys_ts - event_ts <= {HOUR_MS}) AS largest_lag_kept_ms,
               min(sys_ts - event_ts) FILTER (WHERE sys_ts - event_ts > {HOUR_MS}) AS smallest_lag_quarantined_ms,
               round(min(sys_ts - event_ts) FILTER (WHERE sys_ts - event_ts > {HOUR_MS}) / {HOUR_MS}, 2) AS smallest_lag_quarantined_h
        FROM {TICKS}""")
    print(*threshold[0])

    fig, ax = figure(height=2.2)
    ax.vlines([h[0] for h in histogram], 1, [h[1] for h in histogram], color=SEC_COLOR["I"], linewidth=0.8)
    ax.set_xscale("symlog", linthresh=1)
    ax.set_xticks([-1e4, -1e2, -1, 0, 1, 1e2, 1e4])
    ax.set_xticklabels(["$-10^4$", "$-10^2$", "$-1$", "0", "1", "$10^2$", "$10^4$"])
    ax.set_yscale("log")
    ax.set_ylim(0.8, None)
    ax.set_xlabel("Event time minus system time (s, symmetric log)")
    ax.set_ylabel("Trades")
    burst = sum(h[1] for h in histogram if h[0] < -3600)
    auction = sum(h[1] for h in histogram if h[0] >= 3600)
    ax.annotate(f"10.11 burst:\n{burst:,} trades\n17.9 h old", (-64_600, 300), xytext=(6, 0), textcoords="offset points",
                fontsize=7, color="#52514e", va="center")
    ax.annotate(f"$\\geq$1 h ahead:\n{auction:,} Xetra\nauction trades", (20_000, 3), xytext=(300, 3e5),
                textcoords="data", fontsize=7, color="#52514e", va="center", ha="center",
                arrowprops={"arrowstyle": "-", "color": "#52514e", "linewidth": 0.5})
    fig.tight_layout()
    save(fig, "f6_event_minus_system_time")


if __name__ == "__main__":
    main()
