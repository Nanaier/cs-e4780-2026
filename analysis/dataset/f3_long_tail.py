from common import SEC_COLOR, SEC_NAME, TICKS, connect, figure, query_csv, save, write_csv


def main():
    con = connect()
    _, rows = query_csv(con, "f3_trades_per_symbol", f"""
        SELECT row_number() OVER (ORDER BY trades DESC, symbol) AS rank, symbol, exchange, sec_type, trades,
               round(100.0 * sum(trades) OVER (ORDER BY trades DESC, symbol) / sum(trades) OVER (), 4) AS cumulative_pct
        FROM (SELECT symbol, any_value(exchange) AS exchange, any_value(sec_type) AS sec_type, count(*) AS trades
              FROM {TICKS} GROUP BY symbol)
        ORDER BY rank""")
    total = sum(r[4] for r in rows)
    trades = sorted((r[4] for r in rows), reverse=True)
    share = lambda n: round(100.0 * sum(trades[:n]) / total, 2)
    middle = len(trades) // 2
    median = trades[middle] if len(trades) % 2 else (trades[middle - 1] + trades[middle]) / 2
    summary = [["symbols", len(trades)], ["trades", total], ["busiest_symbol_pct", share(1)], ["top10_pct", share(10)],
               ["top100_pct", share(100)], ["top1000_pct", share(1000)], ["median_trades_per_symbol", median],
               ["symbols_under_100_trades", sum(t < 100 for t in trades)],
               ["symbols_under_10_trades", sum(t < 10 for t in trades)]]
    write_csv("f3_long_tail_summary", ["measure", "value"], summary)
    for row in summary:
        print(*row)

    fig, (top, bottom) = figure(height=3.6, nrows=2)
    for sec in ("I", "E"):
        points = [(r[0], r[4]) for r in rows if r[3] == sec]
        top.scatter([p[0] for p in points], [p[1] for p in points], s=3, linewidths=0, color=SEC_COLOR[sec],
                    label=f"{SEC_NAME[sec]} ({len(points):,})")
    top.set_xscale("log")
    top.set_yscale("log")
    top.set_xlabel("Symbol rank by trades")
    top.set_ylabel("Trades per symbol")
    top.legend(loc="lower left", markerscale=2.5)
    bottom.plot([r[0] for r in rows], [r[5] for r in rows], color=SEC_COLOR["I"])
    bottom.set_xscale("log")
    bottom.set_ylim(0, 100)
    bottom.set_xlabel("Number of busiest symbols")
    bottom.set_ylabel("Share of all trades (%)")
    for n in (10, 100, 1000):
        bottom.annotate(f"top {n:,}: {share(n)}%", (n, share(n)), xytext=(-5, 5), textcoords="offset points",
                        fontsize=7, color="#52514e", ha="right")
        bottom.plot([n], [share(n)], marker="o", markersize=3, color=SEC_COLOR["I"])
    fig.tight_layout()
    save(fig, "f3_long_tail")


if __name__ == "__main__":
    main()
