from common import TICKS, connect, query_csv


def main():
    con = connect()
    _, rows = query_csv(con, "f2_exchange_sectype", f"""
        SELECT exchange, sec_type, count(*) AS trades,
               round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS trades_pct,
               count(DISTINCT symbol) AS symbols
        FROM {TICKS} GROUP BY ALL ORDER BY exchange, sec_type""")
    for row in rows:
        print(*row)
    for name, key in (("f2_by_exchange", "exchange"), ("f2_by_sectype", "sec_type")):
        _, rows = query_csv(con, name, f"""
            SELECT {key}, count(*) AS trades, round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS trades_pct,
                   count(DISTINCT symbol) AS symbols
            FROM {TICKS} GROUP BY ALL ORDER BY trades DESC""")
        for row in rows:
            print(*row)


if __name__ == "__main__":
    main()
