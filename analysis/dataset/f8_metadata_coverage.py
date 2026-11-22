import csv
from collections import Counter

from common import META, META_EVENTS, ROOT, TICKS, WEEK, connect, query_csv, write_csv
from trading.files import metadata_changes

ENRICHED = ROOT / "out" / "meta-check" / "reference-q-meta" / "advisories.csv"


def main():
    con = connect()
    _, coverage = query_csv(con, "f8_metadata_coverage", f"""
        SELECT count(*) AS symbols, count(isin) AS with_isin, count(currency) AS with_currency,
               count(related_indices) AS with_related_indices FROM {META}""")
    print(*coverage[0])
    _, missing = query_csv(con, "f8_traded_without_metadata", f"""
        WITH s AS (SELECT symbol, any_value(exchange) AS exchange, any_value(sec_type) AS sec_type, count(*) AS trades
                   FROM {TICKS} GROUP BY 1)
        SELECT s.* FROM s LEFT JOIN {META} m USING (symbol) WHERE m.symbol IS NULL ORDER BY trades DESC, symbol""")
    print("traded without metadata", len(missing), missing[:2])
    _, currencies = query_csv(con, "f8_currencies", f"""
        SELECT coalesce(currency, '(none)') AS currency, count(*) AS symbols FROM {META} GROUP BY 1 ORDER BY 2 DESC, 1""")
    print(currencies[:5])
    _, late = query_csv(con, "f8_metadata_after_first_trade", f"""
        WITH t AS (SELECT symbol, min(seq) AS first_trade_seq, count(*) AS trades FROM {TICKS} GROUP BY 1),
             e AS (SELECT symbol, min(seq) AS first_metadata_seq FROM {META_EVENTS}
                   WHERE coalesce(isin, '') <> '' OR coalesce(currency, '') <> '' GROUP BY 1)
        SELECT symbol, trades, first_trade_seq, first_metadata_seq, first_metadata_seq - first_trade_seq AS lines_later
        FROM t JOIN e USING (symbol) WHERE first_metadata_seq > first_trade_seq ORDER BY lines_later DESC""")
    print("metadata after first trade", late)

    rows, symbols, filled = con.sql(f"""
        SELECT count(*), count(DISTINCT symbol),
               count(DISTINCT symbol) FILTER (WHERE coalesce(isin, '') <> '' OR coalesce(currency, '') <> '')
        FROM {META_EVENTS}""").fetchall()[0]
    changes = list(metadata_changes(WEEK / "metadata_events.parquet"))
    per_symbol = Counter(change[1] for change in changes)
    repeated = sorted(symbol for symbol, n in per_symbol.items() if n > 1)
    facts = [["metadata_rows", rows], ["symbols_with_metadata_rows", symbols], ["symbols_with_a_value", filled],
             ["symbols_with_only_empty_rows", symbols - filled], ["rows_replay_sends", len(changes)],
             ["symbols_replay_sends_for", len(per_symbol)], ["symbols_sent_more_than_once", " ".join(repeated)],
             ["symbols_whose_first_metadata_follows_first_trade", len(late)]]
    if ENRICHED.exists():
        with ENRICHED.open() as handle:
            advisories = list(csv.DictReader(handle))
        facts.append(["advisories_without_metadata_as_of_window_close",
                      sum(1 for a in advisories if not a["isin"] and not a["currency"])])
        facts.append(["advisories", len(advisories)])
    write_csv("f8_inband_metadata", ["measure", "value"], facts)
    for row in facts:
        print(*row)


if __name__ == "__main__":
    main()
