import argparse
import json
import sys
import time
from pathlib import Path

import duckdb

SCAN = (
    "read_csv('{path}', columns={{'line': 'VARCHAR'}}, delim=chr(31), quote='', escape='', "
    "header=false, auto_detect=false, new_line='\\n', strict_mode=false)"
)

TIME_FORMATS = "['%d-%m-%Y %H:%M:%S.%g', '%d-%m-%Y %H:%M:%S.%f', '%d-%m-%Y %H:%M:%S']"

SEQ_STRIDE = 10_000_000_000


def parse_rows(con, path):
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE rows AS
        WITH lines AS (
            SELECT row_number() OVER () AS line_no, rtrim(line, chr(13)) AS line
            FROM {SCAN.format(path=path)}
        )
        SELECT
            line_no,
            split_part(line, ',', 1) AS symbol,
            split_part(line, ',', 2) AS sec_type,
            split_part(line, ',', 3) AS sys_date,
            split_part(line, ',', 4) AS sys_time,
            split_part(line, ',', 5) AS ask,
            split_part(line, ',', 7) AS bid,
            split_part(line, ',', 12) AS currency,
            split_part(line, ',', 15) AS isin,
            split_part(line, ',', 21) AS nominal_value,
            split_part(line, ',', 22) AS last,
            split_part(line, ',', 24) AS trading_time,
            split_part(line, ',', 27) AS trading_date,
            split_part(line, ',', 30) AS related_indices,
            regexp_matches(line, '^([^,]*,){{4}},*$') AS empty_payload
        FROM lines
        WHERE line <> '' AND line NOT LIKE '#%' AND line NOT LIKE 'ID,%'
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE classified AS
        SELECT *,
            CASE
                WHEN sys_time = '' THEN 'metadata'
                WHEN last <> '' AND sec_type IN ('E', 'I') AND TRY_CAST(last AS DOUBLE) > 0 THEN 'price'
                WHEN last <> '' THEN 'price_zero'
                WHEN ask <> '' OR bid <> '' THEN 'quote'
                WHEN empty_payload THEN 'housekeeping'
                ELSE 'other'
            END AS kind,
            regexp_extract(symbol, '\\.([A-Z]+)$', 1) AS exchange
        FROM rows
    """)


def build_ticks(con, file_index):
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE ticks AS
        WITH p AS (
            SELECT
                line_no, symbol, exchange, sec_type,
                CAST(last AS DOUBLE) AS last,
                trading_date = '' AS date_fallback,
                trading_time = '' AS time_fallback,
                CASE WHEN trading_date <> '' THEN trading_date ELSE sys_date END AS event_date,
                CASE WHEN trading_time <> '' THEN trading_time ELSE sys_time END AS event_clock,
                sys_date, sys_time
            FROM classified
            WHERE kind = 'price'
        )
        SELECT
            {file_index} * {SEQ_STRIDE} + line_no AS seq,
            symbol, exchange, sec_type, last,
            epoch_ms(try_strptime(event_date || ' ' || event_clock, {TIME_FORMATS})) AS event_ts,
            epoch_ms(try_strptime(sys_date || ' ' || sys_time, {TIME_FORMATS})) AS sys_ts,
            date_fallback, time_fallback
        FROM p
        ORDER BY seq
    """)


def file_stats(con, path):
    kinds = dict(con.execute("SELECT kind, count(*) FROM classified GROUP BY kind").fetchall())
    total = sum(kinds.values())
    price = con.execute("""
        SELECT count(*), count(DISTINCT symbol), count(*) FILTER (WHERE event_ts IS NULL),
               count(*) FILTER (WHERE date_fallback), count(*) FILTER (WHERE time_fallback),
               min(event_ts), max(event_ts)
        FROM ticks
    """).fetchone()
    by_exchange = dict(con.execute("SELECT exchange, count(*) FROM ticks GROUP BY exchange ORDER BY 1").fetchall())
    by_sec_type = dict(con.execute("SELECT sec_type, count(*) FROM ticks GROUP BY sec_type ORDER BY 1").fetchall())
    disorder = con.execute("""
        WITH r AS (
            SELECT event_ts,
                   max(event_ts) OVER (ORDER BY seq ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS seen
            FROM ticks WHERE event_ts IS NOT NULL
        )
        SELECT count(*) FILTER (WHERE event_ts < seen),
               quantile_cont(seen - event_ts, [0.5, 0.9, 0.99, 0.999, 1.0]) FILTER (WHERE event_ts < seen)
        FROM r
    """).fetchone()
    delay = con.execute("""
        SELECT quantile_cont(sys_ts - event_ts, [0.5, 0.99]) FROM ticks
        WHERE sys_ts IS NOT NULL AND event_ts IS NOT NULL
    """).fetchone()[0]
    top = con.execute("""
        SELECT symbol, count(*) AS n FROM ticks GROUP BY symbol ORDER BY n DESC LIMIT 10
    """).fetchall()
    share_top = con.execute("""
        WITH c AS (SELECT symbol, count(*) AS n FROM ticks GROUP BY symbol),
        r AS (SELECT n, row_number() OVER (ORDER BY n DESC) AS rk, sum(n) OVER () AS tot FROM c)
        SELECT round(100.0 * sum(n) FILTER (WHERE rk <= 10) / any_value(tot), 2),
               round(100.0 * sum(n) FILTER (WHERE rk <= 100) / any_value(tot), 2)
        FROM r
    """).fetchone()
    return {
        "file": str(path),
        "rows": total,
        "kinds": dict(sorted(kinds.items())),
        "price_events": price[0],
        "price_symbols": price[1],
        "price_bad_timestamp": price[2],
        "trading_date_fallback": price[3],
        "trading_time_fallback": price[4],
        "first_event_ms": price[5],
        "last_event_ms": price[6],
        "price_by_exchange": by_exchange,
        "price_by_sec_type": by_sec_type,
        "out_of_order_events": disorder[0],
        "out_of_order_lag_ms_p50_p90_p99_p999_max": disorder[1],
        "arrival_delay_ms_p50_p99": delay,
        "top_symbols": top,
        "share_of_events_top10_top100_pct": share_top,
    }


def write_metadata(con, out_dir):
    con.execute(f"""
        COPY (
            SELECT symbol,
                   arg_max(exchange, line_no) AS exchange,
                   arg_max(sec_type, line_no) AS sec_type,
                   arg_max(isin, line_no) FILTER (WHERE isin <> '') AS isin,
                   arg_max(currency, line_no) FILTER (WHERE currency <> '') AS currency,
                   arg_max(related_indices, line_no) FILTER (WHERE related_indices <> '') AS related_indices
            FROM all_metadata
            GROUP BY symbol
            ORDER BY symbol
        ) TO '{out_dir / "metadata.parquet"}' (FORMAT parquet)
    """)
    con.execute(f"""
        COPY (SELECT seq, symbol, isin, currency FROM all_events ORDER BY seq)
        TO '{out_dir / "metadata_events.parquet"}' (FORMAT parquet)
    """)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Slim raw DEBS 2022 trading CSVs into compact tick and metadata files")
    parser.add_argument("inputs", nargs="+", help="raw CSV files (or /dev/stdin), in chronological order")
    parser.add_argument("--out", default="data/slim", help="output directory")
    parser.add_argument("--memory-limit", default="8GB")
    parser.add_argument("--no-csv", action="store_true", help="skip the plain CSV copy of the ticks")
    parser.add_argument("--first-index", type=int, default=0,
                        help="file index of the first input, for preparing days in separate parallel runs")
    args = parser.parse_args(argv)

    out_dir = Path(args.out)
    (out_dir / "ticks").mkdir(parents=True, exist_ok=True)
    (out_dir / "stats").mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{args.memory_limit}'")
    con.execute("CREATE TEMP TABLE all_metadata (line_no BIGINT, symbol VARCHAR, exchange VARCHAR, sec_type VARCHAR, isin VARCHAR, currency VARCHAR, related_indices VARCHAR)")
    con.execute("CREATE TEMP TABLE all_events (seq BIGINT, symbol VARCHAR, isin VARCHAR, currency VARCHAR)")

    summary = []
    for index, raw in enumerate(args.inputs, start=args.first_index):
        started = time.perf_counter()
        stem = Path(raw).name.removesuffix(".gz").removesuffix(".csv") if raw != "/dev/stdin" else f"stdin-{index}"
        name = f"{index:03d}-{stem}"
        parse_rows(con, raw)
        build_ticks(con, index)
        con.execute(f"""
            INSERT INTO all_metadata
            SELECT {index} * {SEQ_STRIDE} + line_no, symbol, exchange, sec_type, isin, currency, related_indices
            FROM classified WHERE isin <> '' OR currency <> '' OR related_indices <> ''
        """)
        con.execute(f"""
            INSERT INTO all_events
            SELECT {index} * {SEQ_STRIDE} + line_no, symbol, isin, currency FROM classified WHERE kind = 'metadata'
        """)
        con.execute(f"""
            COPY (SELECT seq, symbol, exchange, sec_type, last, event_ts, sys_ts FROM ticks WHERE event_ts IS NOT NULL ORDER BY seq)
            TO '{out_dir / "ticks" / (name + ".parquet")}' (FORMAT parquet)
        """)
        if not args.no_csv:
            con.execute(f"""
                COPY (SELECT symbol, sec_type, last, event_ts, seq FROM ticks WHERE event_ts IS NOT NULL ORDER BY seq)
                TO '{out_dir / "ticks" / (name + ".csv")}' (FORMAT csv, HEADER false)
            """)
        stats = file_stats(con, raw)
        stats["seconds"] = round(time.perf_counter() - started, 2)
        (out_dir / "stats" / f"{name}.json").write_text(json.dumps(stats, indent=2, default=str))
        summary.append(stats)
        print(f"{name}: {stats['rows']} rows, {stats['price_events']} price events, "
              f"{stats['price_symbols']} symbols, kinds={stats['kinds']} ({stats['seconds']} s)", file=sys.stderr)

    write_metadata(con, out_dir)
    (out_dir / "stats" / "summary.json").write_text(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
