#!/usr/bin/env bash
set -euo pipefail

out="${OUT:-data/week}"
filtered="${FILTERED:-data/filtered}"
py="${PY:-.venv/bin/python}"
days=("$@")
if [ ${#days[@]} -eq 0 ]; then
  days=(08-11-21 09-11-21 10-11-21 11-11-21 12-11-21)
fi

parts="$out/.parts"
rm -rf "$parts"
mkdir -p "$parts" "$out/ticks" "$out/stats"
pids=()
for i in "${!days[@]}"; do
  "$py" -m trading.prep --first-index "$i" --memory-limit 3GB --out "$parts/$i" \
    "$filtered/debs2022-gc-trading-day-${days[$i]}.csv.gz" &
  pids+=($!)
done
for pid in "${pids[@]}"; do wait "$pid"; done

for i in "${!days[@]}"; do
  mv "$parts/$i"/ticks/* "$out/ticks/"
  mv "$parts/$i"/stats/0*.json "$out/stats/"
done
"$py" - "$parts" "$out" <<'EOF'
import sys
import duckdb
parts, out = sys.argv[1], sys.argv[2]
duckdb.sql(f"""
    COPY (
        SELECT symbol, arg_max(exchange, f) AS exchange, arg_max(sec_type, f) AS sec_type,
               arg_max(isin, f) AS isin, arg_max(currency, f) AS currency,
               arg_max(related_indices, f) AS related_indices
        FROM (SELECT *, filename AS f FROM read_parquet('{parts}/*/metadata.parquet', filename = true))
        GROUP BY symbol ORDER BY symbol
    ) TO '{out}/metadata.parquet' (FORMAT parquet)
""")
EOF
rm -rf "$parts"
ls -la "$out/ticks"
