#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
PY=${PY:-.venv/bin/python}
for script in f1_row_types f2_exchange_sectype f3_long_tail f4_load_over_time f5_out_of_order_lag f6_event_vs_system_time \
              f6_quarantine_check f7_heartbeat_margins f8_metadata_coverage f9_normalisation; do
  echo "== $script"
  "$PY" "analysis/dataset/$script.py"
done
