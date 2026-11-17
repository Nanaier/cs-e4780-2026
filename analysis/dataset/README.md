# Dataset analysis (F1–F9)

Inputs: `data/week` and `data/filtered` from `make fetch-week` / `make filter-local` and `make prep-week`, the weekend stats in `data/samples/stats`, and `out/week/reference-300`, `out/heartbeat/check-week-300.json` and `out/meta-check/reference-q-meta` from earlier runs. Period: 08.11–12.11.2021 (the weekend files only in F1). Basis: trades = price events kept by the filter (SecType E or I, Last > 0), 59,391,670 rows, unless a file says rows.

```bash
.venv/bin/pip install -r requirements.txt
analysis/dataset/run_all.sh                          # every output, about 1 min
.venv/bin/python analysis/dataset/f7_heartbeat_margins.py --rerun   # recompute F7 instead of reading the saved run
```

Outputs go to `results/`: one CSV per table, one PDF per figure (3.33 in wide, TrueType fonts).

| ID | Script | Outputs | Source |
|---|---|---|---|
| F1 | `f1_row_types.py` | `f1_row_types_by_day.csv`, `f1_row_types_summary.csv` | `data/filtered/*.kinds.txt`, `data/samples/stats/0*.json` |
| F2 | `f2_exchange_sectype.py` | `f2_exchange_sectype.csv`, `f2_by_exchange.csv`, `f2_by_sectype.csv` | ticks Parquet |
| F3 | `f3_long_tail.py` | `f3_long_tail.pdf`, `f3_trades_per_symbol.csv`, `f3_long_tail_summary.csv` | ticks Parquet |
| F4 | `f4_load_over_time.py` | `f4_load_over_time.pdf`, `f4_rate_summary.csv`, `f4_peak_seconds.csv`, `f4_rate_per_minute.csv`, `f4_symbols_per_window.csv`, `f4_window_summary.csv` | ticks Parquet (binned by `sys_ts`), `out/week/reference-300/ema.csv` |
| F5 | `f5_out_of_order_lag.py` | `f5_out_of_order_lag.csv` | `data/week/stats/*.json` (lag in file order, one stream) |
| F6 | `f6_event_vs_system_time.py`, `f6_quarantine_check.py` | `f6_event_minus_system_time.pdf`, `f6_summary.csv`, `f6_event_minus_sys_histogram.csv`, `f6_quarantine_set.csv`, `f6_reverse_case.csv`, `f6_threshold_range.csv`, `f6_quarantine_check.csv` | ticks Parquet; `trading.reference --quarantine-ms 3600000` diffed against `out/week/reference-300` |
| F7 | `f7_heartbeat_margins.py` | `f7_heartbeat_margins.csv` | `out/heartbeat/check-week-300.json` or `--rerun` |
| F8 | `f8_metadata_coverage.py` | `f8_metadata_coverage.csv`, `f8_traded_without_metadata.csv`, `f8_currencies.csv`, `f8_metadata_after_first_trade.csv`, `f8_inband_metadata.csv` | `data/week/metadata.parquet`, `metadata_events.parquet`, ticks Parquet, `out/meta-check/reference-q-meta/advisories.csv` |
| F9 | `f9_normalisation.py` | `f9_normalisation.csv` | `data/week/stats/*.json` |

Figures use one colour per security type in every chart: indices `#2a78d6`, equities `#eb6834`.
