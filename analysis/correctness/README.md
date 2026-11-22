# Correctness, architecture and dashboard (Person 3)

Every engine is checked against the batch oracle `trading/reference.py` with `trading/diff.py`
(key: symbol + window end; close and EMAs to a relative 1e-9, advisory kinds, and `isin,currency` when present).

| File | What it is |
|---|---|
| `collect_verified.py` | Collects every verified run into `results/correctness.csv` (one row per run) and `results/correctness_table.tex` (Table: correctness). Reads Person 2's `analysis/performance/results/**` (bench.py `--verify` runs) and the file-mode logs in `results/*.txt`. No data download needed. |
| `run_file_mode.py` | Runs the reference, the Python engine and the Flink job (file source, p = 1, 2, 4, with Flink's default watermarks and with `--watermark-every-event`) on one tick folder and diffs each against the reference. Works on Windows too (no `make`). |
| `architecture_figure.py` | Draws `figures/architecture.pdf` (Figure 1). |
| `sections/*.tex` | Report sections: architecture, implementation, design space (open questions), visualisation, correctness. `report/main.tex` puts them together with the other sections. |
| `results/synthetic-*.txt` | File-mode correctness logs on the synthetic data (`make synthetic`). |

## Reproduce

With `make`: `make correctness` (synthetic, `LABEL=synthetic-local OOO=2000` by default; set `TICKS`, `OOO`,
`QUARANTINE_MS`, `LABEL` for other data), `make correctness-table`, `make architecture-figure`, and
`make flink-file WM_EVERY_EVENT=1` for the Flink job with per-tick watermarks. Without `make`:

```bash
python analysis/correctness/collect_verified.py            # after Person 2's results are merged
python analysis/correctness/architecture_figure.py

# file mode, synthetic data (needs `make synthetic` and `make flink-build`; JAVA_HOME or java on PATH)
python analysis/correctness/run_file_mode.py --ticks data/synthetic/ticks --label synthetic-local --bound-ms 900000
python analysis/correctness/run_file_mode.py --ticks data/synthetic/ticks --label synthetic-late-events --bound-ms 2000

# file mode, the real week (needs data/week from `make prep-week`)
python analysis/correctness/run_file_mode.py --ticks data/week/ticks --label week-file-mode --bound-ms 300
```

## Results (19.11.2026)

* **Real data: 23 verified runs, all identical** to the reference, 32.7 M result rows compared: Flink file source
  and Kafka source at p = 1, 2, 4 (default and balanced partitioner), 10 s checkpoints with at-least-once and
  exactly-once sinks, a TaskManager killed at about 41 % (at-least-once: 74,308 duplicates, all equal to the kept
  row, removed by the collector), Amsterdam and Amsterdam + Paris subsets, 1 to 10,000 subscribers, and the Python
  engine. Runs by Person 2 (`analysis/performance/results`).
* **Synthetic data with late ticks (2 s bound): Flink's default watermarks are timing-dependent.** The reference and
  the Python engine drop the same 10 late ticks; Flink file mode kept all 10 at p = 1, 2, 4 in the logged run (9 at
  p = 2 in an earlier run; 1,626 EMA rows and 8 advisory values differ), because Flink emits watermarks every 200 ms of processing time and a
  fast file read outruns them. The new switch `--watermark-every-event` (`PerEventWatermarks.java`, decisions.md #17)
  emits a watermark per tick and reads the files as one ordered stream; Flink then equals the reference at p = 1, 2, 4.
  Off by default: on the real week the default is already identical, so no benchmark changes.
* First-window advisories on the week: 4,970 (one per symbol) of 11,349; `--suppress-first` leaves 6,379.
