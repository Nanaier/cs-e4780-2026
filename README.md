# Detecting Trading Trends in Financial Tick Data

CS-E4780 course project. Query 1: EMA38 and EMA100 per symbol over 5-minute event-time tumbling windows. Query 2: buy and sell advisories on EMA crossovers. Plus a dashboard.

## Quick start (no Kafka, no download)

```bash
make venv        # Python 3.12 virtualenv with duckdb, pyarrow, confluent-kafka
make test        # unit tests for the windowing and EMA logic
make demo        # 3 synthetic trading days -> prep -> stream engine -> batch reference -> dashboard -> diff
open out/stream/dashboard.html
```

`make demo` ends with a diff. With the default 2-second out-of-orderness bound it reports a few differences: the synthetic data contains deliberately late events, which the stream drops and the batch reference keeps. Run `make stream OOO=900000 && make diff` to see both agree exactly.

## Getting the real data

The full set is 5 weekday files of 4.7–5.5 GB each (24.9 GB) plus two small weekend files. The weekend files contain no real trades. Stream each day through the filter instead of storing it:

```bash
make fetch-week   # per day: download -> keep price + metadata rows -> gzip (about 59M price events in total)
make prep-week    # -> data/week/ticks/*.parquet, data/week/metadata_events.parquet, data/week/metadata.parquet, data/week/stats/*.json
make stream TICKS=data/week/ticks META=data/week/metadata.parquet
```

If the raw files are already in `~/Downloads`, `make filter-local` filters them instead of downloading: about 7 minutes per day on an M2, one day at a time (filtering days in parallel was no faster; reading the disk is the limit). `make prep-week` then prepares the five days in parallel. Downloads use 8 parallel range requests (`PARALLEL=N`), because Zenodo serves about 0.7 MB/s per connection.

One day only: `scripts/fetch_day.sh 08-11-21`. A local file works too: `scripts/fetch_day.sh ~/Downloads/debs2022-gc-trading-day-08-11-21.csv`.

`data/*/stats/*.json` holds the dataset analysis per file: row kinds, price events per exchange and security type, symbol long tail, out-of-order lag percentiles and arrival delay.

## Components

| Path | What it does |
|---|---|
| `scripts/filter.awk`, `scripts/fetch_day.sh` | Streaming filter for the raw 40-column CSV: keeps price rows (SecType E/I, Last > 0) and metadata rows, counts every other row kind |
| `trading/prep.py` | Raw or filtered CSV -> `ticks/*.parquet` (and plain CSV for Flink), `metadata_events.parquet` (every metadata row in feed order), `metadata.parquet` (latest per symbol, for the dashboard), `stats/*.json` |
| `trading/synth.py` | Synthetic trading days in the raw layout, with quotes, housekeeping rows, zero-price resets, out-of-order and late events, and a long-tailed symbol distribution |
| `trading/core.py` | The operator: event-time windows, watermark, per-symbol EMA state, crossover detection, subscriptions |
| `trading/engine.py` | Runs the operator from tick files or from Kafka; writes `ema.csv`, `advisories.csv`, `metrics.json` |
| `trading/reference.py` | Independent batch implementation (DuckDB + Python) used as the correctness oracle |
| `trading/diff.py` | Compares two result folders; exit code 0 only when identical |
| `trading/replay.py` | Replays ticks into Kafka, keyed by symbol, as fast as possible or at a speed factor; optionally routes quarantined ticks to their own topic, sends metadata changes in-band at their feed position and sends heartbeats to every partition |
| `trading/heartbeat_check.py` | Checks the heartbeat rule against the tick files in replay order: ticks behind the heartbeat watermark, per margin |
| `trading/collect.py` | Reads the `ema` and `advisories` topics back into CSV files and reports latency percentiles; `--isolation`, `--dedupe` |
| `trading/topics.py` | Creates (optionally compacted), deletes and counts Kafka topics |
| `trading/subscribers.py` | Generates N subscribers with K symbols each (popularity-weighted or uniform) into the compacted subscriptions topic |
| `trading/gateway.py` | Fans result rows out to subscribers through an inverted index symbol -> subscribers; counts deliveries and their delay |
| `scripts/subscriber_scaling.sh` | Flink p = 2 plus the gateway on one day for N = 1, 100, 1,000, 10,000 subscribers, each run verified |
| `trading/latency.py`, `scripts/latency_run.sh` | Engine-independent latency from the moment each window could first close, with a paced replay |
| `trading/bench.py` | Throughput benchmark: drains a preloaded topic with the Docker Flink cluster at several parallelisms |
| `trading/partition_map.py` | Balanced symbol -> partition map from one day's tick counts (largest symbol first, to the least loaded partition), for `replay.py --partition-map` |
| `trading/viz.py` | Static HTML dashboard: watchlist, price with EMA38/EMA100 and buy/sell markers, market breadth |
| `docs/decisions.md` | Open questions in the brief, the proposed default for each, and the switch that changes it |
| `flink-job/` | The Flink DataStream job |
| `docker-compose.yml`, `docker-compose.flink.yml` | Kafka broker; Flink JobManager and TaskManager with CPU pinning for the core-count runs |

## Kafka

```bash
make kafka-up                     # apache/kafka:4.4.0, single KRaft broker on localhost:9092, 8 partitions
make replay SPEED=0               # as fast as possible
make stream-kafka                 # Python engine reading the topic
make kafka-down
```

The engine keeps one watermark per partition and uses the minimum, as Flink does; with a single global watermark, a partition that runs ahead makes most other events late.

Latency is only meaningful when the producer and the engine run at the same time and the producer is slower than the engine, for example `make replay SPEED=600` in one terminal and `make stream-kafka` in another. If you replay first and consume later, the measured latency is just the backlog.

Both engines measure latency the same way. A sample starts at the newest `ingest_ms` the operator has read when a window closes and ends when that window's EMA row is produced. There is one sample per EMA row. The Python engine writes the percentiles to `metrics.json`. For the Flink job, `make collect` computes them from the last two columns of the `ema` topic. Neither number includes the time a window-closing tick waits for the next consumer batch (Python, up to the 0.5 s poll timeout) or for the next periodic watermark (Flink, every 200 ms).

## Flink job

Flink 2.2.1, Kafka connector 5.0.0-2.2, Java 17 bytecode (builds with JDK 17 or 21). `keyBy(symbol)` then a `KeyedProcessFunction`: a map of open windows per symbol, one event-time timer per window at `window_end - 1`, EMA state carried across windows, advisories on a side output, late ticks on another side output and the `lateTicks` metric. The window and EMA logic is in `EmaWindows`, shared with the `KeyedCoProcessFunction` used with `--subscriptions-topic`; both skip in-band metadata records or, with `--enrich`, keep them as versions (see Metadata and subscriptions).

```bash
make flink-build                          # shaded jar in flink-job/target
make flink-file OOO=900000                # local mini-cluster, tick CSVs -> out/flink
make diff-flink                           # out/flink vs out/reference
```

From Kafka, with results written to the `ema` and `advisories` topics and read back by the collector:

```bash
make kafka-up
make flink-kafka P=4                      # terminal 1, runs until stopped
make collect                              # terminal 2: topics -> out/collected + latency percentiles
make replay SPEED=5000                    # terminal 3: about 10k events/s for the synthetic data
```

Create the `ticks` topic before starting the job (`make replay` creates it, or run it once with `--limit 1`); the Kafka source fails on a missing topic. Add `--bounded` to the job arguments to stop at the current end of the topic and flush the last windows.

Docker cluster for the core-count runs (JobManager and Kafka pinned to vCPUs 4-5, the TaskManager to `TM_CPUSET`, slots = parallelism = `P`):

```bash
make flink-build
make cluster-up P=2 TM_CPUSET=0-1        # Flink UI on http://localhost:8081, Prometheus metrics on :9249
make cluster-submit P=2
make cluster-down
```

Docker Desktop's VM has 6 vCPUs on this laptop, so measure 1, 2 and 4.

The Kafka sink writes result records with the producer's wall-clock timestamp, not the window's event time: the broker's 6-hour retention deletes records stamped 2021 at its next 5-minute check.

## Latency runs

```bash
ENGINE=flink P=2 SPEED=300 LIMIT=60000 OOO=2000 scripts/latency_run.sh
ENGINE=python SPEED=300 LIMIT=60000 OOO=2000 scripts/latency_run.sh
```

Each run creates fresh topics, starts the engine (the Flink job in a local mini-cluster), replays `TICKS` at `SPEED`, and computes latency from the moment each window could first close: the first tick or heartbeat past window end + bound on every partition (`trading/latency.py`). After the replay the script waits until the EMA and advisory topics have not grown for `SETTLE_S` seconds (default 10, at most `MAX_WAIT_S`, default 600) and hold no uncommitted rows before stopping Flink; the Python engine stops itself after an idle period and flushes its last windows. The run fails if the engine exits early or produced nothing. Output goes to `out/latency/<engine>-p<P>-x<SPEED>-<time>/` (`OUT` changes the folder; `CHECKPOINT_MS` and `DELIVERY` add `-c<ms>-<delivery>` to the name); topics are deleted unless `KEEP_TOPICS=1`. `MAX_GAP_MS` passes `--max-gap-ms` to the replay (tag `-g<ms>`; the Python engine's idle exit becomes `MAX_GAP_MS / SPEED + 20` s, at least 30 s); `HEARTBEAT_MS`, `HEARTBEAT_TAIL_MS` and `HEARTBEAT_MARGIN_MS` pass the heartbeat switches (tag `-hb<ms>`, see Heartbeats). With the settings above, Flink reports 14,444 rows and the Python engine 14,594, of which 150 come from its final flush (`never_closable`).

## Throughput benchmark

```bash
make flink-build
make bench-preload                        # bench-week-q: retention -1, replay at full speed, QUARANTINE_MS=3600000
make bench BENCH_P="1 2 4" BENCH_RUNS=3 BENCH_ARGS="--verify out/week/reference-300 --verify-runs 1 --python-baseline"
```

The bench targets default to `BENCH_TOPIC=bench-week-q` and `QUARANTINE_MS=3600000`; every other target keeps `QUARANTINE_MS` empty. `QUARANTINE_MS=` on the command line turns the quarantine off.

`trading/bench.py` checks that the broker is pinned to vCPUs 4-5 and that the topic holds exactly as many messages as the parquet files under `BENCH_TICKS`. For every run it recreates the JobManager and TaskManager (TaskManager on the first P vCPUs, `--cpusets` overrides), submits the job with `--bounded` to drain the topic, samples the REST API once per second and `docker stats` in a background thread, and deletes the run's output topics at the end. Per run it records:

- records per second three ways: over the job duration (REST start to end), over the source vertex's duration, and the median source rate (`numRecordsOut` over 5-second spans) in the middle 80% of the source's run;
- busy, back-pressured and idle ms/s per vertex (average and busiest subtask) over the same middle 80%, from Flink's accumulated time counters; the `*TimeMsPerSecond` gauges are kept too, but they average over 60 s;
- records per source subtask and per keyed subtask, mean CPU % of broker, JobManager and TaskManager, and the output row counts;
- with `--verify DIR`, a `trading.diff` of the collected outputs against `DIR`.

`--python-baseline` runs the Python engine on the host against the same topic with the same bound. Results go to `out/bench/<topic>-<time>/`: one JSON per run (including the samples), `summary.csv` and `summary.md`. The command exits non-zero when a job fails or a verified output differs. `docker-compose.flink.yml` sets the JobManager's metric fetch interval to 1 s so the samples are fresh; the cluster is removed at the end unless `--keep-cluster`. Make variables: `BENCH_TOPIC`, `BENCH_TICKS`, `BENCH_P`, `BENCH_RUNS`, `BENCH_OOO`, `BENCH_ARGS`.

Draining a preloaded topic without the quarantine (`make bench-preload BENCH_TOPIC=bench-week QUARANTINE_MS=`) lets small partitions run ahead in event time, and both engines take the minimum watermark over partitions, so they can drop fewer late ticks than the single ordered stream that `reference.py --late-bound-ms` models. On the week at 300 ms this concerns the 5,506 ticks stamped 10.11 01:00:00 that arrive at 18:56: Flink (p = 1, 2, 4) and the Python engine keep them for the 692 symbols in partitions 0, 4 and 5, which then match `reference-all` instead of `reference-300`; every other symbol matches `reference-300`. Day 08 has no such ticks and matches `out/day08/reference` exactly.

With the quarantine (see below) those ticks never reach the ticks topic, and every engine matches `reference-300` exactly:

```bash
make bench-preload                        # 59,386,164 ticks + 5,506 in bench-week-q-quarantine
make bench BENCH_RUNS=1 BENCH_ARGS="--verify out/week/reference-300 --python-baseline"
```

Flink p = 1, 2, 4 and the Python engine all gave 1,855,645 EMA rows and 11,349 advisories with 0 mismatched, 0 only-left, 0 only-right (`out/bench/bench-week-q-20261113-214636`).

### Balanced partitioning

With the CRC32 partitioner the busiest of 4 source readers gets 29.5 % of the week's ticks (1.18x the mean) whatever the partition count, because a reader's load is CRC32 mod p whenever p divides the partition count. A map built from one day's counts balances the next days to within 1.5 %:

```bash
.venv/bin/python -m trading.partition_map --ticks data/day08/ticks --out out/partitioner/partition-map-day08.csv
.venv/bin/python -m trading.replay --ticks data/week/ticks/001-debs2022-gc-trading-day-09-11-21.parquet --topic pm09-bal --speed 0 --partition-map out/partitioner/partition-map-day08.csv
```

Symbols missing from the map keep CRC32. The keyed stage is unaffected: `keyBy` hashes the symbol into Flink key groups (busiest of 4 subtasks: 27.4 %). On 09.11 (`out/partitioner/summary.md`, map from 08.11) the busiest reader at p = 4 went from 29.54 % to 25.14 % and both topics matched a 09.11 reference exactly; the throughput difference was within the run-to-run noise of those 10-second drains on a loaded host.

## Quarantine

`--quarantine-ms N` holds back every tick whose system time is more than N ms after its event time (`quarantined()` in `trading/core.py`, `quarantine_sql()` for DuckDB). Off by default.

- `replay.py` sends those ticks to `--quarantine-topic` (default `quarantine`; the tick line plus `sys_ts`) instead of the ticks topic and prints `routed: <ticks topic>=n <quarantine topic>=n`. Both engines in Kafka mode read the already-routed ticks topic.
- `engine.py` file mode and `reference.py` skip them before anything else and report `quarantined` in `metrics.json`.
- The Flink file mode reads the plain CSV copy, which has no `sys_ts`, so it does not quarantine.

Make: `QUARANTINE_MS=3600000` on `stream`, `reference` and `replay`. `bench-preload` (which then also creates `<BENCH_TOPIC>-quarantine` with retention -1) and `bench` (which then expects that many fewer messages) use 3600000 unless `QUARANTINE_MS` is given on the command line.

On the week, `event_ts - sys_ts` lies in [-1 s, +1 s) for 99.89% of the trades (`sys_ts` has whole seconds). With N = 1 h the rule flags exactly the 5,506 index trades that a 300 ms bound drops (stamped 10.11 01:00:00.000, arriving 18:56:11-13, 1,812 symbols); any N from 1 s to 17.9 h flags the same set. The other direction is not quarantined: 1,175 Xetra closing-auction trades carry event time 17:30-17:31 but a `sys_ts` 1-8.5 h older, yet arrive in order at 17:30, so their system time is stale, not their event time. `reference.py --quarantine-ms 3600000` (no bound) and `engine.py --quarantine-ms 3600000 --out-of-orderness-ms 300` both equal `out/week/reference-300` exactly.

## Heartbeats

Every Kafka partition (CRC32 of the symbol, 8 partitions) keeps trading until 22:25-22:35, the evening index ticks; inside a trading day no partition is quiet for more than 5.0 minutes. With the minimum watermark over partitions, event time therefore stops only overnight: on 08.11 the smallest partition maximum is 22:25:05, so the windows ending 22:30, 22:35 and 22:40 close when the next morning's first ticks arrive, 8.5 hours later in live operation, and Friday's last windows never close in an unbounded job. Flink's `withIdleness` does not help: all partitions go quiet together, and a source whose splits are all idle holds its watermark.

`replay.py --heartbeat-ms N` (off by default) adds punctuation. A heartbeat is the line `__heartbeat__,H,0.0,<H>,0,<ingest_ms>`, produced explicitly to every partition of the ticks topic. The feed clock is the largest `sys_ts` replayed so far; H = feed clock - `--heartbeat-margin-ms` (default 1000). A heartbeat follows each tick that moves the feed clock N ms past the previous heartbeat, and while a paced replay sleeps over a quiet period it goes on every N ms of feed time, up to but not including the next arrival. `--heartbeat-tail-ms T` keeps the feed clock and the heartbeats running for T ms after the last tick, so the final windows close. With the default `--max-gap-ms 60000` a night lasts 60 s of feed time and the effect is invisible; replay with a larger gap (`MAX_GAP_MS=36000000` keeps whole nights).

- Python Kafka mode: a heartbeat advances its partition's maximum event time and seen time. It is never ingested, and its `ingest_ms` is not used for the newest-ingest latency stamp, as in Flink, where the stamp comes from the keyed operator.
- Flink Kafka mode: the line parses as a normal `Tick` (`TickDeserializer` is unchanged), so each split's watermark generator sees H; a filter right after the source (`drop-heartbeats`, chained to it) drops heartbeats before `keyBy`. The filter is always there and changes nothing when the topic has no heartbeats. Control topics, `ControlCatchUp` and the checkpointing switches are unaffected.
- File modes, `reference.py` and `bench.py` are unchanged; `bench.py` expects no heartbeats in its topic.
- `latency.py` counts heartbeats separately (`heartbeats`), not in `ticks` or the hot/tail symbol counts. "Closable" includes them: it is the moment the engine could close the window.

The margin is checked against the data, not argued: `trading/heartbeat_check.py` walks the tick files in replay order and takes, for every tick, the largest H the replay can send before it (the feed clock including the tick's own `sys_ts`, minus the margin).

```bash
.venv/bin/python -m trading.heartbeat_check --ticks data/week/ticks --bound-ms 300 \
  --margins 0 300 500 700 1000 2000 --quarantine-ms 3600000 --out out/heartbeat/check-week-300.json
```

Over the week's 59,386,164 non-quarantined ticks, at a 300 ms bound:

| margin ms | event_ts < H - 300 ms | of these, not already behind the newest replayed event time - 300 ms | in a window the heartbeat watermark already closed |
|---|---|---|---|
| 0 | 12,344 | 461 | 0 |
| 300 | 7,053 | 168 | 0 |
| 500 | 6,921 | 166 | 0 |
| 700 | 5,200 | 0 | 0 |
| 1000 | 5,106 | 0 | 0 |
| 2000 | 4,801 | 0 | 0 |

At 1 s all 5,106 are Xetra trades stamped 17:30:00-17:31:59 (417-662 per minute on each of the five days), the closing auctions; 3,515 of them carry a `sys_ts` at least 1 s older than their event time. They arrive when the feed clock is at 17:31:03-04, and every one of them is already more than 300 ms behind the newest event time replayed before it; without heartbeats 477,549 ticks are behind that tick watermark and none falls into a closed window. The engines drop a tick only when its window is closed, which never happens here. `sys_ts` has whole seconds and every tick outside the quarantine has `event_ts >= sys_ts - 1 s`, hence the 1 s default. On the synthetic data, whose deliberately late ticks are up to 889 s old, a 2 s bound with heartbeats at 1 s puts 72 ticks into closed windows instead of 71.

Effect, measured on 08.11 and 09.11 (23,694,641 ticks) replayed at 600x without gap compression (238 s), bound 300 ms, one run each, with and without `HEARTBEAT_MS=1000 HEARTBEAT_TAIL_MS=300000`; TICKS is a directory with symlinks to the two parquet files, the reference is `reference.py --late-bound-ms 300` on the same files (744,433 EMA rows, 5,016 advisories):

```bash
ENGINE=flink P=2 SPEED=600 OOO=300 TICKS=<two-day dir> MAX_GAP_MS=36000000 HEARTBEAT_MS=1000 HEARTBEAT_TAIL_MS=300000 KEEP_TOPICS=1 OUT=out/heartbeat scripts/latency_run.sh
```

| engine | heartbeats | EMA rows vs reference | 08.11 windows ending 22:25 / 22:30 / 22:35 / 22:40 (83 / 83 / 7 / 7 rows): emitted ms after the replay's feed time passed end + 300 ms | 08.11 windows emitted after 09.11's first tick | 09.11 windows ending 22:30-22:40 (97 rows) | latency from closable p50 / p99 / max ms |
|---|---|---|---|---|---|---|
| Flink p = 2 | - | 744,336: the 97 rows of 09.11 22:30-22:40 missing, the rest identical | 386 / 51,216 / 50,716 / 51,736 | 3 (97 rows) | never | 170 / 686 / 994 |
| Flink p = 2 | 142,806 x 8 partitions | 744,433, identical | 228 / 140 / 258 / 132 | 0 | after 166-278 ms | 178 / 492 / 1,538 |
| Python | - | 744,433, identical | 5,690 / 51,488 / 50,990 / 51,726 | 3 (97 rows) | at the final flush, 80 s later (`never_closable` 97) | 182 / 11,138 / 11,444 |
| Python | 142,806 x 8 partitions | 744,433, identical | 164 / 172 / 176 / 184 | 0 | after 46-62 ms | 235 / 4,724 / 4,770 |

All runs give the reference's 5,016 advisories exactly. The rows of each of these windows are emitted within 4 ms of each other. 51 s at 600x is the 8.5 h from 22:30 to the first tick of 09.11 at 07:00. The overnight wait does not show in the latency from closable: without heartbeats those windows become closable only with the morning ticks, and both engines emit right after them. The Python engine keeps up with 600x on average (99,900 ticks/s) but not at the peaks; its 5.7 s on 08.11 22:25 and its p99 of 4.7-11.1 s are backlog. At 1 s the heartbeats add 1,142,448 records, 4.8% of the ticks. Per run: `out/heartbeat/<run>/` (`latency.json`, `diff.txt`, `evening.json`), all four in `out/heartbeat/summary.md`.

## Metadata and subscriptions

### Metadata, in-band

A metadata row (empty system time in the raw feed) travels in the ticks topic at its position in the feed. `prep.py` writes every metadata row to `metadata_events.parquet` (`seq,symbol,isin,currency`, feed order). `replay.py --with-metadata` sends a row only when it changes the symbol's `isin` or `currency` (an empty field keeps the previous value), keyed and partitioned like the symbol's ticks:

`symbol,M,0.0,<stamp>,<seq>,<ingest_ms>,<isin>,<currency>`

`M` in the sec_type field marks the record (ticks carry `E` or `I`, heartbeats `H`). The stamp is the largest event time sent to the ticks topic before it, ticks and heartbeats (0 before the first). Metadata records never move event time: both engines leave them out of the watermark and the late check.

With `--enrich` (Flink job, `engine.py` Kafka mode) an advisory line gets `isin,currency` after `prev_ema100`, before the latency columns: the latest metadata stamped before window end + out-of-orderness bound, which is what the feed had said about the symbol when the window could first close. A symbol with no metadata yet gets two empty fields. Each engine keeps the versions per symbol (Flink keyed `MapState`, Python a list) and drops the ones no later window can pick. The answer does not depend on timing: a window closes only after the symbol's partition has delivered a record with event time >= window end + bound, and every metadata record stamped below that comes before it in the same partition. Without `--enrich` the engines skip metadata records and the output is unchanged. Flink's file mode takes `--enrich` too and reads metadata lines in the same format when its CSV has them (the CSV copy `prep.py` writes has none).

`reference.py --metadata-events FILE` computes the same two columns in batch: feed clock = running maximum event time of the admitted ticks, then an as-of join at window end + `--late-bound-ms`. `collect.py --enriched` keeps the columns and `diff.py` compares them when both sides have them; `bench.py --enrich` expects a topic preloaded with `--with-metadata`.

An earlier version loaded the week's last value per symbol into a compacted `instruments` topic before the first tick, so Monday's advisories already knew Friday's metadata. Over the week no symbol's `isin` or `currency` ever changes, so `--with-metadata` sends 5,479 rows for 5,478 symbols (A3E5ED.ETR gets its ISIN on 09.11 and its currency on 10.11; 891466.ETR has only empty metadata rows and sends none), and one advisory changes: ICAVE.FR's BUY at 10.11 07:20, whose first metadata row comes 281,157 lines after its first trade, now has empty fields. `metadata.parquet` (latest value per symbol) remains for the dashboard.

Checked in `out/meta-check`: `reference.py --late-bound-ms 300 --metadata-events` gives the same 1,855,645 EMA rows and 11,349 advisories as `reference-300`, 22 of them without metadata (21 symbols that never have any, plus ICAVE.FR). The Flink job in file mode at p = 1, 2 and 4 with `--enrich`, on the week's CSVs with the metadata rows mixed in as `replay.py` sends them, equals it including `isin,currency` (5,506 late ticks, as before). The Python engine's Kafka loop with `--enrich`, fed by a stand-in for the Kafka client that delivers each of the 8 CRC32 partitions in 10,000-record chunks (so the partitions drift apart in event time) and with ticks quarantined at 1 h as `replay.py` does, equals `reference.py --quarantine-ms 3600000 --late-bound-ms 300 --metadata-events` on every row and both columns (`out/meta-check/fake_kafka_engine.py`). Through the broker (17.11): `replay.py --with-metadata --quarantine-ms 3600000` into `bench-week-qm` (59,386,164 ticks + 5,479 metadata rows, 5,506 ticks to the quarantine topic), then `bench.py --enrich --verify out/meta-check/reference-q-meta --python-baseline`: Flink at p = 1, 2, 4 and the Python engine each gave 1,855,645 EMA rows and 11,349 advisories identical to the reference including `isin,currency` (`out/meta-check/bench`).

### Subscriptions

| Topic | Key | Value | Written by |
|---|---|---|---|
| subscriptions | `subscriber\|symbol` | `subscribe`, `unsubscribe` or a tombstone | `trading/subscribers.py` |

`python -m trading.topics create subscriptions --compact` creates it; `subscribers.py` creates it compacted when missing.

The Flink job takes `--subscriptions-topic`. One more Kafka source (parallelism 1) reads it, `keyBy(symbol)`, and is connected to the keyed ticks in `ControlledEmaCrossoverFunction` (a `KeyedCoProcessFunction`). Keyed state per symbol: a map of subscribers, the open windows and EMA state, and with `--enrich` the metadata versions. The EMA is updated for every symbol at every window close; the row and its advisory are emitted only while the symbol has at least one subscriber (giving `--subscriptions FILE` as well is an error). Without the option the job graph and output are unchanged.

Subscription records apply at processing time, from the next window close on. So that symbols subscribed before the job starts get every window, the control stream holds event time back until it has been read up to the end offsets the job saw at start (`ControlCatchUp` then emits a `Long.MAX_VALUE` watermark, so the control input never delays the ticks afterwards). With `--bounded` the control source stops at the end of the topic too.

Dynamic subscriptions are Flink-only; the Python engine keeps the static `--subscriptions FILE`. `latency.py` skips metadata records; `collect.py` reads fixed leading columns plus the last two.

Checked on the first 200,000 day-08 ticks replayed at 60x into an unbounded local job (p = 2), with subscriptions changed halfway: every emitted row equals the reference row with the same key; the four symbols subscribed throughout (one of them losing one of its two subscribers to a tombstone) got all 11 closable windows; the symbols unsubscribed or tombstoned halfway got windows 07:05-07:25 and then none; the symbol subscribed halfway got 07:30-07:55; a key without `|` was ignored.

## Subscriber fan-out

```bash
.venv/bin/python -m trading.subscribers --ticks data/day08/ticks -n 1000 -k 20 --mode popular --topic subscriptions
.venv/bin/python -m trading.gateway --subscriptions-topic subscriptions --from-start --sample 3 --sample-dir out/gateway
```

`subscribers.py` draws K distinct symbols for each of N subscribers (`sub0001`, ...), weighted by tick count (`--mode popular`) or uniformly, with a fixed `--seed`; `--action unsubscribe|tombstone` withdraws the same set, `--symbols-out FILE` writes the union of the subscribed symbols for `reference.py --subscriptions`. `gateway.py` loads the subscriptions topic, keeps following it, and delivers every row of the EMA and advisory topics to each subscriber of its symbol. It counts deliveries per subscriber, writes the deliveries of `--sample N` subscribers to files, and reports deliveries/s over the delivery span and over its busy time, and the delay from the engine's `emit_ms` to delivery.

`scripts/subscriber_scaling.sh` (`make scaling`) preloads day 08 into a scratch topic and, for N = 1, 100, 1,000 and 10,000 subscribers with K = 20 popular symbols, runs `trading.bench` at p = 2 with `--subscriptions-topic` and `--gateway`, verifying each run against `reference.py --subscriptions <union>`. A first run without a subscriptions topic (all symbols emitted) is the baseline. Results go to `out/scaling/`; topics are deleted at the end. Settings (environment, or `make scaling VAR=...`): `COUNTS`, `K`, `MODE`, `P`, `OOO`, `TICKS`, `TOPIC`, `OUT`, `KEEP_TOPIC`, `BASELINE`.

Day 08 (11,794,520 ticks, 4,840 symbols), Flink p = 2 on the pinned cluster, K = 20 popular symbols, one run each (`out/scaling/scaling-20261113-220919.md`):

| subscribers | pairs | symbols | rec/s job | rec/s source | EMA rows | advisories | deliveries | deliveries/s (span) | deliveries/s (busy) | delay p50 / p99 ms | verify |
|---|---|---|---|---|---|---|---|---|---|---|---|
| all (no topic) | - | 4,840 | 1,153,047 | 1,204,260 | 373,126 | 4,840 | - | - | - | - | identical |
| 1 | 20 | 20 | 960,309 | 995,654 | 2,305 | 20 | 2,325 | 241 | 103,678 | 280 / 503 | identical |
| 100 | 2,000 | 1,089 | 1,153,047 | 1,199,727 | 128,021 | 1,089 | 231,515 | 27,702 | 1,242,128 | 225 / 536 | identical |
| 1,000 | 20,000 | 2,889 | 1,030,359 | 1,073,693 | 335,868 | 2,889 | 2,325,663 | 253,400 | 3,512,732 | 87 / 569 | identical |
| 10,000 | 200,000 | 3,486 | 967,002 | 1,000,044 | 365,325 | 3,486 | 23,257,394 | 2,329,896 | 7,429,326 | 218 / 454 | identical |

Each job lasts 10-12 s, so the rates include start-up and vary by about ±10% between runs without a trend in N. "span" divides by the time from the first to the last delivery, which follows the rate at which Flink emits; "busy" divides by the time the gateway spent processing batches. The delay runs from the engine's `emit_ms` to the gateway's delivery. At N = 10,000 the median subscriber received 2,325 rows, the busiest 2,596.

## Fault tolerance (Flink Kafka mode)

All off by default; without these switches the job graph and output are unchanged.

| Switch | Effect |
|---|---|
| `--checkpoint-ms N` | Checkpoint every N ms (hashmap state backend). Adds a fixed-delay restart strategy: `--restart-attempts` (default 10), `--restart-delay-ms` (default 10000) |
| `--checkpoint-mode exactly-once\|at-least-once` | Barrier alignment; default `exactly-once` |
| `--checkpoint-dir URI` | File-system checkpoint storage; without it the JobManager keeps checkpoints in memory. The Docker cluster mounts `./checkpoints` at `/opt/checkpoints` in both containers |
| `--delivery none\|at-least-once\|exactly-once` | Delivery guarantee of both Kafka sinks; default `none` |

`exactly-once` needs `--checkpoint-ms`. Each run gets the transactional id prefix `ema-crossover-<random UUID>-<topic>` (`--transactional-id-prefix` overrides it) and a transaction timeout of 10 minutes (`--transaction-timeout-ms`): the broker allows at most 15 minutes, and the connector's default of 1 hour is rejected. Rows become visible to `read_committed` consumers when the checkpoint after them completes. File mode rejects `--checkpoint-ms`. With `--subscriptions-topic`, checkpointing needs `--bounded`: `ControlCatchUp` is not part of the checkpoint, so after a restore of an unbounded job it would hold event time back until every control partition delivers a new record. Newest-ingest stamps (the latency columns) are not checkpointed either and restart from the next tick.

The Python engine has no checkpointing.

Consumers: `collect.py`, `latency.py` and `topics.py count` take `--isolation read_committed|read_uncommitted`; the default is librdkafka's, `read_committed` (it is also what they used before). `topics.py count` then counts up to the last stable offset or the high watermark; both include transaction markers. `collect.py --dedupe` keeps the first row per (topic, symbol, window_end), checks that every dropped duplicate has the same values (latency columns excluded) and reports `duplicates_dropped`, `duplicates_differing` and up to 5 differing examples in `metrics.json`. `diff.py` keeps the last row per key, so a diff alone does not show duplicates.

`bench.py` passes `--checkpoint-ms`, `--checkpoint-mode`, `--delivery` (and `--checkpoint-dir`, default `file:///opt/checkpoints`) to the job, and `--isolation` and `--dedupe` to the collector of `--verify`. With checkpointing it records `/jobs/<id>/checkpoints` (config, counts, summary, last 10) per run and removes the job's checkpoint folder. `--kill-taskmanager-at SHARE` runs `docker kill taskmanager` once the source has read SHARE of the topic and at least 2 checkpoints completed, then starts the TaskManager again with the same `TM_SLOTS`/`TM_CPUSET` (`compose up -d --no-deps`), and records the restart: exception history, attempts, `numRestarts`, the restored checkpoint, and seconds from the kill to the failure being detected, to the restore, to the source reading again and to all tasks running. `--keep-topics` leaves the output topics for further collects.

`scripts/latency_run.sh` takes `CHECKPOINT_MS`, `CHECKPOINT_MODE`, `DELIVERY` (Flink only), `ISOLATION` (default `read_committed`) and `OUT` (default `out/latency`). Before stopping Flink it waits until the result topics are stable and their last stable offset equals the high watermark, so no rows are left in an open transaction. `RECEIVED=1` also runs `latency.py --follow`, which reads the EMA topic live with `ISOLATION` and writes each row's receive time; `latency.py --received` then reports `latency_ms_from_closable_received`, the latency to the moment a consumer could read the row. The engine stamp does not include the wait for the transaction commit.

Recovery test, p = 2, draining `bench-week-q` with 10 s checkpoints (`out/ft/recovery`):

```bash
make bench BENCH_P=2 BENCH_RUNS=1 BENCH_ARGS="--checkpoint-ms 10000 --delivery at-least-once --kill-taskmanager-at 0.4 --keep-topics --verify out/week/reference-300 --dedupe"
make bench BENCH_P=2 BENCH_RUNS=1 BENCH_ARGS="--checkpoint-ms 10000 --delivery exactly-once --kill-taskmanager-at 0.4 --keep-topics --verify out/week/reference-300 --isolation read_committed"
```

| delivery | killed at | restored from | restarts | detected / restored / source reading / all running (s after kill) | rows read | duplicates | after dedupe |
|---|---|---|---|---|---|---|---|
| at-least-once | 24,095,469 ticks (40.6%), 3 checkpoints done | chk-3, completed 4.2 s before the kill | 1 | 16.6 / 26.7 / 28.9 / 30.9 | 1,992,300 EMA + 11,380 advisories | 136,655 + 31, all equal to the kept row | identical to `reference-300` |
| exactly-once, `read_committed` | 24,660,204 ticks (41.5%), 2 checkpoints done | chk-2, completed 4.6 s before the kill | 1 | 11.2 / 21.2 / 23.2 / 26.3 | 1,855,645 + 11,349 | 0 | identical without dedupe |
| exactly-once, `read_uncommitted` | same run | | | | 1,989,014 + 11,362 | 133,369 + 13 from aborted transactions, all equal to the committed row | identical |

The failure is detected when two heartbeat RPCs to the lost TaskManager fail ("TaskManager ... is no longer reachable"); 10 s of the restore time is the restart delay. The checkpoints triggered while the TaskManager was down fail ("Trigger checkpoint failure") without failing the job.

Overhead, `bench-week-q` drained with `--verify out/week/reference-300` (every run identical), configurations alternated in rotating order (none, ALO, EO / ALO, EO, none / EO, none, ALO); median [min-max] of 3 runs at p = 2, one run at p = 4 (`out/ft/throughput`). ALO and EO are `--checkpoint-ms 10000` with `--delivery at-least-once` or `exactly-once`, checkpoint mode exactly-once:

| p | configuration | rec/s job | rec/s source | rec/s steady | CPU % broker / JM / TM | checkpoints per run | checkpoint duration p50 / max ms | checkpoint size p50 / max MB |
|---|---|---|---|---|---|---|---|---|
| 2 | none | 931,635 [829,601-1,394,926] | 939,625 [834,978-1,418,550] | 1,026,099 [952,266-1,694,729] | 14.4 / 15.9 / 200.2 | - | - | - |
| 2 | ALO | 1,093,809 [782,849-1,160,770] | 1,104,099 [795,933-1,170,863] | 1,304,601 [803,221-1,469,142] | 14.6 / 16.7 / 196.5 | 6-8 | 402 [287-406] / 1,170 [870-1,332] | 8.0 [7.3-10.5] / 20.0 [19.5-20.6] |
| 2 | EO | 1,229,044 [1,142,371-1,255,389] | 1,240,831 [1,155,620-1,272,088] | 1,350,412 [1,313,973-1,401,579] | 12.8 / 11.5 / 192.0 | 5-6 | 378 [313-406] / 564 [499-704] | 9.4 [4.6-9.4] / 19.2 [16.2-19.3] |
| 4 | none | 1,604,381 | 1,643,316 | 1,948,668 | 31.4 / 20.6 / 380.3 | - | - | - |
| 4 | ALO | 1,162,725 | 1,230,623 | 1,314,393 | 25.9 / 32.1 / 381.0 | 5 | 333 / 638 | 6.7 / 18.2 |
| 4 | EO | 1,368,976 | 1,400,089 | 1,602,370 | 28.0 / 27.2 / 387.7 | 5 | 374 / 1,353 | 11.9 / 19.8 |

At p = 2 the job rate per round was 1,394,926 / 1,160,770 / 1,255,389 (none, ALO, EO), 1,093,809 / 1,229,044 / 931,635 (ALO, EO, none) and 1,142,371 / 829,601 / 782,849 (EO, none, ALO): the host slowed down during the series by more than the differences between configurations. In every configuration the job ends 0.2-0.9 s after its source finishes, so the final checkpoint that commits the last transactions adds no visible wait. Topic offsets include transaction markers (for example 1,855,741 offsets for 1,855,645 EMA rows).

Latency, `ENGINE=flink P=2 SPEED=300 LIMIT=60000 OOO=2000 RECEIVED=1` (`out/ft/latency`), configurations alternated, two runs each, ms from the moment the window could close:

| configuration | rows | to the engine stamp p50 / p99 / max | to the receive time (`read_committed`) p50 / p99 / max |
|---|---|---|---|
| none | 14,444, 14,444 | 198 / 301 / 373, 188 / 321 / 400 | 212 / 311 / 403, 199 / 417 / 495 |
| `CHECKPOINT_MS=1000 DELIVERY=at-least-once` | 14,444, 14,444 | 173 / 310 / 365, 190 / 305 / 310 | 185 / 354 / 423, 202 / 318 / 329 |
| `CHECKPOINT_MS=1000 DELIVERY=exactly-once` | 14,444, 14,444 | 181 / 309 / 310, 189 / 318 / 320 | 492 / 722 / 769, 784 / 919 / 1,016 |
| `CHECKPOINT_MS=5000 DELIVERY=exactly-once` | 14,444, 14,444 | 192 / 309 / 309, 181 / 434 / 1,409 | 2,638 / 4,657 / 4,678, 3,098 / 5,154 / 5,240 |

## Switches

`--out-of-orderness-ms`, `--suppress-first` and `--subscriptions FILE|all` work in `engine.py` and the Flink job; `reference.py` takes the last two, plus `--late-bound-ms N`, which drops exactly the events a stream with bound N drops (by default the reference keeps every event). `--empty-window skip|repeat` exists in `engine.py` and `reference.py` only; the Flink job implements `skip`. `--quarantine-ms N` works in `replay.py`, `engine.py` file mode and `reference.py`; `--with-metadata` and `--metadata-events FILE` in `replay.py`, `--enrich` in the Flink job and `engine.py` Kafka mode, `--metadata-events FILE` in `reference.py`, `--enriched` in `collect.py`; `--subscriptions-topic` in the Flink job only; `--checkpoint-ms`, `--checkpoint-mode`, `--checkpoint-dir` and `--delivery` in the Flink job's Kafka mode only (see Fault tolerance); `--isolation` in `collect.py`, `latency.py` and `topics.py count`; `--heartbeat-ms`, `--heartbeat-margin-ms` and `--heartbeat-tail-ms` in `replay.py` (both engines' Kafka modes always recognise heartbeat and metadata records, see Heartbeats and Metadata). Make variables: `OOO`, `SUBS`, `TICKS`, `META`, `SPEED`, `DAYS`, `QUARANTINE_MS`.
