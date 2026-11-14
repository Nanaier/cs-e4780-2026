PY ?= .venv/bin/python
TICKS ?= data/synthetic/ticks
META ?= data/synthetic/metadata.parquet
OOO ?= 2000
SUBS ?= all
SPEED ?= 0
QUARANTINE_MS ?=
QUARANTINE = $(if $(QUARANTINE_MS),--quarantine-ms $(QUARANTINE_MS))
DAYS ?= 08-11-21 09-11-21 10-11-21 11-11-21 12-11-21
SAMPLES ?= $(HOME)/Downloads/debs2022-gc-trading-day-13-11-21.csv $(HOME)/Downloads/debs2022-gc-trading-day-14-11-21.csv

FLINK_CP = flink-job/target/classes:$(shell cat flink-job/target/classpath.txt 2>/dev/null)
FLINK_MAIN = fi.aalto.cse4780.trading.EmaCrossoverJob
P ?= 4
TM_CPUSET ?= 0-3
BENCH_TOPIC ?= bench-week-q
BENCH_TICKS ?= data/week/ticks
BENCH_P ?= 1 2 4
BENCH_RUNS ?= 3
BENCH_OOO ?= 300
BENCH_ARGS ?=

.PHONY: venv test synthetic samples fetch-week filter-local prep-week stream reference diff dashboard demo kafka-up kafka-down replay stream-kafka flink-build flink-file flink-kafka diff-flink collect cluster-up cluster-down cluster-submit bench-preload bench scaling

venv:
	python3.12 -m venv .venv
	$(PY) -m pip install -r requirements.txt

test:
	$(PY) -m unittest discover -s tests -t . -v

synthetic:
	$(PY) -m trading.synth --out-dir data/raw
	$(PY) -m trading.prep --out data/synthetic data/raw/synthetic-trading-day-*.csv

samples:
	$(PY) -m trading.prep --out data/samples $(SAMPLES)

fetch-week:
	for d in $(DAYS); do scripts/fetch_day.sh $$d data/filtered; done

filter-local:
	for d in $(DAYS); do scripts/fetch_day.sh $(HOME)/Downloads/debs2022-gc-trading-day-$$d.csv data/filtered; done

prep-week:
	PY=$(PY) scripts/prep_week.sh $(DAYS)

stream:
	$(PY) -m trading.engine --ticks $(TICKS) --out out/stream --out-of-orderness-ms $(OOO) --subscriptions $(SUBS) $(QUARANTINE)

reference:
	$(PY) -m trading.reference --ticks $(TICKS) --out out/reference --subscriptions $(SUBS) $(QUARANTINE)

diff:
	$(PY) -m trading.diff out/stream out/reference

dashboard:
	$(PY) -m trading.viz --results out/stream --metadata $(META)

demo: synthetic stream reference dashboard
	-$(PY) -m trading.diff out/stream out/reference

kafka-up:
	docker compose up -d

kafka-down:
	docker compose down

replay:
	$(PY) -m trading.replay --ticks $(TICKS) --speed $(SPEED) $(QUARANTINE)

stream-kafka:
	$(PY) -m trading.engine --kafka localhost:9092 --topic ticks --out out/stream-kafka --out-of-orderness-ms $(OOO) --subscriptions $(SUBS)

flink-build:
	mvn -q -f flink-job/pom.xml package

flink-file:
	java -cp "$(FLINK_CP)" $(FLINK_MAIN) --source file --input $(TICKS) --out out/flink --out-of-orderness-ms $(OOO) --subscriptions $(SUBS) --parallelism $(P)

flink-kafka:
	java -cp "$(FLINK_CP)" $(FLINK_MAIN) --source kafka --bootstrap localhost:9092 --topic ticks --out-of-orderness-ms $(OOO) --subscriptions $(SUBS) --parallelism $(P)

diff-flink:
	$(PY) -m trading.diff out/flink out/reference

collect:
	$(PY) -m trading.collect --out out/collected

cluster-up:
	TM_SLOTS=$(P) TM_CPUSET=$(TM_CPUSET) docker compose -f docker-compose.yml -f docker-compose.flink.yml up -d

cluster-down:
	docker compose -f docker-compose.yml -f docker-compose.flink.yml down

cluster-submit:
	docker exec jobmanager flink run -d /opt/job/ema-crossover-flink-1.0.0.jar --source kafka --bootstrap broker:19092 --topic ticks --out-of-orderness-ms $(OOO) --parallelism $(P)

bench-preload bench: QUARANTINE_MS = 3600000

bench-preload:
	$(PY) -m trading.topics create $(BENCH_TOPIC) $(if $(QUARANTINE_MS),$(BENCH_TOPIC)-quarantine) --retention-ms -1
	@test "$$($(PY) -m trading.topics count $(BENCH_TOPIC) | cut -d' ' -f2)" = 0 || { echo "$(BENCH_TOPIC) is not empty; delete it first"; exit 1; }
	$(PY) -m trading.replay --ticks $(BENCH_TICKS) --topic $(BENCH_TOPIC) --speed 0 $(QUARANTINE) $(if $(QUARANTINE_MS),--quarantine-topic $(BENCH_TOPIC)-quarantine)

bench:
	$(PY) -m trading.bench --topic $(BENCH_TOPIC) --ticks $(BENCH_TICKS) --parallelism $(BENCH_P) --runs $(BENCH_RUNS) --ooo-ms $(BENCH_OOO) --out out/bench $(QUARANTINE) $(BENCH_ARGS)

scaling:
	PY=$(PY) scripts/subscriber_scaling.sh
