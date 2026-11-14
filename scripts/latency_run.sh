#!/usr/bin/env bash
set -euo pipefail

engine="${ENGINE:-flink}"
speed="${SPEED:-60}"
limit="${LIMIT:-0}"
ticks="${TICKS:-data/synthetic/ticks}"
ooo="${OOO:-300}"
p="${P:-2}"
py="${PY:-.venv/bin/python}"
keep_topics="${KEEP_TOPICS:-0}"
settle_s="${SETTLE_S:-10}"
max_wait_s="${MAX_WAIT_S:-600}"
checkpoint_ms="${CHECKPOINT_MS:-}"
checkpoint_mode="${CHECKPOINT_MODE:-}"
delivery="${DELIVERY:-}"
isolation="${ISOLATION:-read_committed}"
received="${RECEIVED:-0}"
max_gap_ms="${MAX_GAP_MS:-}"
heartbeat_ms="${HEARTBEAT_MS:-}"
heartbeat_tail_ms="${HEARTBEAT_TAIL_MS:-}"
heartbeat_margin_ms="${HEARTBEAT_MARGIN_MS:-}"
out_base="${OUT:-out/latency}"
flink_main=fi.aalto.cse4780.trading.EmaCrossoverJob

if [ "$speed" = "0" ]; then
  echo "SPEED must be > 0: latency needs a paced replay (use bench for throughput)" >&2
  exit 1
fi
if [ "$engine" = "python" ]; then
  p=1
  if [ -n "$checkpoint_ms$checkpoint_mode$delivery" ]; then
    echo "CHECKPOINT_MS, CHECKPOINT_MODE and DELIVERY apply to the Flink job only" >&2
    exit 1
  fi
fi
fault_tolerance=()
tag=""
if [ -n "$checkpoint_ms" ]; then
  fault_tolerance+=(--checkpoint-ms "$checkpoint_ms")
  tag="$tag-c$checkpoint_ms"
fi
if [ -n "$checkpoint_mode" ]; then
  fault_tolerance+=(--checkpoint-mode "$checkpoint_mode")
fi
if [ -n "$delivery" ]; then
  fault_tolerance+=(--delivery "$delivery")
  tag="$tag-$delivery"
fi
replay_args=()
if [ -n "$max_gap_ms" ]; then
  replay_args+=(--max-gap-ms "$max_gap_ms")
  tag="$tag-g$max_gap_ms"
fi
if [ -n "$heartbeat_ms" ]; then
  replay_args+=(--heartbeat-ms "$heartbeat_ms")
  tag="$tag-hb$heartbeat_ms"
elif [ -n "$heartbeat_tail_ms$heartbeat_margin_ms" ]; then
  echo "HEARTBEAT_TAIL_MS and HEARTBEAT_MARGIN_MS need HEARTBEAT_MS" >&2
  exit 1
fi
if [ -n "$heartbeat_tail_ms" ]; then
  replay_args+=(--heartbeat-tail-ms "$heartbeat_tail_ms")
fi
if [ -n "$heartbeat_margin_ms" ]; then
  replay_args+=(--heartbeat-margin-ms "$heartbeat_margin_ms")
fi

run="$(date +%s)"
name="$engine-p$p-x$speed$tag-$run"
out="$out_base/$name"
mkdir -p "$out"
ticks_topic="lat-ticks-$run"
ema_topic="lat-ema-$run"
adv_topic="lat-adv-$run"
idle_exit="$($py -c "print(int(max(30, ${max_gap_ms:-60000} / 1000 / $speed + 20)))")"
pid=""
follower=""
follower_out=""

cleanup() {
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
  if [ -n "$follower" ] && kill -0 "$follower" 2>/dev/null; then
    kill "$follower" 2>/dev/null || true
    wait "$follower" 2>/dev/null || true
  fi
  if [ "$keep_topics" != "1" ]; then
    "$py" -m trading.topics delete "$ticks_topic" "$ema_topic" "$adv_topic" || true
  fi
}
trap cleanup EXIT

die() {
  echo "FAILED $name: $*" >&2
  echo "--- tail of $out/engine.log" >&2
  tail -20 "$out/engine.log" >&2 || true
  exit 1
}

require_alive() {
  if ! kill -0 "$pid" 2>/dev/null; then
    status=0
    wait "$pid" 2>/dev/null || status=$?
    pid=""
    die "engine exited early ($1, exit status $status)"
  fi
}

result_count() {
  "$py" -m trading.topics count "$ema_topic" "$adv_topic" --isolation "$1" | awk '{ n += $2 } END { print n + 0 }'
}

"$py" -m trading.topics create "$ticks_topic" "$ema_topic" "$adv_topic"
if [ "$received" = "1" ]; then
  follower_out="$out/received.csv"
  "$py" -m trading.latency --follow --results-topic "$ema_topic" --isolation "$isolation" \
    --received "$follower_out" > "$out/follow.log" 2>&1 &
  follower=$!
fi

case "$engine" in
  python)
    "$py" -m trading.engine --kafka localhost:9092 --topic "$ticks_topic" --group "lat-$run" \
      --out "$out/engine" --out-of-orderness-ms "$ooo" --idle-partition-ms 86400000 --idle-exit-s "$idle_exit" \
      --ema-topic "$ema_topic" --advisories-topic "$adv_topic" > "$out/engine.log" 2>&1 &
    pid=$!
    sleep 3
    ;;
  flink)
    java -cp "flink-job/target/classes:$(cat flink-job/target/classpath.txt)" "$flink_main" --source kafka \
      --bootstrap localhost:9092 --topic "$ticks_topic" --group "lat-$run" --out-of-orderness-ms "$ooo" \
      --parallelism "$p" --ema-topic "$ema_topic" --advisories-topic "$adv_topic" \
      ${fault_tolerance[@]+"${fault_tolerance[@]}"} > "$out/engine.log" 2>&1 &
    pid=$!
    sleep 25
    ;;
  *)
    echo "ENGINE must be python or flink" >&2
    exit 1
    ;;
esac
require_alive "before the replay"

"$py" -m trading.replay --ticks "$ticks" --topic "$ticks_topic" --speed "$speed" --limit "$limit" \
  ${replay_args[@]+"${replay_args[@]}"} > "$out/replay.log" 2>&1
require_alive "during the replay"

if [ "$engine" = "python" ]; then
  status=0
  wait "$pid" || status=$?
  pid=""
  [ "$status" = "0" ] || die "engine failed with exit status $status"
else
  replay_end=$SECONDS
  last=-1
  committed=-2
  stable_since=$SECONDS
  while [ $((SECONDS - stable_since)) -lt "$settle_s" ] || [ "$committed" != "$last" ]; do
    require_alive "while draining"
    n="$(result_count read_uncommitted)"
    committed="$(result_count read_committed)"
    if [ "$n" != "$last" ]; then
      last=$n
      stable_since=$SECONDS
    fi
    if [ $((SECONDS - replay_end)) -ge "$max_wait_s" ]; then
      die "results still growing or uncommitted ${max_wait_s}s after the replay ended ($n offsets, $committed committed)"
    fi
    sleep 1
  done
  [ "$last" -gt 0 ] || die "no results after ${settle_s}s of silence"
  echo "drained: $last result offsets, all committed, stable ${settle_s}s, $((stable_since - replay_end))s after the replay ended"
  kill "$pid"
  wait "$pid" 2>/dev/null || true
  pid=""
fi
if [ -n "$follower" ]; then
  kill "$follower"
  wait "$follower" || die "follower failed, see $out/follow.log"
  follower=""
fi

"$py" -m trading.latency --ticks-topic "$ticks_topic" --results-topic "$ema_topic" --bound-ms "$ooo" \
  --isolation "$isolation" ${follower_out:+--received "$follower_out"} --out "$out/latency.json" > /dev/null
tail -1 "$out/replay.log"
"$py" -c "
import json
m = json.load(open('$out/latency.json'))
print('$name', 'rows', m['counts']['rows'], 'never_closable', m['counts']['never_closable'],
      'before_closable', m['counts']['emitted_before_closable'])
for k in ('latency_ms_from_closable', 'latency_ms_from_closable_hot', 'latency_ms_from_closable_tail',
          'window_complete_ms', 'window_emit_spread_ms', 'latency_ms_engine_stamp', 'latency_ms_from_closable_received'):
    if k in m:
        print(f'  {k:32s}', m[k])
if 'not_received' in m['counts']:
    print('  not_received', m['counts']['not_received'])
"
echo "results in $out"
