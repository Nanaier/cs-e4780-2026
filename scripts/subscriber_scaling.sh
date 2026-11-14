#!/usr/bin/env bash
set -euo pipefail

py="${PY:-.venv/bin/python}"
topic="${TOPIC:-scale-day08}"
ticks="${TICKS:-data/day08/ticks}"
counts="${COUNTS:-1 100 1000 10000}"
k="${K:-20}"
mode="${MODE:-popular}"
p="${P:-2}"
ooo="${OOO:-300}"
out="${OUT:-out/scaling}"
work="$(mktemp -d "${TMPDIR:-/tmp}/scaling.XXXXXX")"
trap 'rm -rf "$work"' EXIT
runs=()

latest_run() {
  ls -dt "$out/$topic"-* | head -1
}

"$py" -m trading.topics create "$topic" --retention-ms -1
sleep 2
if [ "$("$py" -m trading.topics count "$topic" | cut -d' ' -f2)" = "0" ]; then
  "$py" -m trading.replay --ticks "$ticks" --topic "$topic" --speed 0 | tail -1
fi

if [ "${BASELINE:-1}" = "1" ]; then
  "$py" -m trading.reference --ticks "$ticks" --late-bound-ms "$ooo" --out "$work/ref-all" > /dev/null
  "$py" -m trading.bench --topic "$topic" --ticks "$ticks" --parallelism "$p" --runs 1 --ooo-ms "$ooo" \
    --out "$out" --verify "$work/ref-all"
  runs+=("$(latest_run)")
  rm -rf "$work/ref-all"
fi
for n in $counts; do
  subs="$topic-subs-$n"
  "$py" -m trading.subscribers --ticks "$ticks" -n "$n" -k "$k" --mode "$mode" --topic "$subs" \
    --symbols-out "$work/symbols-$n.txt" > "$work/subscribers-$n.json"
  "$py" -m trading.reference --ticks "$ticks" --late-bound-ms "$ooo" --subscriptions "$work/symbols-$n.txt" \
    --out "$work/ref-$n" > /dev/null
  "$py" -m trading.bench --topic "$topic" --ticks "$ticks" --parallelism "$p" --runs 1 --ooo-ms "$ooo" \
    --out "$out" --subscriptions-topic "$subs" --gateway --verify "$work/ref-$n"
  run_dir="$(latest_run)"
  runs+=("$run_dir")
  cp "$work/subscribers-$n.json" "$run_dir/subscribers.json"
  "$py" -m trading.topics delete "$subs"
  rm -rf "$work/ref-$n"
done
if [ "${KEEP_TOPIC:-0}" != "1" ]; then
  "$py" -m trading.topics delete "$topic"
fi

"$py" - "${runs[@]}" <<'EOF' | tee "$out/scaling-$(date +%Y%m%d-%H%M%S).md"
import json, sys
from pathlib import Path
head = ["subscribers", "pairs", "symbols", "rec/s job", "rec/s source", "rec/s steady", "EMA rows", "advisories",
        "deliveries", "deliveries/s", "deliveries/busy s", "delay p50/p99 ms", "verify"]
print("| " + " | ".join(head) + " |")
print("|" + "---|" * len(head))
fmt = lambda v: "-" if v is None else f"{v:,}"
for run in sys.argv[1:]:
    run = Path(run)
    r = json.loads(next(run.glob("flink-p*-r1.json")).read_text())
    s = json.loads((run / "subscribers.json").read_text()) if (run / "subscribers.json").exists() else {}
    g = r.get("gateway", {})
    t = r["throughput"]
    delay = g.get("delay_ms_emit_to_delivery", {})
    print("| " + " | ".join([
        fmt(s.get("subscribers")) if s else "all (no topic)", fmt(s.get("pairs")), fmt(s.get("union_symbols")),
        fmt(t["records_per_s_job"]), fmt(t["records_per_s_source"]), fmt(t["records_per_s_steady"]),
        fmt(r["outputs"]["ema"]), fmt(r["outputs"]["advisories"]), fmt(g.get("counts", {}).get("deliveries")),
        fmt(g.get("deliveries_per_s")), fmt(g.get("deliveries_per_busy_s")),
        f"{fmt(delay.get('p50'))} / {fmt(delay.get('p99'))}" if delay else "-",
        "identical" if r.get("verify", {}).get("identical") else "DIFFERS"]) + " |")
EOF
