#!/usr/bin/env bash
set -euo pipefail

if [ $# -lt 1 ]; then
  echo "usage: $0 DD-MM-YY|path/to/raw.csv [out_dir]" >&2
  echo "env: PARALLEL=N ranged connections for downloads (default 8; 1 = single stream, no temporary files)" >&2
  exit 1
fi

source_arg="$1"
out_dir="${2:-data/filtered}"
parallel="${PARALLEL:-8}"
here="$(cd "$(dirname "$0")" && pwd)"
mkdir -p "$out_dir"

if [ -f "$source_arg" ]; then
  name="$(basename "$source_arg")"
  url=""
else
  name="debs2022-gc-trading-day-${source_arg}.csv"
  url="https://zenodo.org/records/6382482/files/${name}?download=1"
fi

target="$out_dir/${name}.gz"
kinds="$out_dir/${name%.csv}.kinds.txt"
start=$(date +%s)

filter() {
  LC_ALL=C awk -f "$here/filter.awk" 2> "$kinds" | gzip -1 > "$target.part"
}

if [ -z "$url" ]; then
  cat "$source_arg" | filter
elif [ "$parallel" -le 1 ]; then
  curl -fL --retry 5 --retry-delay 10 "$url" | filter
else
  size=$(curl -sfIL "$url" | tr -d '\r' | awk 'tolower($1) == "content-length:" { n = $2 } END { print n }')
  parts_dir="$out_dir/.parts-${name%.csv}"
  mkdir -p "$parts_dir"
  chunk=$(( (size + parallel - 1) / parallel ))
  pids=()
  for i in $(seq 0 $(( parallel - 1 ))); do
    from=$(( i * chunk ))
    to=$(( from + chunk - 1 ))
    if [ "$to" -ge "$size" ]; then to=$(( size - 1 )); fi
    part="$parts_dir/$(printf '%03d' "$i")"
    if [ -f "$part" ] && [ "$(stat -f%z "$part" 2>/dev/null || stat -c%s "$part")" -eq $(( to - from + 1 )) ]; then
      continue
    fi
    curl -sfL --retry 8 --retry-delay 10 -r "${from}-${to}" -o "$part" "$url" &
    pids+=($!)
  done
  for pid in "${pids[@]}"; do wait "$pid"; done
  got=$(cat "$parts_dir"/* | wc -c | tr -d ' ')
  if [ "$got" -ne "$size" ]; then
    echo "downloaded $got of $size bytes; rerun to resume" >&2
    exit 1
  fi
  echo "downloaded $size bytes in $(( $(date +%s) - start )) s with $parallel connections"
  cat "$parts_dir"/* | filter
  rm -rf "$parts_dir"
fi

mv "$target.part" "$target"
echo "$target written in $(( $(date +%s) - start )) s"
sort "$kinds"
