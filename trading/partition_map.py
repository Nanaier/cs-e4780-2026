import argparse
import heapq

import duckdb

from trading.files import tick_files


def build(paths, partitions):
    counts = duckdb.connect().execute(
        "SELECT symbol, count(*) FROM read_parquet(?) GROUP BY 1 ORDER BY 2 DESC, 1", [[str(p) for p in paths]]).fetchall()
    heap = [(0, p) for p in range(partitions)]
    mapping = {}
    for symbol, n in counts:
        load, p = heapq.heappop(heap)
        mapping[symbol] = p
        heapq.heappush(heap, (load + n, p))
    return mapping, sorted(load for load, _ in heap)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Balanced symbol-to-partition map from tick counts (largest symbols first, each to the least loaded partition)")
    parser.add_argument("--ticks", required=True, help="tick parquet file or directory used as the statistics, e.g. the previous day")
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    mapping, loads = build(tick_files(args.ticks), args.partitions)
    with open(args.out, "w") as f:
        f.writelines(f"{s},{p}\n" for s, p in sorted(mapping.items()))
    print(f"{len(mapping)} symbols, partition loads min {loads[0]:,} max {loads[-1]:,} ({loads[-1] * len(loads) / sum(loads):.3f}x mean)")


if __name__ == "__main__":
    main()
