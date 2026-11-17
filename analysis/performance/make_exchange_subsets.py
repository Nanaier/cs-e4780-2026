"""Build the exchange subsets for the exchange-scaling runs.

Writes one tick folder per subset, with the same per-day file names and the same row (feed) order as the
full week, so a subset replays exactly like the week with the other exchanges' ticks taken out:

    data/subsets/nl/ticks          Amsterdam only
    data/subsets/nl-fr/ticks       Amsterdam + Paris
    (all three exchanges = data/week/ticks, the full week)

Usage: .venv/bin/python analysis/performance/make_exchange_subsets.py [--week data/week/ticks] [--out data/subsets]
"""

import argparse
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq

SUBSETS = {"nl": ["NL"], "nl-fr": ["NL", "FR"]}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--week", default="data/week/ticks", help="full-week tick folder (parquet per day)")
    parser.add_argument("--out", default="data/subsets")
    args = parser.parse_args()

    day_files = sorted(Path(args.week).glob("*.parquet"))
    if not day_files:
        raise SystemExit(f"no parquet files in {args.week}")

    for name, exchanges in SUBSETS.items():
        target = Path(args.out) / name / "ticks"
        target.mkdir(parents=True, exist_ok=True)
        total = 0
        for day in day_files:
            table = pq.read_table(day)
            # filter() keeps the original row order, which is the feed order the replay relies on.
            kept = table.filter(pc.is_in(table["exchange"], value_set=pc.cast(exchanges, table["exchange"].type)))
            pq.write_table(kept, target / day.name)
            total += kept.num_rows
        print(f"{name}: {'+'.join(exchanges)} -> {total:,} ticks in {target}")


if __name__ == "__main__":
    main()
