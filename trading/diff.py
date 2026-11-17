import argparse
import csv
import math
import sys
from pathlib import Path


def header(path):
    with Path(path).open() as fh:
        return next(csv.reader(fh))


def load(path, key_fields, value_fields):
    rows = {}
    with Path(path).open() as fh:
        for record in csv.DictReader(fh):
            key = tuple(record[k] for k in key_fields)
            rows[key] = tuple(record[v] for v in value_fields)
    return rows


def same_number(a, b, rel_tol):
    x, y = float(a), float(b)
    return math.isclose(x, y, rel_tol=rel_tol, abs_tol=1e-12)


def compare(name, left, right, numeric, rel_tol, show):
    only_left = sorted(set(left) - set(right))
    only_right = sorted(set(right) - set(left))
    mismatched = []
    for key in set(left) & set(right):
        for index, (a, b) in enumerate(zip(left[key], right[key])):
            ok = same_number(a, b, rel_tol) if numeric[index] else a == b
            if not ok:
                mismatched.append((key, left[key], right[key]))
                break
    print(f"{name}: left={len(left)} right={len(right)} matched={len(set(left) & set(right)) - len(mismatched)} "
          f"mismatched={len(mismatched)} only_left={len(only_left)} only_right={len(only_right)}")
    for key in only_left[:show]:
        print(f"  only left  {key} {left[key]}")
    for key in only_right[:show]:
        print(f"  only right {key} {right[key]}")
    for key, a, b in sorted(mismatched)[:show]:
        print(f"  differs    {key} left={a} right={b}")
    return not (only_left or only_right or mismatched)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Compare two result directories (ema.csv and advisories.csv)")
    parser.add_argument("left")
    parser.add_argument("right")
    parser.add_argument("--rel-tol", type=float, default=1e-9)
    parser.add_argument("--show", type=int, default=5)
    args = parser.parse_args(argv)

    ema_ok = compare(
        "ema",
        load(Path(args.left, "ema.csv"), ["symbol", "window_end_ms"], ["close", "ema38", "ema100"]),
        load(Path(args.right, "ema.csv"), ["symbol", "window_end_ms"], ["close", "ema38", "ema100"]),
        [True, True, True], args.rel_tol, args.show,
    )
    left_adv, right_adv = Path(args.left, "advisories.csv"), Path(args.right, "advisories.csv")
    enriched = all("isin" in header(p) for p in (left_adv, right_adv))
    fields, numeric = ["advisory", "ema38", "ema100"], [False, True, True]
    if enriched:
        fields, numeric = fields + ["isin", "currency"], numeric + [False, False]
    adv_ok = compare(
        "advisories" + (" (with isin,currency)" if enriched else ""),
        load(left_adv, ["symbol", "window_end_ms"], fields),
        load(right_adv, ["symbol", "window_end_ms"], fields),
        numeric, args.rel_tol, args.show,
    )
    sys.exit(0 if ema_ok and adv_ok else 1)


if __name__ == "__main__":
    main()
