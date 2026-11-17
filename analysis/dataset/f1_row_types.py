import json
from datetime import date

from common import KINDS, WEEKEND_STATS, day_of, pct, write_csv

COLUMNS = ["housekeeping", "quote", "trade", "other", "zero_or_invalid_price", "metadata"]
RENAME = {"price": "trade", "price_zero_or_invalid": "zero_or_invalid_price", "price_zero": "zero_or_invalid_price"}


def weekday_counts():
    for path in KINDS:
        counts = {}
        for line in path.read_text().split("\n"):
            if line.strip():
                kind, value = line.split()
                counts[RENAME.get(kind, kind)] = int(value)
        yield day_of(path), counts


def weekend_counts():
    for path in WEEKEND_STATS:
        kinds = json.loads(path.read_text())["kinds"]
        yield day_of(path), {RENAME.get(kind, kind): value for kind, value in kinds.items()}


def main():
    days = [(day, counts, True) for day, counts in weekday_counts()]
    days += [(day, counts, False) for day, counts in weekend_counts()]
    by_day = []
    for day, counts, _ in days:
        total = sum(counts.values())
        by_day.append([day, date.fromisoformat(day).strftime("%a"), total] + [counts.get(c, 0) for c in COLUMNS])
    write_csv("f1_row_types_by_day", ["day", "weekday", "rows"] + COLUMNS, by_day)

    weekday_total = {c: sum(counts.get(c, 0) for _, counts, weekday in days if weekday) for c in COLUMNS}
    weekend_total = {c: sum(counts.get(c, 0) for _, counts, weekday in days if not weekday) for c in COLUMNS}
    rows_weekdays = sum(weekday_total.values())
    rows_weekend = sum(weekend_total.values())
    summary = [[c, weekday_total[c], pct(weekday_total[c], rows_weekdays), weekend_total[c]] for c in COLUMNS]
    summary.append(["all", rows_weekdays, 100.0, rows_weekend])
    write_csv("f1_row_types_summary", ["kind", "weekday_rows", "weekday_pct", "weekend_rows"], summary)
    for row in summary:
        print(*row)


if __name__ == "__main__":
    main()
