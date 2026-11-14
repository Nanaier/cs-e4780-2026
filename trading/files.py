import csv
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

EMA_HEADER = ["symbol", "window_end_ms", "window_end", "close", "ema38", "ema100"]
ADVISORY_HEADER = ["symbol", "window_end_ms", "window_end", "advisory", "ema38", "ema100", "prev_ema38", "prev_ema100"]


def local_time(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def tick_files(path):
    path = Path(path)
    if path.is_dir():
        files = sorted(path.glob("*.parquet"))
        if not files:
            files = sorted(path.glob("ticks/*.parquet"))
        return files
    return [path]


TICK_COLUMNS = ("symbol", "last", "event_ts", "seq")


def iter_ticks(paths, batch_size=65_536, columns=TICK_COLUMNS):
    for path in paths:
        parquet = pq.ParquetFile(path)
        for batch in parquet.iter_batches(batch_size=batch_size, columns=list(columns)):
            data = batch.to_pydict()
            yield from zip(*(data[c] for c in columns))


def default_metadata(ticks):
    path = Path(ticks)
    return (path if path.is_dir() else path.parent).parent / "metadata.parquet"


def metadata_values(path):
    fields = ("exchange", "sec_type", "isin", "currency", "related_indices")
    for row in pq.read_table(path, columns=["symbol", *fields]).to_pylist():
        yield row["symbol"], ",".join(row[f] or "" for f in fields)


def load_subscriptions(spec):
    if spec is None or spec == "all":
        return None
    symbols = set()
    for line in Path(spec).read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            symbols.add(line)
    return symbols


class ResultWriter:
    def __init__(self, out_dir):
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self._ema_file = (self.out_dir / "ema.csv").open("w", newline="")
        self._adv_file = (self.out_dir / "advisories.csv").open("w", newline="")
        self.ema = csv.writer(self._ema_file)
        self.advisories = csv.writer(self._adv_file)
        self.ema.writerow(EMA_HEADER)
        self.advisories.writerow(ADVISORY_HEADER)

    def write_ema(self, symbol, window_end, close, ema38, ema100):
        self.ema.writerow([symbol, window_end, local_time(window_end), repr(close), repr(ema38), repr(ema100)])

    def write_advisory(self, symbol, window_end, kind, ema38, ema100, prev38, prev100):
        self.advisories.writerow([symbol, window_end, local_time(window_end), kind,
                                  repr(ema38), repr(ema100), repr(prev38), repr(prev100)])

    def close(self):
        self._ema_file.close()
        self._adv_file.close()


def percentiles(values, points=(50, 95, 99)):
    if not values:
        return {}
    ordered = sorted(values)
    result = {}
    for p in points:
        index = min(len(ordered) - 1, max(0, round(p / 100 * (len(ordered) - 1))))
        result[f"p{p}"] = ordered[index]
    result["max"] = ordered[-1]
    return result
