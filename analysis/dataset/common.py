import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import matplotlib

matplotlib.use("pdf")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
RESULTS = Path(__file__).resolve().parent / "results"
DATA = ROOT / "data"
WEEK = DATA / "week"
STATS = sorted((WEEK / "stats").glob("*.json"))
WEEKEND_STATS = sorted((DATA / "samples" / "stats").glob("0*.json"))
KINDS = sorted((DATA / "filtered").glob("*.kinds.txt"))
TICKS = f"read_parquet('{WEEK / 'ticks' / '*.parquet'}')"
META = f"read_parquet('{WEEK / 'metadata.parquet'}')"
META_EVENTS = f"read_parquet('{WEEK / 'metadata_events.parquet'}')"
EMA_300 = ROOT / "out" / "week" / "reference-300" / "ema.csv"

SEC_COLOR = {"I": "#2a78d6", "E": "#eb6834"}
SEC_NAME = {"I": "Indices", "E": "Equities"}
INK = "#0b0b0b"
MUTED = "#52514e"
GRID = "#e4e3df"
WIDTH_IN = 3.33

plt.rcParams.update({
    "font.size": 8,
    "axes.titlesize": 8,
    "axes.labelsize": 8,
    "xtick.labelsize": 7,
    "ytick.labelsize": 7,
    "legend.fontsize": 7,
    "legend.frameon": False,
    "axes.edgecolor": MUTED,
    "axes.labelcolor": INK,
    "xtick.color": MUTED,
    "ytick.color": MUTED,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.grid": True,
    "grid.color": GRID,
    "grid.linewidth": 0.5,
    "axes.axisbelow": True,
    "lines.linewidth": 1.2,
    "pdf.fonttype": 42,
    "ps.fonttype": 42,
})


def connect():
    return duckdb.connect()


def day_of(path):
    stem = Path(path).name.split("trading-day-")[1][:8]
    day, month, year = stem.split("-")
    return f"20{year}-{month}-{day}"


def load_stats():
    return [(day_of(path), json.loads(path.read_text())) for path in STATS]


def wall_clock(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).replace(tzinfo=None)


def write_csv(name, header, rows):
    RESULTS.mkdir(parents=True, exist_ok=True)
    path = RESULTS / f"{name}.csv"
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)
    return path


def query_csv(con, name, sql):
    relation = con.sql(sql)
    rows = relation.fetchall()
    write_csv(name, relation.columns, rows)
    return relation.columns, rows


def figure(height=2.2, nrows=1, ncols=1, **kwargs):
    return plt.subplots(nrows, ncols, figsize=(WIDTH_IN, height), **kwargs)


def save(fig, name):
    RESULTS.mkdir(parents=True, exist_ok=True)
    fig.savefig(RESULTS / f"{name}.pdf", bbox_inches="tight")
    plt.close(fig)


def pct(part, whole):
    return round(100.0 * part / whole, 3) if whole else 0.0
