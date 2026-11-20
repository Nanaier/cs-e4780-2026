"""Architecture diagram for the report (Figure 1): writes figures/architecture.pdf and .png.

    python analysis/correctness/architecture_figure.py
"""
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

OUT = Path(__file__).resolve().parent / "figures"
INK, MUTED, LINE = "#1d2433", "#5f6b7a", "#8a94a3"
FILL = {"data": "#eef2f7", "kafka": "#fdf1e3", "flink": "#e7f0fb", "out": "#e9f5ec", "check": "#f4f4f4"}
EDGE = {"data": "#7a8699", "kafka": "#c27c2c", "flink": "#2f6db3", "out": "#3b8a52", "check": "#8a8a8a"}


def box(ax, x, y, w, h, title, lines=(), kind="data", dashed=False):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.012,rounding_size=0.025",
                                fc=FILL[kind], ec=EDGE[kind], lw=0.9, ls="--" if dashed else "-"))
    ax.text(x + w / 2, y + h - 0.035, title, ha="center", va="top", fontsize=6.8, fontweight="bold", color=INK)
    for i, line in enumerate(lines):
        ax.text(x + w / 2, y + h - 0.10 - i * 0.056, line, ha="center", va="top", fontsize=5.6, color=MUTED)
    return (x, y, w, h)


def arrow(ax, start, end, label=None, offset=(0, 0.02), dashed=False, rad=0.0):
    ax.add_patch(FancyArrowPatch(start, end, arrowstyle="-|>", mutation_scale=7, lw=0.8, color=LINE,
                                 ls="--" if dashed else "-", connectionstyle=f"arc3,rad={rad}", shrinkA=1, shrinkB=1))
    if label:
        ax.text((start[0] + end[0]) / 2 + offset[0], (start[1] + end[1]) / 2 + offset[1], label,
                ha="center", va="bottom", fontsize=5.6, color=MUTED, style="italic")


def right(b, f=0.5):
    return (b[0] + b[2], b[1] + b[3] * f)


def left(b, f=0.5):
    return (b[0], b[1] + b[3] * f)


def bottom(b, f=0.5):
    return (b[0] + b[2] * f, b[1])


def top(b, f=0.5):
    return (b[0] + b[2] * f, b[1] + b[3])


def main():
    fig, ax = plt.subplots(figsize=(7.0, 2.55))
    ax.set_xlim(0, 3.5)
    ax.set_ylim(0, 1.27)
    ax.axis("off")

    raw = box(ax, 0.02, 0.72, 0.60, 0.48, "DEBS 2022 GC data", ["5 days, 289M rows", "FR · NL · ETR, 25 GB CSV"])
    prep = box(ax, 0.76, 0.72, 0.60, 0.48, "Filter + prep",
               ["filter.awk, prep.py", "DuckDB → parquet", "event time, stats"])
    replay = box(ax, 0.76, 0.12, 0.60, 0.48, "replay.py",
                 ["key = symbol, speed ×N", "metadata in-band", "heartbeats, quarantine"])
    ref = box(ax, 0.02, 0.12, 0.60, 0.48, "Batch oracle",
              ["reference.py (DuckDB)", "same rules, offline"], "check", dashed=True)

    kafka = box(ax, 1.50, 0.12, 0.42, 1.08, "Kafka 4.4",
                ["single KRaft broker", "", "ticks", "(8 partitions)", "", "subscriptions", "(compacted)", "",
                 "ema", "advisories", "quarantine"], "kafka")

    flink = box(ax, 2.06, 0.40, 0.70, 0.80, "Flink 2.2 job",
                ["watermark per partition,", "bound 300 ms", "keyBy(symbol) →", "KeyedProcessFunction",
                 "MapState: open windows", "timer at window end − 1", "ValueState: EMA38/EMA100",
                 "side outputs: advisories,", "late ticks"], "flink")
    py = box(ax, 2.06, 0.165, 0.70, 0.20, "Python engine", ["same operator (core.py)"], "flink", dashed=True)

    gate = box(ax, 2.90, 0.88, 0.58, 0.32, "gateway.py", ["fan-out to subscribers"], "out")
    viz = box(ax, 2.90, 0.50, 0.58, 0.32, "Dashboard", ["viz.py: EMAs, signals"], "out")
    coll = box(ax, 2.90, 0.12, 0.58, 0.32, "collect.py", ["topics → CSV, latency"], "out")

    arrow(ax, right(raw), left(prep))
    arrow(ax, bottom(prep), top(replay))
    ax.text(1.06 + 0.02, 0.66, "ticks.parquet", ha="left", va="center", fontsize=5.4, color=MUTED, style="italic")
    arrow(ax, right(replay), left(kafka, 0.30))
    arrow(ax, bottom(raw), top(ref), dashed=True)
    ax.text(0.34, 0.66, "same ticks", ha="left", va="center", fontsize=5.4, color=MUTED, style="italic")
    arrow(ax, right(kafka, 0.86), left(flink, 0.86))
    arrow(ax, right(kafka, 0.66), left(flink, 0.62))
    arrow(ax, left(flink, 0.22), right(kafka, 0.38))
    arrow(ax, right(kafka, 0.22), left(py, 0.5), dashed=True)
    arrow(ax, right(flink, 0.88), left(gate))
    arrow(ax, right(kafka, 0.014), left(coll, 0.05))
    arrow(ax, top(coll), bottom(viz))
    ax.plot([0.32, 0.32, 3.19, 3.19], [0.12, 0.035, 0.035, 0.12], color=LINE, lw=0.8, ls="--")
    ax.text(1.75, 0.035, "diff.py: each engine's output vs the oracle, row by row", ha="center", va="center",
            fontsize=5.6, color=MUTED, style="italic", bbox=dict(fc="white", ec="none", pad=0.8))

    OUT.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT / "architecture.pdf", bbox_inches="tight", pad_inches=0.02)
    fig.savefig(OUT / "architecture.png", dpi=220, bbox_inches="tight", pad_inches=0.02)
    print(f"wrote {OUT / 'architecture.pdf'}")


if __name__ == "__main__":
    main()
