"""Build the dashboard as <results>/dashboard.html.

There is one dashboard, implemented in trading/monitor.py and trading/monitor.html; it combines the terminal
layout of the first dashboard (watchlist with gap bars and buy/sell counts, signal panel, read-out, advisory tape)
with the replay view. This module keeps the original command and output name working:

    python -m trading.viz --results out/week/reference-300 --metadata data/week/metadata.parquet
"""
from trading import monitor


def main(argv=None):
    return monitor.main(argv, default_name="dashboard.html")


if __name__ == "__main__":
    main()
