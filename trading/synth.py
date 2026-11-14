import argparse
import math
import random
from datetime import datetime, timedelta
from pathlib import Path

HEADER = (
    "ID,SecType,Date,Time,Ask,Ask volume,Bid,Bid volume,Ask time,Day's high ask,Close,Currency,"
    "Day's high ask time,Day's high,ISIN,Auction price,Day's low ask,Day's low,Day's low ask time,Open,"
    "Nominal value,Last,Last volume,Trading time,Total volume,Mid price,Trading date,Profit,Current price,"
    "Related indices,Day high bid time, Day low bid time,Open Time, Last trade time, Close Time, "
    "Day high Time, Day low Time,Bid time,Auction Time"
)

EXCHANGES = [("FR", 0.55, "CAC40", 0.0), ("NL", 0.28, "AEX", 0.0), ("ETR", 0.17, "DAX", 1.0)]

SESSION_OPEN_MS = 9 * 3_600_000
SESSION_CLOSE_MS = 17 * 3_600_000 + 30 * 60_000
DAY_MS = 86_400_000


def fmt_date(day):
    return day.strftime("%d-%m-%Y")


def fmt_clock(ms_of_day):
    ms_of_day = max(0, min(DAY_MS - 1, int(ms_of_day)))
    h, rem = divmod(ms_of_day, 3_600_000)
    m, rem = divmod(rem, 60_000)
    s, ms = divmod(rem, 1000)
    return f"{h:02d}:{m:02d}:{s:02d}.{ms:03d}"


def row(values):
    fields = [""] * 40
    for index, value in values.items():
        fields[index - 1] = value
    return ",".join(fields)


def make_symbols(rng, count):
    symbols = []
    for i in range(count):
        roll = rng.random()
        acc = 0.0
        for exchange, share, index_name, delay_weight in EXCHANGES:
            acc += share
            if roll <= acc:
                break
        sec_type = "I" if rng.random() < 0.25 else "E"
        symbols.append({
            "id": f"SY{i:04d}.{exchange}",
            "exchange": exchange,
            "sec_type": sec_type,
            "isin": f"XS{rng.randrange(10**9, 10**10)}",
            "index": index_name,
            "delay_weight": delay_weight,
            "price": rng.uniform(5.0, 400.0),
            "period_min": rng.uniform(150.0, 600.0),
            "phase": rng.uniform(0.0, 2 * math.pi),
            "amplitude": rng.uniform(0.0002, 0.0008),
        })
    return symbols


def generate(args):
    rng = random.Random(args.seed)
    symbols = make_symbols(rng, args.symbols)
    weights = [1.0 / (rank + 1) ** args.zipf for rank in range(len(symbols))]
    start_day = datetime.strptime(args.start, "%d-%m-%Y").date()
    days = []
    d = start_day
    while len(days) < args.days:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)

    sigma = args.volatility
    out = []
    t0_minutes = None
    for day_index, day in enumerate(days):
        if day_index == 0:
            for s in symbols:
                out.append((0, row({1: s["id"], 2: s["sec_type"], 3: fmt_date(day), 4: fmt_clock(0),
                                    11: "0.000000", 14: "0.000000", 18: "0.000000", 20: "0.000000",
                                    22: "0.000000", 24: "00:00:00.000"})))
        for s in symbols:
            out.append((rng.uniform(0, SESSION_OPEN_MS), row({1: s["id"], 2: s["sec_type"], 3: fmt_date(day),
                                                             12: "EUR", 15: s["isin"],
                                                             30: s["index"] if s["sec_type"] == "E" else ""})))
        for _ in range(args.housekeeping):
            s = rng.choice(symbols)
            at = rng.uniform(3_600_000, SESSION_OPEN_MS)
            out.append((at, row({1: s["id"], 2: s["sec_type"], 3: fmt_date(day), 4: fmt_clock(at)})))
            out.append((at + 1, row({1: s["id"], 2: s["sec_type"], 3: fmt_date(day), 4: fmt_clock(int(at) // 1000 * 1000)})))

        counts = [0] * len(symbols)
        for k in rng.choices(range(len(symbols)), weights=weights, k=args.trades_per_day):
            counts[k] += 1
        for s, n in zip(symbols, counts):
            if n == 0:
                continue
            times = sorted(rng.uniform(SESSION_OPEN_MS, SESSION_CLOSE_MS) for _ in range(n))
            prev_minute = None
            for trade_ms in times:
                minute = (day_index * DAY_MS + trade_ms) / 60_000
                if t0_minutes is None:
                    t0_minutes = minute
                dt = 0.0 if prev_minute is None else max(minute - prev_minute, 0.0)
                prev_minute = minute
                drift = s["amplitude"] * math.sin(2 * math.pi * (minute - t0_minutes) / s["period_min"] + s["phase"])
                s["price"] *= math.exp(drift * dt + sigma * math.sqrt(dt) * rng.gauss(0.0, 1.0))
                delay = rng.expovariate(1 / 30.0) + s["delay_weight"] * rng.uniform(0, args.jitter_ms)
                if rng.random() < args.late_prob:
                    delay += rng.uniform(args.late_min_ms, args.late_max_ms)
                arrival = trade_ms + delay
                out.append((arrival, row({1: s["id"], 2: s["sec_type"], 3: fmt_date(day), 4: fmt_clock(arrival),
                                          22: f"{s['price']:.6f}", 23: str(rng.randrange(1, 2000)),
                                          24: fmt_clock(trade_ms), 27: fmt_date(day)})))
                for _ in range(rng.randrange(0, 2 * args.quotes_per_trade + 1)):
                    q_at = rng.uniform(trade_ms - 5_000, trade_ms + 5_000)
                    spread = s["price"] * 0.0005
                    out.append((q_at, row({1: s["id"], 2: s["sec_type"], 3: fmt_date(day), 4: fmt_clock(q_at),
                                           5: f"{s['price'] + spread:.6f}", 6: str(rng.randrange(1, 5000)),
                                           7: f"{s['price'] - spread:.6f}", 8: str(rng.randrange(1, 5000))})))
        out.sort(key=lambda item: item[0])
        yield day, [line for _, line in out]
        out = []


def main(argv=None):
    parser = argparse.ArgumentParser(description="Generate synthetic trading days in the DEBS 2022 raw CSV format")
    parser.add_argument("--out-dir", default="data/raw")
    parser.add_argument("--start", default="08-11-2021", help="first day, DD-MM-YYYY")
    parser.add_argument("--days", type=int, default=3, help="number of weekdays")
    parser.add_argument("--symbols", type=int, default=300)
    parser.add_argument("--trades-per-day", type=int, default=60_000)
    parser.add_argument("--quotes-per-trade", type=int, default=2)
    parser.add_argument("--housekeeping", type=int, default=2_000)
    parser.add_argument("--zipf", type=float, default=1.1)
    parser.add_argument("--volatility", type=float, default=0.0008)
    parser.add_argument("--jitter-ms", type=float, default=1_500)
    parser.add_argument("--late-prob", type=float, default=0.0005)
    parser.add_argument("--late-min-ms", type=float, default=60_000)
    parser.add_argument("--late-max-ms", type=float, default=900_000)
    parser.add_argument("--seed", type=int, default=4780)
    args = parser.parse_args(argv)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    for day, lines in generate(args):
        path = out_dir / f"synthetic-trading-day-{day.strftime('%d-%m-%y')}.csv"
        with path.open("w", newline="") as fh:
            fh.write("# Synthetic test data in the DEBS 2022 Grand Challenge trading data layout\n")
            fh.write("#\n")
            fh.write(HEADER + "\n")
            fh.write("#'Unique ID','Security Type (E)quity/(I)ndex'\n")
            for line in lines:
                fh.write(line + "\n")
        print(f"{path}: {len(lines)} rows")


if __name__ == "__main__":
    main()
