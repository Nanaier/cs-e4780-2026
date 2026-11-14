import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path

PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Trading Trends</title>
<script src="https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.production.js"></script>
<style>
:root { --bg: #ffffff; --panel: #f6f7f9; --ink: #1d2433; --muted: #5f6b7a; --line: #dde1e6; --buy: #15803d; --sell: #b91c1c; --e38: #2563eb; --e100: #ea580c; --close: #8a94a3; }
@media (prefers-color-scheme: dark) { :root { --bg: #11151c; --panel: #1a2029; --ink: #e6e9ee; --muted: #9aa4b2; --line: #2a323d; --buy: #4ade80; --sell: #f87171; --e38: #60a5fa; --e100: #fb923c; --close: #6b7584; } }
* { box-sizing: border-box; }
body { margin: 0; font: 14px/1.4 system-ui, -apple-system, Segoe UI, sans-serif; background: var(--bg); color: var(--ink); }
header { padding: 16px 20px; border-bottom: 1px solid var(--line); display: flex; flex-wrap: wrap; gap: 24px; align-items: baseline; }
header h1 { font-size: 18px; margin: 0; }
.stat { color: var(--muted); } .stat b { color: var(--ink); }
main { display: grid; grid-template-columns: 280px 1fr; min-height: calc(100vh - 60px); }
aside { border-right: 1px solid var(--line); background: var(--panel); overflow: auto; max-height: calc(100vh - 60px); }
aside input { width: calc(100% - 24px); margin: 12px; padding: 8px; border: 1px solid var(--line); border-radius: 6px; background: var(--bg); color: var(--ink); }
.sym { padding: 8px 12px; cursor: pointer; display: flex; justify-content: space-between; gap: 8px; border-top: 1px solid var(--line); }
.sym:hover, .sym.on { background: var(--bg); }
.sym small { color: var(--muted); display: block; }
.tag { font-weight: 600; font-size: 12px; } .BUY { color: var(--buy); } .SELL { color: var(--sell); }
section { padding: 16px 20px; min-width: 0; }
.legend { display: flex; gap: 16px; color: var(--muted); margin: 4px 0 8px; flex-wrap: wrap; }
.legend i { display: inline-block; width: 14px; height: 3px; vertical-align: middle; margin-right: 6px; }
#chart { height: 380px; } #breadth { height: 160px; }
h2 { font-size: 15px; margin: 18px 0 4px; }
table { border-collapse: collapse; width: 100%; font-variant-numeric: tabular-nums; }
th, td { text-align: left; padding: 4px 8px; border-bottom: 1px solid var(--line); }
th { color: var(--muted); font-weight: 500; }
@media (max-width: 760px) { main { grid-template-columns: 1fr; } aside { max-height: 240px; border-right: 0; border-bottom: 1px solid var(--line); } }
</style>
</head>
<body>
<header>
  <h1>Trading Trends</h1>
  <span class="stat">symbols shown <b id="n-sym"></b></span>
  <span class="stat">EMA rows <b id="n-ema"></b></span>
  <span class="stat">buy <b id="n-buy" class="BUY"></b></span>
  <span class="stat">sell <b id="n-sell" class="SELL"></b></span>
  <span class="stat">first-window signals <b id="n-first"></b></span>
  <span class="stat" id="perf"></span>
</header>
<main>
  <aside>
    <input id="filter" placeholder="Filter symbols">
    <div id="list"></div>
  </aside>
  <section>
    <h2 id="title"></h2>
    <div class="legend"><span><i style="background:var(--close)"></i>close per window</span><span><i style="background:var(--e38)"></i>EMA38</span><span><i style="background:var(--e100)"></i>EMA100</span><span><b class="BUY">&#9650;</b> buy</span><span><b class="SELL">&#9660;</b> sell</span><span>&#9679; first-window signal</span></div>
    <div id="chart"></div>
    <h2>Market breadth: buy minus sell advisories per hour, shown symbols, first-window signals excluded</h2>
    <div id="breadth"></div>
    <h2>Advisories for this symbol</h2>
    <table><thead><tr><th>Window end</th><th>Advisory</th><th>EMA38</th><th>EMA100</th></tr></thead><tbody id="rows"></tbody></table>
  </section>
</main>
<script type="application/json" id="data">__DATA__</script>
<script>
const D = JSON.parse(document.getElementById('data').textContent);
const css = n => getComputedStyle(document.documentElement).getPropertyValue(n).trim();
const fmt = t => new Date(t * 1000).toISOString().replace('T', ' ').slice(0, 16);
document.getElementById('n-sym').textContent = D.symbols.length;
document.getElementById('n-ema').textContent = D.totals.ema.toLocaleString();
document.getElementById('n-buy').textContent = D.totals.buy.toLocaleString();
document.getElementById('n-sell').textContent = D.totals.sell.toLocaleString();
document.getElementById('n-first').textContent = D.totals.first.toLocaleString();
if (D.metrics) document.getElementById('perf').innerHTML = 'throughput <b>' + (D.metrics.events_per_second || 0).toLocaleString() + '</b> events/s, p99 latency <b>' + ((D.metrics.latency_ms || {}).p99 ?? '-') + '</b> ms';
const base = { layout: { background: { color: 'transparent' }, textColor: css('--muted') }, grid: { vertLines: { color: css('--line') }, horzLines: { color: css('--line') } }, timeScale: { timeVisible: true, secondsVisible: false, borderColor: css('--line') }, rightPriceScale: { borderColor: css('--line') } };
const chart = LightweightCharts.createChart(document.getElementById('chart'), { ...base, autoSize: true });
const closeS = chart.addLineSeries({ color: css('--close'), lineWidth: 1, pointMarkersVisible: true, pointMarkersRadius: 2, lastValueVisible: false, priceLineVisible: false });
const e38 = chart.addLineSeries({ color: css('--e38'), lineWidth: 2, priceLineVisible: false });
const e100 = chart.addLineSeries({ color: css('--e100'), lineWidth: 2, priceLineVisible: false });
const breadth = LightweightCharts.createChart(document.getElementById('breadth'), { ...base, autoSize: true });
const hist = breadth.addHistogramSeries({ priceLineVisible: false, lastValueVisible: false });
hist.setData(D.breadth.map(b => ({ time: b.t, value: b.buy - b.sell, color: b.buy - b.sell >= 0 ? css('--buy') : css('--sell') })));
requestAnimationFrame(() => breadth.timeScale().fitContent());
function show(sym) {
  const s = D.series[sym];
  document.querySelectorAll('.sym').forEach(el => el.classList.toggle('on', el.dataset.s === sym));
  const meta = D.symbols.find(x => x.s === sym);
  document.getElementById('title').textContent = sym + (meta.isin ? '  ·  ' + meta.isin : '') + (meta.index ? '  ·  ' + meta.index : '');
  closeS.setData(s.t.map((t, i) => ({ time: t, value: s.c[i] })));
  e38.setData(s.t.map((t, i) => ({ time: t, value: s.a[i] })));
  e100.setData(s.t.map((t, i) => ({ time: t, value: s.b[i] })));
  const adv = D.advisories.filter(a => a.s === sym);
  e38.setMarkers(adv.map(a => a.w ? { time: a.t, position: 'belowBar', color: css('--close'), shape: 'circle', text: 'start' } : a.k === 'BUY'
    ? { time: a.t, position: 'belowBar', color: css('--buy'), shape: 'arrowUp', text: 'BUY' }
    : { time: a.t, position: 'aboveBar', color: css('--sell'), shape: 'arrowDown', text: 'SELL' }));
  requestAnimationFrame(() => chart.timeScale().fitContent());
  document.getElementById('rows').innerHTML = adv.slice().reverse().map(a => '<tr><td>' + fmt(a.t) + '</td><td class="tag ' + a.k + '">' + a.k + (a.w ? ' (first window)' : '') + '</td><td>' + a.a.toFixed(4) + '</td><td>' + a.b.toFixed(4) + '</td></tr>').join('') || '<tr><td colspan="4">No advisories</td></tr>';
}
function render(filter) {
  const list = document.getElementById('list');
  list.innerHTML = '';
  D.symbols.filter(x => x.s.toLowerCase().includes(filter)).forEach(x => {
    const el = document.createElement('div');
    el.className = 'sym'; el.dataset.s = x.s;
    el.innerHTML = '<div>' + x.s + '<small>' + x.buy + ' buy · ' + x.sell + ' sell' + (x.first ? ' · ' + x.first + ' first' : '') + '</small></div>' + (x.last ? '<div class="tag ' + x.last + '">' + x.last + '<small>' + fmt(x.last_t) + '</small></div>' : '');
    el.onclick = () => show(x.s);
    list.appendChild(el);
  });
}
document.getElementById('filter').oninput = e => render(e.target.value.toLowerCase());
render('');
if (D.symbols.length) show(D.symbols[0].s);
</script>
</body>
</html>
"""


def read_csv(path):
    with Path(path).open() as fh:
        return list(csv.DictReader(fh))


def first_window(advisory):
    return float(advisory["prev_ema38"]) == 0.0 and float(advisory["prev_ema100"]) == 0.0


def load_metadata(path):
    if not path or not Path(path).exists():
        return {}
    import duckdb
    rows = duckdb.sql(f"SELECT symbol, isin, related_indices FROM read_parquet('{path}')").fetchall()
    return {symbol: (isin, index) for symbol, isin, index in rows}


def main(argv=None):
    parser = argparse.ArgumentParser(description="Build a static HTML dashboard from ema.csv and advisories.csv")
    parser.add_argument("--results", default="out/stream")
    parser.add_argument("--metadata", default=None, help="metadata.parquet from the prep step")
    parser.add_argument("--max-symbols", type=int, default=60)
    parser.add_argument("--out", default=None)
    args = parser.parse_args(argv)

    results = Path(args.results)
    ema = read_csv(results / "ema.csv")
    advisories = read_csv(results / "advisories.csv")
    metrics_path = results / "metrics.json"
    metrics = json.loads(metrics_path.read_text()) if metrics_path.exists() else None
    metadata = load_metadata(args.metadata)

    per_symbol = defaultdict(lambda: {"buy": 0, "sell": 0, "first": 0, "last": None, "last_t": None, "rows": 0})
    for row in ema:
        per_symbol[row["symbol"]]["rows"] += 1
    for a in advisories:
        info = per_symbol[a["symbol"]]
        if first_window(a):
            info["first"] += 1
            continue
        info["buy" if a["advisory"] == "BUY" else "sell"] += 1
        t = int(a["window_end_ms"]) // 1000
        if info["last_t"] is None or t >= info["last_t"]:
            info["last"], info["last_t"] = a["advisory"], t
    ranked = sorted(per_symbol, key=lambda s: (-(per_symbol[s]["buy"] + per_symbol[s]["sell"]), -per_symbol[s]["rows"], s))
    chosen = set(ranked[: args.max_symbols])

    series = defaultdict(lambda: {"t": [], "c": [], "a": [], "b": []})
    for row in ema:
        if row["symbol"] in chosen:
            s = series[row["symbol"]]
            s["t"].append(int(row["window_end_ms"]) // 1000)
            s["c"].append(round(float(row["close"]), 6))
            s["a"].append(round(float(row["ema38"]), 6))
            s["b"].append(round(float(row["ema100"]), 6))
    shown_advisories, breadth = [], defaultdict(lambda: {"buy": 0, "sell": 0})
    for a in advisories:
        if a["symbol"] not in chosen:
            continue
        t = int(a["window_end_ms"]) // 1000
        warmup = first_window(a)
        shown_advisories.append({"s": a["symbol"], "t": t, "k": a["advisory"], "a": float(a["ema38"]), "b": float(a["ema100"]), "w": warmup})
        if not warmup:
            breadth[t - t % 3600]["buy" if a["advisory"] == "BUY" else "sell"] += 1

    symbols = []
    for s in sorted(chosen, key=lambda s: (-(per_symbol[s]["last_t"] or 0), s)):
        isin, index = metadata.get(s, (None, None))
        info = per_symbol[s]
        symbols.append({"s": s, "isin": isin, "index": index, "buy": info["buy"], "sell": info["sell"],
                        "first": info["first"], "last": info["last"], "last_t": info["last_t"]})
    data = {
        "symbols": symbols,
        "series": series,
        "advisories": shown_advisories,
        "breadth": [{"t": t, **v} for t, v in sorted(breadth.items())],
        "totals": {"ema": len(ema),
                   "buy": sum(a["advisory"] == "BUY" and not first_window(a) for a in advisories),
                   "sell": sum(a["advisory"] == "SELL" and not first_window(a) for a in advisories),
                   "first": sum(first_window(a) for a in advisories)},
        "metrics": metrics,
    }
    out = Path(args.out) if args.out else results / "dashboard.html"
    out.write_text(PAGE.replace("__DATA__", json.dumps(data, separators=(",", ":")).replace("</", "<\\/")))
    print(f"wrote {out} ({len(symbols)} symbols)")


if __name__ == "__main__":
    main()
