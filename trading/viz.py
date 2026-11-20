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
:root { --bg: #ffffff; --panel: #f6f7f9; --ink: #1d2433; --muted: #5f6b7a; --line: #dde1e6; --buy: #15803d; --sell: #b91c1c; --buy-fill: rgba(21,128,61,0.18); --sell-fill: rgba(185,28,28,0.18); --e38: #2563eb; --e100: #ea580c; --close: #8a94a3; --first: #9aa4b2; }
@media (prefers-color-scheme: dark) { :root { --bg: #11151c; --panel: #1a2029; --ink: #e6e9ee; --muted: #9aa4b2; --line: #2a323d; --buy: #4ade80; --sell: #f87171; --buy-fill: rgba(74,222,128,0.18); --sell-fill: rgba(248,113,113,0.18); --e38: #60a5fa; --e100: #fb923c; --close: #6b7584; --first: #5f6b7a; } }
* { box-sizing: border-box; }
body { margin: 0; font: 14px/1.4 system-ui, -apple-system, Segoe UI, sans-serif; background: var(--bg); color: var(--ink); }
header { padding: 14px 20px; border-bottom: 1px solid var(--line); display: flex; flex-wrap: wrap; gap: 8px 24px; align-items: baseline; }
header h1 { font-size: 18px; margin: 0; }
.stat { color: var(--muted); } .stat b { color: var(--ink); }
main { display: grid; grid-template-columns: 300px 1fr; min-height: calc(100vh - 56px); }
aside { border-right: 1px solid var(--line); background: var(--panel); display: flex; flex-direction: column; max-height: calc(100vh - 56px); }
.controls { padding: 12px; display: grid; grid-template-columns: 1fr 1fr; gap: 8px; border-bottom: 1px solid var(--line); }
.controls input[type=search], .controls select { width: 100%; padding: 6px 8px; border: 1px solid var(--line); border-radius: 6px; background: var(--bg); color: var(--ink); font: inherit; }
.controls input[type=search] { grid-column: 1 / -1; }
.controls label { color: var(--muted); font-size: 12px; display: flex; flex-direction: column; gap: 2px; }
.controls .check { grid-column: 1 / -1; flex-direction: row; align-items: center; gap: 6px; font-size: 13px; }
#list { overflow: auto; flex: 1; }
.sym { padding: 8px 12px; cursor: pointer; display: flex; justify-content: space-between; gap: 8px; border-top: 1px solid var(--line); }
.sym:first-child { border-top: 0; }
.sym:hover, .sym.on { background: var(--bg); }
.sym small { color: var(--muted); display: block; }
.badge { display: inline-block; font-size: 11px; padding: 0 5px; border: 1px solid var(--line); border-radius: 4px; color: var(--muted); margin-left: 4px; }
.trend { font-size: 12px; margin-right: 4px; }
.tag { font-weight: 600; font-size: 12px; text-align: right; } .BUY { color: var(--buy); } .SELL { color: var(--sell); } .FIRST { color: var(--first); }
.empty { padding: 16px 12px; color: var(--muted); }
section { padding: 16px 20px; min-width: 0; }
.legend { display: flex; gap: 16px; color: var(--muted); margin: 4px 0 8px; flex-wrap: wrap; font-size: 13px; }
.legend i { display: inline-block; width: 14px; height: 3px; vertical-align: middle; margin-right: 6px; }
.legend .box { height: 10px; }
#chart { height: 340px; } #spread { height: 130px; } #breadth { height: 150px; }
h2 { font-size: 15px; margin: 18px 0 4px; }
h2 small { font-weight: 400; color: var(--muted); }
#title { font-size: 17px; margin-top: 0; }
#subtitle { color: var(--muted); margin: -2px 0 6px; }
table { border-collapse: collapse; width: 100%; font-variant-numeric: tabular-nums; }
th, td { text-align: left; padding: 4px 8px; border-bottom: 1px solid var(--line); }
th { color: var(--muted); font-weight: 500; }
@media (max-width: 760px) { main { grid-template-columns: 1fr; } aside { max-height: 360px; border-right: 0; border-bottom: 1px solid var(--line); } section { padding: 12px 16px; } }
</style>
</head>
<body>
<header>
  <h1>Trading Trends</h1>
  <span class="stat">symbols <b id="n-sym"></b></span>
  <span class="stat">EMA rows <b id="n-ema"></b></span>
  <span class="stat">buy <b id="n-buy" class="BUY"></b></span>
  <span class="stat">sell <b id="n-sell" class="SELL"></b></span>
  <span class="stat">first-window signals <b id="n-first"></b></span>
  <span class="stat" id="perf"></span>
</header>
<main>
  <aside>
    <div class="controls">
      <input id="q" type="search" placeholder="Search symbol or ISIN" aria-label="Search symbols">
      <label>Exchange<select id="exchange"></select></label>
      <label>Type<select id="type"><option value="">All</option><option value="E">Equity</option><option value="I">Index</option></select></label>
      <label>Trend<select id="trend"><option value="">All</option><option value="up">EMA38 above EMA100</option><option value="down">EMA38 below EMA100</option></select></label>
      <label>Sort by<select id="sort"><option value="latest">Latest signal</option><option value="crossovers">Most crossovers</option><option value="rows">Most windows</option><option value="name">Name</option></select></label>
      <label class="check"><input id="first" type="checkbox"> Show first-window signals (EMA warm-up)</label>
    </div>
    <div id="list"></div>
  </aside>
  <section>
    <h2 id="title"></h2>
    <div id="subtitle"></div>
    <div class="legend"><span><i style="background:var(--close)"></i>close per window</span><span><i style="background:var(--e38)"></i>EMA38</span><span><i style="background:var(--e100)"></i>EMA100</span><span><b class="BUY">&#9650;</b> buy</span><span><b class="SELL">&#9660;</b> sell</span><span class="FIRST">&#9679; first-window signal</span><span>axis follows the close price; the EMA warm-up from 0 runs off the bottom</span></div>
    <div id="chart"></div>
    <h2>Trend: EMA38 minus EMA100 <small>(% of EMA100; a zero crossing is a buy or sell advisory)</small></h2>
    <div class="legend"><span><i class="box" style="background:var(--buy-fill);border-top:2px solid var(--buy)"></i>uptrend, EMA38 above EMA100</span><span><i class="box" style="background:var(--sell-fill);border-bottom:2px solid var(--sell)"></i>downtrend, EMA38 below EMA100</span></div>
    <div id="spread"></div>
    <h2>Market breadth <small>(buy minus sell advisories per hour, symbols matching the filters, first-window signals excluded)</small></h2>
    <div id="breadth"></div>
    <h2>Advisories for this symbol</h2>
    <table><thead><tr><th>Window end</th><th>Advisory</th><th>EMA38</th><th>EMA100</th></tr></thead><tbody id="rows"></tbody></table>
  </section>
</main>
<script type="application/json" id="data">__DATA__</script>
<script>
const D = JSON.parse(document.getElementById('data').textContent);
const $ = id => document.getElementById(id);
const css = n => getComputedStyle(document.documentElement).getPropertyValue(n).trim();
const fmt = t => new Date(t * 1000).toISOString().replace('T', ' ').slice(0, 16);
const TYPE = { E: 'Equity', I: 'Index' };
const bySym = Object.fromEntries(D.symbols.map(x => [x.s, x]));
let current = null;

$('n-ema').textContent = D.totals.ema.toLocaleString();
$('n-first').textContent = D.totals.first.toLocaleString();
if (D.metrics) $('perf').innerHTML = 'throughput <b>' + Math.round(D.metrics.events_per_second || 0).toLocaleString() + '</b> events/s, p99 latency <b>' + ((D.metrics.latency_ms || {}).p99 ?? '-') + '</b> ms';
const exchanges = [...new Set(D.symbols.map(x => x.x).filter(Boolean))].sort();
$('exchange').innerHTML = '<option value="">All</option>' + exchanges.map(x => '<option>' + x + '</option>').join('');

const base = { layout: { background: { color: 'transparent' }, textColor: css('--muted') }, grid: { vertLines: { color: css('--line') }, horzLines: { color: css('--line') } }, timeScale: { timeVisible: true, secondsVisible: false, borderColor: css('--line') }, rightPriceScale: { borderColor: css('--line'), minimumWidth: 72 } };
const chart = LightweightCharts.createChart($('chart'), { ...base, autoSize: true });
const closeS = chart.addLineSeries({ color: css('--close'), lineWidth: 1, pointMarkersVisible: true, pointMarkersRadius: 2, lastValueVisible: false, priceLineVisible: false });
// The price axis follows the close prices only, so the EMA warm-up from 0 is clipped instead of squashing the chart.
const WARMUP = 230;  // windows until (1 - 2/101)^n < 1%, i.e. EMA100 has forgotten EMA_0 = 0
const e38 = chart.addLineSeries({ color: css('--e38'), lineWidth: 2, priceLineVisible: false, autoscaleInfoProvider: () => null });
const e100 = chart.addLineSeries({ color: css('--e100'), lineWidth: 2, priceLineVisible: false, autoscaleInfoProvider: () => null });
const spreadChart = LightweightCharts.createChart($('spread'), { ...base, autoSize: true });
const spread = spreadChart.addBaselineSeries({ baseValue: { type: 'price', price: 0 }, topLineColor: css('--buy'), topFillColor1: css('--buy-fill'), topFillColor2: css('--buy-fill'), bottomLineColor: css('--sell'), bottomFillColor1: css('--sell-fill'), bottomFillColor2: css('--sell-fill'), lineWidth: 2, priceLineVisible: false, priceFormat: { type: 'custom', formatter: v => v.toFixed(3) + ' %' } });
let syncing = false;
[[chart, spreadChart], [spreadChart, chart]].forEach(([a, b]) => a.timeScale().subscribeVisibleLogicalRangeChange(r => {
  if (syncing || !r) return;
  syncing = true; b.timeScale().setVisibleLogicalRange(r); syncing = false;
}));
const breadthChart = LightweightCharts.createChart($('breadth'), { ...base, autoSize: true });
const hist = breadthChart.addHistogramSeries({ priceLineVisible: false, lastValueVisible: false });

function filters() {
  return { q: $('q').value.trim().toLowerCase(), x: $('exchange').value, k: $('type').value, trend: $('trend').value, sort: $('sort').value, first: $('first').checked };
}
function visible(f) {
  const rows = D.symbols.filter(x => (!f.q || x.s.toLowerCase().includes(f.q) || (x.isin || '').toLowerCase().includes(f.q))
    && (!f.x || x.x === f.x) && (!f.k || x.k === f.k) && (!f.trend || x.trend === f.trend));
  const key = {
    latest: (a, b) => (b.last_t || 0) - (a.last_t || 0),
    crossovers: (a, b) => (b.buy + b.sell) - (a.buy + a.sell),
    rows: (a, b) => b.rows - a.rows,
    name: () => 0,
  }[f.sort];
  return rows.sort((a, b) => key(a, b) || a.s.localeCompare(b.s));
}
function marker(a) {
  if (a.w) return { time: a.t, position: 'belowBar', color: css('--first'), shape: 'circle', text: 'warm-up ' + a.k };
  return a.k === 'BUY'
    ? { time: a.t, position: 'belowBar', color: css('--buy'), shape: 'arrowUp', text: 'BUY' }
    : { time: a.t, position: 'aboveBar', color: css('--sell'), shape: 'arrowDown', text: 'SELL' };
}
function show(sym) {
  current = sym;
  const s = D.series[sym], meta = bySym[sym], f = filters();
  document.querySelectorAll('.sym').forEach(el => el.classList.toggle('on', el.dataset.s === sym));
  $('title').textContent = sym;
  $('subtitle').textContent = [meta.x, TYPE[meta.k], meta.isin, meta.index && 'index ' + meta.index, meta.rows.toLocaleString() + ' windows'].filter(Boolean).join('  ·  ');
  closeS.setData(s.t.map((t, i) => ({ time: t, value: s.c[i] })));
  e38.setData(s.t.map((t, i) => ({ time: t, value: s.a[i] })));
  e100.setData(s.t.map((t, i) => ({ time: t, value: s.b[i] })));
  const sp = s.t.map((t, i) => ({ time: t, value: s.b[i] ? (s.a[i] - s.b[i]) / Math.abs(s.b[i]) * 100 : 0 }));
  spread.setData(sp);
  // Scale the trend panel to the windows after the EMA100 warm-up; the climb from EMA_0 = 0 would flatten everything else.
  const settled = sp.slice(Math.min(WARMUP, Math.floor(sp.length / 2))).map(p => p.value);
  const lo = Math.min(0, ...settled), hi = Math.max(0, ...settled), pad = (hi - lo) * 0.1 || 0.01;
  spread.applyOptions({ autoscaleInfoProvider: () => ({ priceRange: { minValue: lo - pad, maxValue: hi + pad } }) });
  const adv = D.advisories.filter(a => a.s === sym && (f.first || !a.w));
  e38.setMarkers(adv.map(marker));
  spread.setMarkers(adv.filter(a => !a.w).map(a => ({ time: a.t, position: a.k === 'BUY' ? 'belowBar' : 'aboveBar', color: css(a.k === 'BUY' ? '--buy' : '--sell'), shape: 'circle' })));
  requestAnimationFrame(() => { chart.timeScale().fitContent(); spreadChart.timeScale().fitContent(); });
  $('rows').innerHTML = adv.slice().reverse().map(a => '<tr><td>' + fmt(a.t) + '</td><td class="tag ' + (a.w ? 'FIRST' : a.k) + '" style="text-align:left">' + a.k + (a.w ? ' (first window, EMA warm-up)' : '') + '</td><td>' + a.a.toFixed(4) + '</td><td>' + a.b.toFixed(4) + '</td></tr>').join('') || '<tr><td colspan="4">No advisories</td></tr>';
  try { history.replaceState(null, '', '#' + encodeURIComponent(sym)); } catch (e) {}
}
function render() {
  const f = filters(), rows = visible(f), names = new Set(rows.map(x => x.s));
  $('n-sym').textContent = rows.length + (rows.length === D.symbols.length ? '' : ' of ' + D.symbols.length);
  $('n-buy').textContent = rows.reduce((n, x) => n + x.buy, 0).toLocaleString();
  $('n-sell').textContent = rows.reduce((n, x) => n + x.sell, 0).toLocaleString();
  const list = $('list');
  list.innerHTML = rows.length ? '' : '<div class="empty">No symbols match these filters.</div>';
  rows.forEach(x => {
    const el = document.createElement('div');
    el.className = 'sym'; el.dataset.s = x.s;
    const trend = x.trend === 'up' ? '<span class="trend BUY" title="EMA38 above EMA100">&#9650;</span>' : x.trend === 'down' ? '<span class="trend SELL" title="EMA38 below EMA100">&#9660;</span>' : '';
    el.innerHTML = '<div>' + trend + x.s + (x.k ? '<span class="badge">' + TYPE[x.k] + '</span>' : '')
      + '<small>' + x.buy + ' buy · ' + x.sell + ' sell' + (f.first && x.first ? ' · ' + x.first + ' warm-up' : '') + '</small></div>'
      + (x.last ? '<div class="tag ' + x.last + '">' + x.last + '<small>' + fmt(x.last_t) + '</small></div>' : '<div class="tag"><small>no crossover</small></div>');
    el.onclick = () => show(x.s);
    list.appendChild(el);
  });
  const buckets = new Map();
  D.advisories.forEach(a => {
    if (a.w || !names.has(a.s)) return;
    const h = a.t - a.t % 3600;
    buckets.set(h, (buckets.get(h) || 0) + (a.k === 'BUY' ? 1 : -1));
  });
  hist.setData([...buckets].sort((a, b) => a[0] - b[0]).map(([t, v]) => ({ time: t, value: v, color: v >= 0 ? css('--buy') : css('--sell') })));
  requestAnimationFrame(() => breadthChart.timeScale().fitContent());
  const keep = current && names.has(current) ? current : rows.length ? rows[0].s : null;
  if (keep) show(keep);
}
['q', 'exchange', 'type', 'trend', 'sort', 'first'].forEach(id => $(id).addEventListener('input', render));
const wanted = decodeURIComponent(location.hash.slice(1));
if (bySym[wanted]) current = wanted;
render();
</script>
</body>
</html>
"""


def read_csv(path):
    with Path(path).open(newline="") as fh:
        return list(csv.DictReader(fh))


def ema_rows(path, *names):
    """Stream (symbol, window_end_ms, *names) from ema.csv without keeping it in memory (the week has 1.9M rows)."""
    with Path(path).open(newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        cols = [header.index(n) for n in ("symbol", "window_end_ms", *names)]
        for row in reader:
            yield [row[c] for c in cols]


def first_window(advisory):
    return float(advisory["prev_ema38"]) == 0.0 and float(advisory["prev_ema100"]) == 0.0


def exchange_of(symbol):
    return symbol.rsplit(".", 1)[1] if "." in symbol else None


def load_metadata(path):
    if not path or not Path(path).exists():
        return {}
    import duckdb
    rows = duckdb.sql(f"SELECT symbol, isin, related_indices, sec_type, exchange FROM read_parquet('{path}')").fetchall()
    return {symbol: {"isin": isin, "index": index, "sec_type": sec_type, "exchange": exchange}
            for symbol, isin, index, sec_type, exchange in rows}


def choose(per_symbol, max_symbols, include):
    """Pick the most active symbols, round-robin over exchanges so every exchange is represented."""
    ranked = sorted(per_symbol, key=lambda s: (-(per_symbol[s]["buy"] + per_symbol[s]["sell"]), -per_symbol[s]["rows"], s))
    queues = defaultdict(list)
    for s in ranked:
        queues[exchange_of(s)].append(s)
    chosen = [s for s in include if s in per_symbol]
    order = sorted(queues, key=lambda x: (-len(queues[x]), str(x)))
    while len(chosen) < max_symbols and any(queues.values()):
        for x in order:
            while queues[x] and queues[x][0] in chosen:
                queues[x].pop(0)
            if queues[x] and len(chosen) < max_symbols:
                chosen.append(queues[x].pop(0))
    return set(chosen)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Build a static HTML dashboard from ema.csv and advisories.csv")
    parser.add_argument("--results", default="out/stream")
    parser.add_argument("--metadata", default=None, help="metadata.parquet from the prep step")
    parser.add_argument("--max-symbols", type=int, default=60)
    parser.add_argument("--include", default="", help="comma-separated symbols that are always shown")
    parser.add_argument("--out", default=None)
    args = parser.parse_args(argv)

    results = Path(args.results)
    ema_path = results / "ema.csv"
    advisories = read_csv(results / "advisories.csv")
    metrics_path = results / "metrics.json"
    metrics = json.loads(metrics_path.read_text()) if metrics_path.exists() else None
    metadata = load_metadata(args.metadata)

    per_symbol = defaultdict(lambda: {"buy": 0, "sell": 0, "first": 0, "last": None, "last_t": None, "rows": 0,
                                      "end": None, "trend": None})
    ema_total = 0
    for symbol, end, ema38, ema100 in ema_rows(ema_path, "ema38", "ema100"):  # pass 1: counts and current trend
        ema_total += 1
        info = per_symbol[symbol]
        info["rows"] += 1
        end = int(end)
        if info["end"] is None or end >= info["end"]:
            info["end"] = end
            info["trend"] = "up" if float(ema38) > float(ema100) else "down"
    advisory_isin = {a["symbol"]: a["isin"] for a in advisories if a.get("isin")}  # --enrich output
    for a in advisories:
        info = per_symbol[a["symbol"]]
        if first_window(a):
            info["first"] += 1
            continue
        info["buy" if a["advisory"] == "BUY" else "sell"] += 1
        t = int(a["window_end_ms"]) // 1000
        if info["last_t"] is None or t >= info["last_t"]:
            info["last"], info["last_t"] = a["advisory"], t
    include = [s.strip() for s in args.include.split(",") if s.strip()]
    chosen = choose(per_symbol, args.max_symbols, include)

    series = defaultdict(lambda: {"t": [], "c": [], "a": [], "b": []})
    for symbol, end, close, ema38, ema100 in ema_rows(ema_path, "close", "ema38", "ema100"):  # pass 2: chosen only
        if symbol in chosen:
            s = series[symbol]
            s["t"].append(int(end) // 1000)
            s["c"].append(round(float(close), 6))
            s["a"].append(round(float(ema38), 6))
            s["b"].append(round(float(ema100), 6))
    for s in series.values():  # the chart needs ascending time; a multi-subtask engine may write out of order
        if any(a > b for a, b in zip(s["t"], s["t"][1:])):
            order = sorted(range(len(s["t"])), key=s["t"].__getitem__)
            for k in s:
                s[k] = [s[k][i] for i in order]
    shown_advisories = []
    for a in advisories:
        if a["symbol"] in chosen:
            shown_advisories.append({"s": a["symbol"], "t": int(a["window_end_ms"]) // 1000, "k": a["advisory"],
                                     "a": float(a["ema38"]), "b": float(a["ema100"]), "w": first_window(a)})
    shown_advisories.sort(key=lambda a: (a["s"], a["t"]))  # chart markers must be in ascending time

    symbols = []
    for s in sorted(chosen):
        meta = metadata.get(s, {})
        info = per_symbol[s]
        symbols.append({"s": s, "isin": meta.get("isin") or advisory_isin.get(s), "index": meta.get("index"),
                        "x": meta.get("exchange") or exchange_of(s), "k": meta.get("sec_type"),
                        "buy": info["buy"], "sell": info["sell"], "first": info["first"], "rows": info["rows"],
                        "trend": info["trend"], "last": info["last"], "last_t": info["last_t"]})
    data = {
        "symbols": symbols,
        "series": series,
        "advisories": shown_advisories,
        "totals": {"ema": ema_total, "first": sum(first_window(a) for a in advisories)},
        "metrics": metrics,
    }
    out = Path(args.out) if args.out else results / "dashboard.html"
    out.write_text(encoding="utf-8", data=PAGE.replace("__DATA__", json.dumps(data, separators=(",", ":")).replace("</", "<\\/")))
    print(f"wrote {out} ({len(symbols)} symbols)")


if __name__ == "__main__":
    main()
