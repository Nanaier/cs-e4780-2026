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
/* Terminal look: one dark theme. Series colors (close, EMA38, EMA100) are checked for colour-blind separation. */
:root { --bg: #04070c; --panel: #0b111b; --head: #0f1826; --line: #1b2636; --grid: #131c29; --ink: #e9eef6; --muted: #8a97aa;
  --accent: #3fb6ff; --brand: #1667c7; --tab: #2e9e58; --buy: #3ccf7a; --sell: #f25c5c; --buy-fill: rgba(60,207,122,0.22); --sell-fill: rgba(242,92,92,0.22);
  --close: #1fa878; --close-top: rgba(31,168,120,0.42); --close-bottom: rgba(31,168,120,0.03); --e38: #d97706; --e100: #3d94e8; --first: #6f7c8f; }
* { box-sizing: border-box; }
html, body { height: 100%; }
body { margin: 0; font: 12px/1.35 "Segoe UI", system-ui, -apple-system, Arial, sans-serif; background: var(--bg); color: var(--ink); display: flex; flex-direction: column; font-variant-numeric: tabular-nums; }
button, input, select { font: inherit; color: inherit; }
.BUY { color: var(--buy); } .SELL { color: var(--sell); } .FIRST { color: var(--first); }

.top { display: flex; align-items: center; gap: 14px; padding: 6px 10px; background: #070c14; border-bottom: 1px solid var(--line); }
.brand { font-size: 15px; font-weight: 700; letter-spacing: .02em; white-space: nowrap; }
.brand span { color: var(--accent); }
#q { width: 230px; padding: 6px 10px; border: 0; border-radius: 2px; background: var(--brand); color: #fff; }
#q::placeholder { color: #cfe2fb; }
.ticker { display: flex; flex-wrap: wrap; gap: 0 22px; flex: 1; min-width: 0; height: 31px; overflow: hidden; }
.tick { cursor: pointer; white-space: nowrap; line-height: 1.25; }
.tick b { font-weight: 600; margin: 0 6px 0 3px; }
.tick small { display: block; color: var(--muted); font-size: 11px; padding-left: 14px; }
.clock { text-align: right; white-space: nowrap; }
.clock small { display: block; color: var(--accent); font-size: 11px; }

.tabs { display: flex; flex-wrap: wrap; align-items: center; gap: 4px 6px; padding: 4px 10px; background: var(--head); border-bottom: 1px solid var(--line); }
.tab { border: 1px solid var(--line); background: transparent; padding: 3px 10px; border-radius: 2px; cursor: pointer; color: var(--muted); }
.tab:hover { color: var(--ink); }
.tab.on { background: var(--tab); border-color: var(--tab); color: #fff; }
.tabs .gap { width: 14px; } .tabs .fill { flex: 1; }
.stat { color: var(--muted); margin-left: 12px; white-space: nowrap; } .stat b { color: var(--ink); font-weight: 600; }

main { flex: 1; min-height: 0; display: grid; gap: 4px; padding: 4px; grid-template-columns: 300px minmax(0, 1fr) 390px;
  grid-template-rows: minmax(430px, 58fr) minmax(280px, 42fr); grid-template-areas: "side chart over" "watch watch right"; }
.panel { background: var(--panel); border: 1px solid var(--line); min-width: 0; min-height: 0; display: flex; flex-direction: column; }
.panel > h2 { margin: 0; padding: 5px 10px; font-size: 12px; font-weight: 600; color: var(--accent); background: var(--head); border-bottom: 1px solid var(--line); display: flex; gap: 10px; align-items: center; white-space: nowrap; }
.panel > h2 small { color: var(--muted); font-weight: 400; overflow: hidden; text-overflow: ellipsis; }
.panel > h2 .fill { flex: 1; }
.panel > h2 label { color: var(--muted); font-weight: 400; display: flex; align-items: center; gap: 5px; }
.panel > h2 select { background: var(--panel); border: 1px solid var(--line); border-radius: 2px; padding: 1px 4px; }
.body { padding: 8px 10px; overflow: auto; flex: 1; min-height: 0; }
#p-side { grid-area: side; } #p-chart { grid-area: chart; } #p-over { grid-area: over; } #p-watch { grid-area: watch; } #p-right { grid-area: right; }
h3 { margin: 12px 0 5px; font-size: 11px; font-weight: 600; color: var(--accent); }
h3:first-child { margin-top: 0; }
h3 small { color: var(--muted); font-weight: 400; }

.name { font-size: 15px; font-weight: 700; letter-spacing: .03em; }
.split { display: flex; gap: 2px; height: 18px; }
.split div { display: flex; align-items: center; padding: 0 6px; font-size: 11px; font-weight: 600; color: #04130a; min-width: 22px; overflow: hidden; white-space: nowrap; }
.split .b { background: var(--buy); } .split .s { background: var(--sell); color: #1c0606; justify-content: flex-end; }
.split .none { background: var(--line); color: var(--muted); flex: 1; font-weight: 400; }
.note { color: var(--muted); font-size: 11px; margin-top: 3px; }
#side th, #side td { padding: 2px 3px; } #side td:first-child, #side th:first-child { padding-left: 0; }
table { border-collapse: collapse; width: 100%; }
th, td { text-align: left; padding: 2px 6px; white-space: nowrap; }
th { color: var(--accent); font-weight: 600; font-size: 11px; }
td.n, th.n { text-align: right; }
.kv td { padding: 2px 0; } .kv td:first-child { color: var(--muted); } .kv td:last-child { text-align: right; }
.days td { padding: 2px 0; } .days .bar { display: inline-block; height: 8px; vertical-align: middle; margin-right: 4px; }

#readout { display: flex; flex-wrap: wrap; gap: 2px 14px; align-items: center; padding: 5px 10px; background: var(--head); border-bottom: 1px solid var(--line); color: var(--muted); }
#readout b { color: var(--ink); font-weight: 600; } #readout b.BUY { color: var(--buy); } #readout b.SELL { color: var(--sell); }
#readout i, .sw { display: inline-block; width: 12px; height: 3px; vertical-align: middle; margin-right: 5px; }
#chart { flex: 3; min-height: 180px; } #spread { flex: 1.25; min-height: 90px; }
.sub { padding: 4px 10px; background: var(--head); border-top: 1px solid var(--line); border-bottom: 1px solid var(--line); color: var(--accent); font-weight: 600; }
.sub small { color: var(--muted); font-weight: 400; }

.big { font-size: 24px; font-weight: 300; line-height: 1.1; }
.quote { display: flex; align-items: baseline; justify-content: space-between; gap: 10px; margin: 2px 0 8px; }
.quote .last { font-size: 24px; font-weight: 300; }
.quote .chg { text-align: right; font-weight: 600; } .quote .chg small { display: block; color: var(--muted); font-weight: 400; }
.rows td, .rows th { border-bottom: 1px solid var(--grid); }

#watch { font-size: 12px; }
#watch thead th { position: sticky; top: 0; background: var(--panel); cursor: pointer; border-bottom: 1px solid var(--line); user-select: none; }
#watch thead th.nosort { cursor: default; }
#watch th[data-dir]::after { content: attr(data-dir); }
#watch tbody tr { cursor: pointer; }
#watch tbody tr:nth-child(even) { background: #0d1522; }
#watch tbody tr:hover, #watch tbody tr.on { background: #16263c; }
#watch td { padding: 2px 6px; }
.dbar { display: flex; width: 90px; height: 9px; }
.dbar span { flex: 1; display: flex; } .dbar span:first-child { justify-content: flex-end; border-right: 1px solid var(--muted); }
.dbar i { display: block; height: 100%; } .dbar .neg { background: var(--sell); } .dbar .pos { background: var(--buy); }
.chip { display: inline-block; min-width: 38px; text-align: center; padding: 0 5px; border-radius: 2px; font-size: 11px; font-weight: 700; }
.chip.BUY { background: var(--buy); color: #04130a; } .chip.SELL { background: var(--sell); color: #1c0606; }
.empty { padding: 14px 10px; color: var(--muted); }
#breadth { height: 130px; flex: none; }
#tape td { border-bottom: 1px solid var(--grid); cursor: pointer; }
footer { padding: 3px 10px; color: var(--muted); font-size: 11px; border-top: 1px solid var(--line); display: flex; gap: 16px; flex-wrap: wrap; }
footer a { color: var(--accent); }
@media (max-width: 1150px) {
  html, body { height: auto; } body { display: block; }
  main { grid-template-columns: 1fr; grid-template-rows: none; grid-template-areas: "chart" "over" "side" "watch" "right"; }
  #p-chart { height: 520px; } #p-watch { height: 420px; } #p-over, #p-right { max-height: 520px; }
  .ticker, .clock { display: none; } #q { flex: 1; width: auto; }
  .panel > h2 { flex-wrap: wrap; white-space: normal; } .panel > h2 .fill { display: none; } .stat { margin-left: 0; margin-right: 10px; }
}
</style>
</head>
<body>
<header class="top">
  <div class="brand">Trading <span>Trends</span></div>
  <input id="q" type="search" placeholder="Search symbol or ISIN" aria-label="Search symbols">
  <div class="ticker" id="ticker"></div>
  <div class="clock"><small>Data</small><b id="range"></b></div>
</header>
<nav class="tabs" aria-label="Filters">
  <span id="tabs-x"></span><span class="gap"></span>
  <span id="tabs-k"><button class="tab on" data-k="">All types</button> <button class="tab" data-k="E">Equities</button> <button class="tab" data-k="I">Indices</button></span>
  <span class="fill"></span>
  <span class="stat">symbols <b id="n-sym"></b></span>
  <span class="stat">EMA rows <b id="n-ema"></b></span>
  <span class="stat">buy <b id="n-buy"></b></span>
  <span class="stat">sell <b id="n-sell"></b></span>
  <span class="stat">first-window signals <b id="n-first"></b></span>
  <span class="stat" id="perf"></span>
</nav>
<main>
  <section class="panel" id="p-side">
    <h2 id="side-title">Symbol</h2>
    <div class="body" id="side"></div>
  </section>
  <section class="panel" id="p-chart">
    <div id="readout"></div>
    <div id="chart"></div>
    <div class="sub">Trend: EMA38 minus EMA100 <small>(% of EMA100; a zero crossing is a buy or sell advisory)</small></div>
    <div id="spread"></div>
  </section>
  <section class="panel" id="p-over">
    <h2 id="over-title">Overview</h2>
    <div class="body" id="over"></div>
  </section>
  <section class="panel" id="p-watch">
    <h2>Watchlist <small id="watch-note"></small><span class="fill"></span>
      <label>Trend <select id="trend"><option value="">All</option><option value="up">EMA38 above EMA100</option><option value="down">EMA38 below EMA100</option></select></label>
      <label><input id="first" type="checkbox"> Show first-window signals (EMA warm-up)</label></h2>
    <div class="body" style="padding:0"><table id="watch"><thead><tr>
      <th data-sort="s">Symbol</th><th data-sort="k">Type</th><th data-sort="x">Exch</th><th class="n" data-sort="c">Last</th>
      <th class="n" data-sort="a">EMA38</th><th class="n" data-sort="b">EMA100</th><th class="n" data-sort="sp">Spread</th><th class="nosort" title="EMA38 below or above EMA100; a full bar is the upper quartile of the spreads shown">Below / above</th>
      <th class="n" data-sort="buy">Buy</th><th class="n" data-sort="sell">Sell</th><th data-sort="latest">Signal</th><th data-sort="latest">Time</th>
    </tr></thead><tbody id="list"></tbody></table><div class="empty" id="none" hidden>No symbols match these filters.</div></div>
  </section>
  <section class="panel" id="p-right">
    <h2>Market breadth <small>buy minus sell advisories per hour</small></h2>
    <div id="breadth"></div>
    <div class="sub">Advisory tape <small>latest crossovers</small></div>
    <div class="body" style="padding:0"><table id="tape"><tbody id="tape-rows"></tbody></table></div>
  </section>
</main>
<footer><span>Times are exchange local time. Both EMAs start at 0, so every symbol's first window is a warm-up BUY; it is hidden unless enabled.</span>
  <span>Charts: <a href="https://www.tradingview.com/" target="_blank" rel="noopener">TradingView Lightweight Charts</a></span></footer>
<script type="application/json" id="data">__DATA__</script>
<script>
const D = JSON.parse(document.getElementById('data').textContent);
const $ = id => document.getElementById(id);
const css = n => getComputedStyle(document.documentElement).getPropertyValue(n).trim();
const esc = v => String(v == null ? '' : v).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
const fmt = t => new Date(t * 1000).toISOString().replace('T', ' ').slice(0, 16);
const day = t => new Date(t * 1000).toLocaleDateString('en-GB', { weekday: 'short', day: '2-digit', month: 'short', timeZone: 'UTC' });
const price = v => v == null ? '-' : Math.abs(v) >= 1000 ? v.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : v.toFixed(Math.abs(v) < 10 ? 3 : 2);
const pct = v => (v > 0 ? '+' : '') + v.toFixed(3) + ' %';
const side = v => v > 0 ? 'BUY' : v < 0 ? 'SELL' : '';
const arrow = v => v > 0 ? '<span class="BUY" title="EMA38 above EMA100">&#9650;</span>' : v < 0 ? '<span class="SELL" title="EMA38 below EMA100">&#9660;</span>' : '';
const TYPE = { E: 'Equity', I: 'Index' };
const PLACE = { FR: 'Paris', NL: 'Amsterdam', ETR: 'Frankfurt' };
const WARMUP = 230;  // windows until (1 - 2/101)^n < 1%, i.e. EMA100 has forgotten EMA_0 = 0
const bySym = Object.fromEntries(D.symbols.map(x => [x.s, x]));
const advOf = {};
D.advisories.forEach(a => (advOf[a.s] = advOf[a.s] || []).push(a));
let current = null, index = new Map();
const state = { x: '', k: '', sort: 'latest', dir: -1 };

// Per-symbol figures for the watchlist and the side panels, from the chart series.
D.symbols.forEach(x => {
  const s = D.series[x.s], n = s.t.length, i = n - 1;
  x.c = s.c[i]; x.a = s.a[i]; x.b = s.b[i];
  x.sp = x.b ? (x.a - x.b) / Math.abs(x.b) * 100 : 0;
  x.hi = Math.max(...s.c); x.lo = Math.min(...s.c); x.avg = s.c.reduce((u, v) => u + v, 0) / n;
  const from = Math.min(WARMUP, Math.floor(n / 2));
  let up = 0;
  for (let j = from; j < n; j++) if (s.a[j] > s.b[j]) up++;
  x.settled = n - from; x.up = x.settled ? up / x.settled : 0;
  x.t0 = s.t[0]; x.t1 = s.t[i];
});

$('n-ema').textContent = D.totals.ema.toLocaleString();
$('n-first').textContent = D.totals.first.toLocaleString();
if (D.metrics && D.metrics.events_per_second) $('perf').innerHTML = 'throughput <b>' + Math.round(D.metrics.events_per_second).toLocaleString() + '</b> events/s, p99 latency <b>' + ((D.metrics.latency_ms || {}).p99 ?? '-') + '</b> ms';
if (D.symbols.length) $('range').textContent = day(Math.min(...D.symbols.map(x => x.t0))) + ' - ' + day(Math.max(...D.symbols.map(x => x.t1))) + ' ' + new Date(D.symbols[0].t1 * 1000).getUTCFullYear();
const exchanges = [...new Set(D.symbols.map(x => x.x).filter(Boolean))].sort();
$('tabs-x').innerHTML = '<button class="tab on" data-x="">All exchanges</button> ' + exchanges.map(x => '<button class="tab" data-x="' + esc(x) + '">' + esc(PLACE[x] ? PLACE[x] + ' (' + x + ')' : x) + '</button>').join(' ');

// Ticker strip: the most active indices first, then other symbols.
const strip = D.symbols.slice().sort((a, b) => (b.k === 'I') - (a.k === 'I') || b.rows - a.rows).slice(0, 6);
$('ticker').innerHTML = strip.map(x => '<div class="tick" data-s="' + esc(x.s) + '">' + arrow(x.sp) + '<b>' + esc(x.s) + '</b><span class="' + side(x.sp) + '">' + pct(x.sp) + '</span><small>' + price(x.c) + '</small></div>').join('');
$('ticker').onclick = e => { const el = e.target.closest('.tick'); if (el) show(el.dataset.s); };

const tick = (t, type) => type < 3 ? new Date(t * 1000).toLocaleDateString('en-GB', { day: '2-digit', month: 'short', timeZone: 'UTC' }) : new Date(t * 1000).toISOString().slice(11, 16);
const base = { layout: { background: { color: 'transparent' }, textColor: css('--muted'), fontSize: 11, attributionLogo: false },
  grid: { vertLines: { color: css('--grid') }, horzLines: { color: css('--grid') } }, localization: { timeFormatter: fmt },
  timeScale: { timeVisible: true, secondsVisible: false, borderColor: css('--line'), tickMarkFormatter: tick, rightOffset: 3 },
  rightPriceScale: { borderColor: css('--line'), minimumWidth: 72 } };
const chart = LightweightCharts.createChart($('chart'), { ...base, autoSize: true });
const closeS = chart.addAreaSeries({ lineColor: css('--close'), topColor: css('--close-top'), bottomColor: css('--close-bottom'), lineWidth: 2, lastValueVisible: false, priceLineVisible: false });
// The price axis follows the close prices only, so the EMA warm-up from 0 is clipped instead of squashing the chart.
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
const hist = breadthChart.addHistogramSeries({ priceLineVisible: false, lastValueVisible: false, priceFormat: { type: 'price', precision: 0, minMove: 1 } });

// The line above the chart is both the legend and the read-out for the window under the crosshair.
function readout(i) {
  const s = D.series[current], v = s.b[i] ? (s.a[i] - s.b[i]) / Math.abs(s.b[i]) * 100 : 0;
  $('readout').innerHTML = '<b>' + esc(current) + '</b><span>' + fmt(s.t[i]) + '</span>'
    + '<span><i style="background:var(--close)"></i>Close <b>' + price(s.c[i]) + '</b></span>'
    + '<span><i style="background:var(--e38)"></i>EMA38 <b>' + price(s.a[i]) + '</b></span>'
    + '<span><i style="background:var(--e100)"></i>EMA100 <b>' + price(s.b[i]) + '</b></span>'
    + '<span>Spread <b class="' + side(v) + '">' + pct(v) + '</b></span>'
    + '<span><b class="BUY">&#9650;</b> buy</span><span><b class="SELL">&#9660;</b> sell</span>';
}
[chart, spreadChart].forEach(c => c.subscribeCrosshairMove(p => {
  if (!current) return;
  const i = p && p.time != null ? index.get(p.time) : undefined;
  readout(i == null ? D.series[current].t.length - 1 : i);
}));

function filters() {
  return { q: $('q').value.trim().toLowerCase(), x: state.x, k: state.k, trend: $('trend').value, first: $('first').checked };
}
function visible(f) {
  const rows = D.symbols.filter(x => (!f.q || x.s.toLowerCase().includes(f.q) || (x.isin || '').toLowerCase().includes(f.q))
    && (!f.x || x.x === f.x) && (!f.k || x.k === f.k) && (!f.trend || x.trend === f.trend));
  const key = state.sort, val = x => key === 'latest' ? (x.last_t || 0) : x[key];
  return rows.sort((a, b) => {
    const u = val(a), v = val(b);
    const c = typeof u === 'string' || typeof v === 'string' ? String(u || '').localeCompare(String(v || '')) : (u || 0) - (v || 0);
    return c * state.dir || a.s.localeCompare(b.s);
  });
}
function marker(a) {
  if (a.w) return { time: a.t, position: 'belowBar', color: css('--first'), shape: 'circle', text: 'warm-up ' + a.k };
  return a.k === 'BUY'
    ? { time: a.t, position: 'belowBar', color: css('--buy'), shape: 'arrowUp', text: 'BUY' }
    : { time: a.t, position: 'aboveBar', color: css('--sell'), shape: 'arrowDown', text: 'SELL' };
}
function split(left, right, a, b, none) {
  if (!a && !b) return '<div class="split"><div class="none">' + none + '</div></div>';
  const fits = v => v / (a + b) >= 0.3;  // a narrow segment stays unlabelled; the line below the bar names both sides
  return '<div class="split">' + (a ? '<div class="b" style="flex:' + a + '">' + (fits(a) ? left : '') + '</div>' : '') + (b ? '<div class="s" style="flex:' + b + '">' + (fits(b) ? right : '') + '</div>' : '') + '</div>';
}
function show(sym) {
  if (!bySym[sym]) return;
  current = sym;
  const s = D.series[sym], x = bySym[sym], f = filters();
  index = new Map(s.t.map((t, i) => [t, i]));
  document.querySelectorAll('#list tr').forEach(el => el.classList.toggle('on', el.dataset.s === sym));
  closeS.setData(s.t.map((t, i) => ({ time: t, value: s.c[i] })));
  e38.setData(s.t.map((t, i) => ({ time: t, value: s.a[i] })));
  e100.setData(s.t.map((t, i) => ({ time: t, value: s.b[i] })));
  const sp = s.t.map((t, i) => ({ time: t, value: s.b[i] ? (s.a[i] - s.b[i]) / Math.abs(s.b[i]) * 100 : 0 }));
  spread.setData(sp);
  // Scale the trend panel to the windows after the EMA100 warm-up; the climb from EMA_0 = 0 would flatten everything else.
  const settled = sp.slice(Math.min(WARMUP, Math.floor(sp.length / 2))).map(p => p.value);
  const lo = Math.min(0, ...settled), hi = Math.max(0, ...settled), pad = (hi - lo) * 0.1 || 0.01;
  spread.applyOptions({ autoscaleInfoProvider: () => ({ priceRange: { minValue: lo - pad, maxValue: hi + pad } }) });
  const all = advOf[sym] || [], adv = all.filter(a => f.first || !a.w), real = all.filter(a => !a.w);
  e38.setMarkers(adv.map(marker));
  spread.setMarkers(real.map(a => ({ time: a.t, position: a.k === 'BUY' ? 'belowBar' : 'aboveBar', color: css(a.k === 'BUY' ? '--buy' : '--sell'), shape: 'circle' })));
  requestAnimationFrame(() => { chart.timeScale().fitContent(); spreadChart.timeScale().fitContent(); });
  readout(s.t.length - 1);

  // Left panel: signal counts, trend share, close statistics and crossovers per day.
  const days = new Map();
  s.t.forEach(t => { const d = t - t % 86400; if (!days.has(d)) days.set(d, [0, 0]); });
  real.forEach(a => { const d = days.get(a.t - a.t % 86400); if (d) d[a.k === 'BUY' ? 0 : 1]++; });
  const most = Math.max(1, ...[...days.values()].map(d => Math.max(d[0], d[1])));
  const upPct = Math.round(x.up * 100);
  $('side-title').textContent = sym;
  $('side').innerHTML = '<h3>Crossover signals <small>after the EMA warm-up</small></h3>'
    + split('BUY ' + x.buy, 'SELL ' + x.sell, x.buy, x.sell, 'no crossover yet')
    + '<div class="note">' + x.buy + ' buy and ' + x.sell + ' sell advisories</div>'
    + '<h3>Trend share <small>of ' + x.settled.toLocaleString() + ' settled windows</small></h3>'
    + split(upPct + ' % up', (100 - upPct) + ' % down', upPct, 100 - upPct, 'no settled windows')
    + '<div class="note">EMA38 above EMA100 in ' + upPct + ' % of them, below in ' + (100 - upPct) + ' %</div>'
    + '<h3>Close price' + (x.cur ? ' <small>in ' + esc(x.cur) + '</small>' : '') + '</h3>'
    + '<table><tr><th class="n">Last</th><th class="n">Average</th><th class="n">High</th><th class="n">Low</th></tr><tr><td class="n">' + price(x.c) + '</td><td class="n">' + price(x.avg) + '</td><td class="n">' + price(x.hi) + '</td><td class="n">' + price(x.lo) + '</td></tr></table>'
    + '<h3>Crossovers by day</h3><table class="days">' + [...days].map(([d, c]) => '<tr><td>' + day(d) + '</td>'
      + '<td><i class="bar" style="background:var(--buy);width:' + Math.round(c[0] / most * 46) + 'px"></i>' + c[0] + ' buy</td>'
      + '<td><i class="bar" style="background:var(--sell);width:' + Math.round(c[1] / most * 46) + 'px"></i>' + c[1] + ' sell</td></tr>').join('') + '</table>'
    + '<h3>Latest windows</h3><table><tr><th>Window end</th><th class="n">Close</th><th class="n">EMA38</th><th class="n">EMA100</th></tr>'
    + s.t.slice(-12).reverse().map((t, j) => { const i = s.t.length - 1 - j; return '<tr><td>' + fmt(t).slice(5) + '</td><td class="n">' + price(s.c[i]) + '</td><td class="n">' + price(s.a[i]) + '</td><td class="n">' + price(s.b[i]) + '</td></tr>'; }).join('') + '</table>';

  // Right panel: quote, key data and this symbol's advisories.
  const kv = [['Exchange', PLACE[x.x] ? PLACE[x.x] + ' (' + x.x + ')' : x.x], ['Type', TYPE[x.k]], ['ISIN', x.isin], ['Currency', x.cur], ['Related index', x.index],
    ['Windows with trades', x.rows.toLocaleString()], ['First window', fmt(x.t0)], ['Last window', fmt(x.t1)], ['EMA38', price(x.a)], ['EMA100', price(x.b)],
    ['Current trend', x.sp > 0 ? 'up (EMA38 above EMA100)' : 'down (EMA38 below EMA100)'], ['Last advisory', x.last ? x.last + ' at ' + fmt(x.last_t) : 'none after warm-up']];
  $('over-title').textContent = sym + ' - Overview';
  $('over').innerHTML = '<div class="name">' + esc(sym) + '</div>'
    + '<div class="quote"><span class="last">' + arrow(x.sp) + ' ' + price(x.c) + '</span><span class="chg ' + side(x.sp) + '">' + pct(x.sp) + '<small>EMA38 vs EMA100</small></span></div>'
    + '<h3>Key data</h3><table class="kv">' + kv.filter(r => r[1]).map(r => '<tr><td>' + r[0] + '</td><td>' + esc(r[1]) + '</td></tr>').join('') + '</table>'
    + '<h3>Advisories <small>newest first</small></h3><table class="rows"><thead><tr><th>Window end</th><th>Advisory</th><th class="n">EMA38</th><th class="n">EMA100</th></tr></thead><tbody>'
    + (adv.slice().reverse().map(a => '<tr><td>' + fmt(a.t) + '</td><td class="' + (a.w ? 'FIRST' : a.k) + '"><b>' + a.k + '</b>' + (a.w ? ' (warm-up)' : '') + '</td><td class="n">' + a.a.toFixed(4) + '</td><td class="n">' + a.b.toFixed(4) + '</td></tr>').join('') || '<tr><td colspan="4">No advisories</td></tr>') + '</tbody></table>';
  try { history.replaceState(null, '', '#' + encodeURIComponent(sym)); } catch (e) {}
}
function render() {
  const f = filters(), rows = visible(f), names = new Set(rows.map(x => x.s));
  $('n-sym').textContent = rows.length + (rows.length === D.symbols.length ? '' : ' of ' + D.symbols.length);
  $('n-buy').textContent = rows.reduce((n, x) => n + x.buy, 0).toLocaleString();
  $('n-sell').textContent = rows.reduce((n, x) => n + x.sell, 0).toLocaleString();
  $('watch-note').textContent = rows.length + ' symbols, click a column to sort';
  document.querySelectorAll('#watch th[data-sort]').forEach(th => {
    const on = th.dataset.sort === state.sort;
    th.setAttribute('aria-sort', on ? (state.dir > 0 ? 'ascending' : 'descending') : 'none');
    th.dataset.dir = on ? ' ' + String.fromCharCode(state.dir > 0 ? 9650 : 9660) : '';
  });
  // A full bar is the upper quartile of the spreads shown; larger spreads are capped, so a few outliers do not flatten the rest.
  const sizes = rows.map(x => Math.abs(x.sp)).sort((a, b) => a - b), scale = sizes[Math.floor(sizes.length * 0.75)] || sizes[sizes.length - 1] || 1;
  $('none').hidden = rows.length > 0;
  $('list').innerHTML = rows.map(x => {
    const w = Math.min(100, Math.abs(x.sp) / scale * 100).toFixed(0);
    return '<tr data-s="' + esc(x.s) + '"><td>' + arrow(x.sp) + ' <b>' + esc(x.s) + '</b></td><td>' + (TYPE[x.k] || '') + '</td><td>' + esc(x.x || '') + '</td>'
      + '<td class="n">' + price(x.c) + '</td><td class="n">' + price(x.a) + '</td><td class="n">' + price(x.b) + '</td><td class="n ' + side(x.sp) + '">' + pct(x.sp) + '</td>'
      + '<td><div class="dbar"><span>' + (x.sp < 0 ? '<i class="neg" style="width:' + w + '%"></i>' : '') + '</span><span>' + (x.sp > 0 ? '<i class="pos" style="width:' + w + '%"></i>' : '') + '</span></div></td>'
      + '<td class="n">' + x.buy + '</td><td class="n">' + x.sell + '</td>'
      + (x.last ? '<td><span class="chip ' + x.last + '">' + x.last + '</span></td><td>' + fmt(x.last_t) + '</td>' : '<td colspan="2" class="FIRST">no crossover</td>') + '</tr>';
  }).join('');
  const buckets = new Map(), tape = [];
  D.advisories.forEach(a => {
    if (a.w || !names.has(a.s)) return;
    const h = a.t - a.t % 3600;
    buckets.set(h, (buckets.get(h) || 0) + (a.k === 'BUY' ? 1 : -1));
    tape.push(a);
  });
  hist.setData([...buckets].sort((a, b) => a[0] - b[0]).map(([t, v]) => ({ time: t, value: v, color: v >= 0 ? css('--buy') : css('--sell') })));
  requestAnimationFrame(() => breadthChart.timeScale().fitContent());
  tape.sort((a, b) => b.t - a.t || a.s.localeCompare(b.s));
  $('tape-rows').innerHTML = tape.slice(0, 60).map(a => '<tr data-s="' + esc(a.s) + '"><td>' + fmt(a.t) + '</td><td><b>' + esc(a.s) + '</b></td><td><span class="chip ' + a.k + '">' + a.k + '</span></td><td class="n">' + price(a.a) + '</td></tr>').join('')
    || '<tr><td class="FIRST">No crossovers for these filters</td></tr>';
  const keep = current && names.has(current) ? current : rows.length ? rows[0].s : null;
  if (keep) show(keep);
}
function tabs(id, key) {
  $(id).addEventListener('click', e => {
    const b = e.target.closest('.tab');
    if (!b) return;
    state[key] = b.dataset[key];
    $(id).querySelectorAll('.tab').forEach(t => t.classList.toggle('on', t === b));
    render();
  });
}
tabs('tabs-x', 'x'); tabs('tabs-k', 'k');
['q', 'trend', 'first'].forEach(id => $(id).addEventListener('input', render));
$('watch').querySelector('thead').addEventListener('click', e => {
  const th = e.target.closest('th[data-sort]');
  if (!th) return;
  const text = ['s', 'k', 'x'].includes(th.dataset.sort);
  state.dir = state.sort === th.dataset.sort ? -state.dir : text ? 1 : -1;
  state.sort = th.dataset.sort;
  render();
});
[['list', 'tr'], ['tape-rows', 'tr']].forEach(([id, sel]) => $(id).addEventListener('click', e => { const el = e.target.closest(sel); if (el && el.dataset.s) show(el.dataset.s); }));
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
    columns = {row[0] for row in duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()}
    currency = "currency" if "currency" in columns else "NULL"  # older metadata files have no currency column
    rows = duckdb.sql(
        f"SELECT symbol, isin, related_indices, sec_type, exchange, {currency} FROM read_parquet('{path}')").fetchall()
    return {symbol: {"isin": isin, "index": index, "sec_type": sec_type, "exchange": exchange, "currency": cur}
            for symbol, isin, index, sec_type, exchange, cur in rows}


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
    advisory_currency = {a["symbol"]: a["currency"] for a in advisories if a.get("currency")}
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
                        "cur": meta.get("currency") or advisory_currency.get(s),
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
