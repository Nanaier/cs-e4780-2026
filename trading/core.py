from dataclasses import dataclass

WINDOW_MS = 300_000
SHORT_J = 38
LONG_J = 100
HEARTBEAT = "__heartbeat__"


@dataclass(frozen=True)
class Params:
    window_ms: int = WINDOW_MS
    out_of_orderness_ms: int = 0
    empty_window: str = "skip"
    suppress_first: bool = False


@dataclass(frozen=True)
class EmaRow:
    symbol: str
    window_start: int
    window_end: int
    close: float
    ema38: float
    ema100: float


@dataclass(frozen=True)
class Advisory:
    symbol: str
    window_end: int
    kind: str
    ema38: float
    ema100: float
    prev_ema38: float
    prev_ema100: float


def alpha(j):
    return 2.0 / (1.0 + j)


def ema_step(close, previous, a):
    return close * a + previous * (1.0 - a)


def quarantined(event_ts, sys_ts, quarantine_ms):
    return sys_ts is not None and sys_ts - event_ts > quarantine_ms


def quarantine_sql(quarantine_ms):
    return f"(sys_ts IS NOT NULL AND sys_ts - event_ts > {int(quarantine_ms)})"


def crossover(prev_short, prev_long, short, long):
    if short > long and prev_short <= prev_long:
        return "BUY"
    if short < long and prev_short >= prev_long:
        return "SELL"
    return None


class EmaCrossoverOperator:
    def __init__(self, params=Params(), subscribed=None, on_ema=None, on_advisory=None):
        if params.empty_window not in ("skip", "repeat"):
            raise ValueError("empty_window must be 'skip' or 'repeat'")
        self.params = params
        self.a_short = alpha(SHORT_J)
        self.a_long = alpha(LONG_J)
        self.subscribed = subscribed
        self.on_ema = on_ema or (lambda row, trigger: None)
        self.on_advisory = on_advisory or (lambda advisory, trigger: None)
        self.state = {}
        self.open = {}
        self.max_event_ts = None
        self.closed_until = None
        self.counts = {
            "events": 0,
            "late_events": 0,
            "windows_closed": 0,
            "ema_updates": 0,
            "ema_emitted": 0,
            "advisories": 0,
            "advisories_emitted": 0,
            "first_window_signals": 0,
        }

    def process(self, symbol, price, event_ts, seq, trigger=None):
        if not self.ingest(symbol, price, event_ts, seq):
            return
        if self.max_event_ts is None or event_ts > self.max_event_ts:
            self.max_event_ts = event_ts
            self.advance(event_ts - self.params.out_of_orderness_ms, trigger)

    def ingest(self, symbol, price, event_ts, seq):
        self.counts["events"] += 1
        w = self.params.window_ms
        start = event_ts - event_ts % w
        if self.closed_until is not None and start < self.closed_until:
            self.counts["late_events"] += 1
            return False
        bucket = self.open.get(start)
        if bucket is None:
            bucket = self.open[start] = {}
        current = bucket.get(symbol)
        if current is None or (event_ts, seq) >= (current[0], current[1]):
            bucket[symbol] = (event_ts, seq, price)
        return True

    def advance(self, watermark, trigger=None):
        w = self.params.window_ms
        boundary = watermark - watermark % w
        if self.closed_until is not None and boundary <= self.closed_until:
            return
        for start in self._windows_before(boundary):
            self._close(start, trigger)
        self.closed_until = boundary

    def flush(self, trigger=None):
        if not self.open:
            return
        last = max(self.open)
        self.advance(last + self.params.window_ms, trigger)

    def _windows_before(self, boundary):
        ready = sorted(start for start in self.open if start < boundary)
        if self.params.empty_window == "skip" or (not ready and self.closed_until is None):
            return ready
        first = self.closed_until if self.closed_until is not None else ready[0]
        if ready:
            first = min(first, ready[0])
        return range(first, boundary, self.params.window_ms)

    def _close(self, start, trigger):
        bucket = self.open.pop(start, None) or {}
        self.counts["windows_closed"] += 1
        end = start + self.params.window_ms
        for symbol, (_, _, price) in bucket.items():
            self._update(symbol, start, end, price, trigger)
        if self.params.empty_window == "repeat":
            for symbol, st in list(self.state.items()):
                if symbol not in bucket:
                    self._update(symbol, start, end, st[3], trigger)

    def _update(self, symbol, start, end, close, trigger):
        st = self.state.get(symbol)
        if st is None:
            st = self.state[symbol] = [0.0, 0.0, False, close]
        prev_short, prev_long, evaluated = st[0], st[1], st[2]
        ema38 = ema_step(close, prev_short, self.a_short)
        ema100 = ema_step(close, prev_long, self.a_long)
        st[0], st[1], st[2], st[3] = ema38, ema100, True, close
        self.counts["ema_updates"] += 1
        watched = self.subscribed is None or symbol in self.subscribed
        if watched:
            self.counts["ema_emitted"] += 1
            self.on_ema(EmaRow(symbol, start, end, close, ema38, ema100), trigger)
        kind = crossover(prev_short, prev_long, ema38, ema100)
        if kind is None:
            return
        if not evaluated:
            self.counts["first_window_signals"] += 1
            if self.params.suppress_first:
                return
        self.counts["advisories"] += 1
        if watched:
            self.counts["advisories_emitted"] += 1
            self.on_advisory(Advisory(symbol, end, kind, ema38, ema100, prev_short, prev_long), trigger)
