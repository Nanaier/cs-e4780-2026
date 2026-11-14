package fi.aalto.cse4780.trading;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Consumer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.Collector;

final class EmaWindows implements Serializable {

    private static final DateTimeFormatter WINDOW_END_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC);

    private final boolean suppressFirst;
    private final boolean latencyColumns;

    private transient MapState<Long, WindowClose> openWindows;
    private transient ValueState<EmaState> emaState;
    private transient Counter lateTicks;
    private transient long newestIngestMs;

    EmaWindows(boolean suppressFirst, boolean latencyColumns) {
        this.suppressFirst = suppressFirst;
        this.latencyColumns = latencyColumns;
    }

    void open(RuntimeContext context) {
        openWindows = context.getMapState(
                new MapStateDescriptor<>("open-windows", Types.LONG, TypeInformation.of(WindowClose.class)));
        emaState = context.getState(new ValueStateDescriptor<>("ema", EmaState.class));
        lateTicks = context.getMetricGroup().counter("lateTicks");
        newestIngestMs = 0L;
    }

    boolean add(Tick tick, TimerService timers) throws Exception {
        newestIngestMs = Math.max(newestIngestMs, tick.ingestMs());
        long windowStart = tick.eventTs() - Math.floorMod(tick.eventTs(), EmaCrossoverFunction.WINDOW_MS);
        long firingTime = windowStart + EmaCrossoverFunction.WINDOW_MS - 1;
        if (firingTime <= timers.currentWatermark()) {
            lateTicks.inc();
            return false;
        }
        WindowClose current = openWindows.get(windowStart);
        if (current == null) {
            timers.registerEventTimeTimer(firingTime);
            openWindows.put(windowStart, WindowClose.of(tick));
        } else {
            openWindows.put(windowStart, current.merge(tick));
        }
        return true;
    }

    void close(long timestamp, String symbol, boolean watched, String[] advisoryExtra,
               Collector<String> emaLines, Consumer<String> advisories) throws Exception {
        long windowEnd = timestamp + 1;
        long windowStart = windowEnd - EmaCrossoverFunction.WINDOW_MS;
        WindowClose window = openWindows.get(windowStart);
        openWindows.remove(windowStart);

        EmaState previous = Objects.requireNonNullElse(emaState.value(), EmaState.INITIAL);
        EmaState current = previous.next(window.price());
        emaState.update(current);
        if (!watched) {
            return;
        }
        String windowEndText = WINDOW_END_FORMAT.format(Instant.ofEpochMilli(windowEnd));
        emaLines.collect(csvLine(null, symbol, windowEnd, windowEndText, window.price(), current.ema38(), current.ema100()));

        String advisory = current.crossoverFrom(previous);
        if (advisory != null && (previous.evaluated() || !suppressFirst)) {
            advisories.accept(csvLine(advisoryExtra, symbol, windowEnd, windowEndText, advisory,
                    current.ema38(), current.ema100(), previous.ema38(), previous.ema100()));
        }
    }

    private String csvLine(String[] extra, Object... fields) {
        StringJoiner line = new StringJoiner(",");
        for (Object field : fields) {
            line.add(String.valueOf(field));
        }
        if (extra != null) {
            for (String field : extra) {
                line.add(field);
            }
        }
        if (latencyColumns) {
            line.add(String.valueOf(newestIngestMs));
            line.add(String.valueOf(System.currentTimeMillis()));
        }
        return line.toString();
    }
}
