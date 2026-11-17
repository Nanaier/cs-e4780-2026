package fi.aalto.cse4780.trading;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class ControlledEmaCrossoverFunction extends KeyedCoProcessFunction<String, Tick, Control, String> {

    private final EmaWindows windows;
    private final boolean enrich;
    private final long boundMs;
    private final InstrumentVersions instruments = new InstrumentVersions();

    private transient MapState<String, Boolean> subscribers;

    public ControlledEmaCrossoverFunction(boolean suppressFirst, boolean latencyColumns, boolean enrich, long boundMs) {
        this.windows = new EmaWindows(suppressFirst, latencyColumns);
        this.enrich = enrich;
        this.boundMs = boundMs;
    }

    @Override
    public void open(OpenContext openContext) {
        windows.open(getRuntimeContext());
        instruments.open(getRuntimeContext());
        subscribers = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("subscribers", Types.STRING, Types.BOOLEAN));
    }

    @Override
    public void processElement1(Tick tick, Context ctx, Collector<String> out) throws Exception {
        if (tick.metadata()) {
            if (enrich) {
                instruments.add(tick);
            }
            return;
        }
        if (!windows.add(tick, ctx.timerService())) {
            ctx.output(EmaCrossoverFunction.LATE_TICKS, tick);
        }
    }

    @Override
    public void processElement2(Control control, Context ctx, Collector<String> out) throws Exception {
        if ("subscribe".equals(control.value())) {
            subscribers.put(control.subscriber(), Boolean.TRUE);
        } else {
            subscribers.remove(control.subscriber());
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        windows.close(timestamp, ctx.getCurrentKey(), !subscribers.isEmpty(),
                enrich ? instruments.asOf(timestamp + 1 + boundMs) : null, out,
                line -> ctx.output(EmaCrossoverFunction.ADVISORIES, line));
    }
}
