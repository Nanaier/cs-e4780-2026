package fi.aalto.cse4780.trading;

import java.util.Set;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class ControlledEmaCrossoverFunction extends KeyedCoProcessFunction<String, Tick, Control, String> {

    private static final String[] UNKNOWN_INSTRUMENT = {"", ""};

    private final Set<String> subscribedSymbols;
    private final boolean enrich;
    private final boolean dynamicSubscriptions;
    private final EmaWindows windows;

    private transient ValueState<Instrument> instrument;
    private transient MapState<String, Boolean> subscribers;

    public ControlledEmaCrossoverFunction(Set<String> subscribedSymbols, boolean suppressFirst, boolean latencyColumns,
                                          boolean enrich, boolean dynamicSubscriptions) {
        this.subscribedSymbols = subscribedSymbols;
        this.enrich = enrich;
        this.dynamicSubscriptions = dynamicSubscriptions;
        this.windows = new EmaWindows(suppressFirst, latencyColumns);
    }

    @Override
    public void open(OpenContext openContext) {
        windows.open(getRuntimeContext());
        instrument = getRuntimeContext().getState(new ValueStateDescriptor<>("instrument", Instrument.class));
        subscribers = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("subscribers", Types.STRING, Types.BOOLEAN));
    }

    @Override
    public void processElement1(Tick tick, Context ctx, Collector<String> out) throws Exception {
        if (!windows.add(tick, ctx.timerService())) {
            ctx.output(EmaCrossoverFunction.LATE_TICKS, tick);
        }
    }

    @Override
    public void processElement2(Control control, Context ctx, Collector<String> out) throws Exception {
        if (control.subscriber() == null) {
            if (control.value() == null) {
                instrument.clear();
            } else {
                instrument.update(Instrument.parse(control.value()));
            }
        } else if ("subscribe".equals(control.value())) {
            subscribers.put(control.subscriber(), Boolean.TRUE);
        } else {
            subscribers.remove(control.subscriber());
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        String symbol = ctx.getCurrentKey();
        boolean watched = dynamicSubscriptions
                ? !subscribers.isEmpty()
                : subscribedSymbols == null || subscribedSymbols.contains(symbol);
        windows.close(timestamp, symbol, watched, enrich ? instrumentFields() : null, out,
                line -> ctx.output(EmaCrossoverFunction.ADVISORIES, line));
    }

    private String[] instrumentFields() throws Exception {
        Instrument known = instrument.value();
        return known == null ? UNKNOWN_INSTRUMENT : new String[] {known.isin(), known.currency()};
    }
}
