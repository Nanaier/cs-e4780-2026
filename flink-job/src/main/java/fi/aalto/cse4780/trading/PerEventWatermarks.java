package fi.aalto.cse4780.trading;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * Bounded out-of-orderness watermarks emitted on every tick that raises the maximum event time, instead of every
 * 200 ms of processing time (Flink's periodic default). Switch: --watermark-every-event.
 *
 * With periodic emission, which ticks count as late depends on how many ticks the source reads between two
 * emissions, so a fast file replay drops fewer late ticks than the batch reference. Emitting per tick makes the
 * watermark a function of the input order alone, the rule trading/core.py and reference.py --late-bound-ms apply.
 * The watermark value is the same as Flink's BoundedOutOfOrderness generator: max event time - bound - 1.
 */
public class PerEventWatermarks implements WatermarkGenerator<Tick> {

    private final long boundMs;
    private long maxTs = Long.MIN_VALUE;

    public PerEventWatermarks(long boundMs) {
        this.boundMs = boundMs;
    }

    @Override
    public void onEvent(Tick tick, long eventTimestamp, WatermarkOutput output) {
        if (eventTimestamp > maxTs) {
            maxTs = eventTimestamp;
            output.emitWatermark(new Watermark(maxTs - boundMs - 1));
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // every watermark is already emitted in onEvent
    }
}
