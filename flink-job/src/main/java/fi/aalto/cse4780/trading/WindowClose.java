package fi.aalto.cse4780.trading;

public record WindowClose(long eventTs, long seq, double price) {

    public static WindowClose of(Tick tick) {
        return new WindowClose(tick.eventTs(), tick.seq(), tick.price());
    }

    public WindowClose merge(Tick tick) {
        boolean tickIsLatest = tick.eventTs() > eventTs || (tick.eventTs() == eventTs && tick.seq() >= seq);
        return tickIsLatest ? of(tick) : this;
    }
}
