package fi.aalto.cse4780.trading;

public record EmaState(double ema38, double ema100, boolean evaluated) {

    public static final EmaState INITIAL = new EmaState(0.0, 0.0, false);

    private static final double SHORT_ALPHA = 2.0 / (1.0 + 38);
    private static final double LONG_ALPHA = 2.0 / (1.0 + 100);

    public EmaState next(double close) {
        return new EmaState(
                close * SHORT_ALPHA + ema38 * (1.0 - SHORT_ALPHA),
                close * LONG_ALPHA + ema100 * (1.0 - LONG_ALPHA),
                true);
    }

    public String crossoverFrom(EmaState previous) {
        if (ema38 > ema100 && previous.ema38 <= previous.ema100) {
            return "BUY";
        }
        if (ema38 < ema100 && previous.ema38 >= previous.ema100) {
            return "SELL";
        }
        return null;
    }
}
