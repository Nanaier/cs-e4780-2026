package fi.aalto.cse4780.trading;

public record Tick(String symbol, double price, long eventTs, long seq, long ingestMs, boolean metadata,
                   String isin, String currency) {

    public static final String HEARTBEAT = "__heartbeat__";
    public static final String METADATA = "M";

    public static Tick parse(String line) {
        String[] fields = line.split(",", -1);
        long ingestMs = fields.length > 5 ? Long.parseLong(fields[5]) : 0L;
        boolean metadata = METADATA.equals(fields[1]);
        return new Tick(fields[0], Double.parseDouble(fields[2]), Long.parseLong(fields[3]), Long.parseLong(fields[4]),
                ingestMs, metadata, metadata ? fields[6] : null, metadata ? fields[7] : null);
    }

    public boolean heartbeat() {
        return HEARTBEAT.equals(symbol);
    }

    public long watermarkTs() {
        return metadata ? Long.MIN_VALUE : eventTs;
    }
}
