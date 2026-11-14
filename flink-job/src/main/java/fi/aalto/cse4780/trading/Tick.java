package fi.aalto.cse4780.trading;

public record Tick(String symbol, double price, long eventTs, long seq, long ingestMs) {

    public static final String HEARTBEAT = "__heartbeat__";

    public static Tick parse(String line) {
        String[] fields = line.split(",");
        long ingestMs = fields.length > 5 ? Long.parseLong(fields[5]) : 0L;
        return new Tick(fields[0], Double.parseDouble(fields[2]), Long.parseLong(fields[3]), Long.parseLong(fields[4]), ingestMs);
    }

    public boolean heartbeat() {
        return HEARTBEAT.equals(symbol);
    }
}
