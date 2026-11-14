package fi.aalto.cse4780.trading;

public record Instrument(String exchange, String secType, String isin, String currency, String relatedIndices) {

    public static Instrument parse(String value) {
        String[] fields = java.util.Arrays.copyOf(value.split(",", -1), 5);
        for (int i = 0; i < fields.length; i++) {
            fields[i] = fields[i] == null ? "" : fields[i];
        }
        return new Instrument(fields[0], fields[1], fields[2], fields[3], fields[4]);
    }
}
