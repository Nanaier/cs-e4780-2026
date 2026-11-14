package fi.aalto.cse4780.trading;

public record Control(String symbol, String subscriber, String value, String split, long offset) {

    public boolean valid() {
        return symbol != null;
    }
}
