package fi.aalto.cse4780.trading;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.flink.api.connector.sink2.SinkWriter;

public class ResultFiles implements SinkWriter<String> {

    public static final String EMA_TAG = "ema";
    public static final String ADVISORY_TAG = "advisory";
    public static final String LATE_TAG = "late";

    private static final String EMA_HEADER = "symbol,window_end_ms,window_end,close,ema38,ema100";
    private static final String ADVISORY_HEADER =
            "symbol,window_end_ms,window_end,advisory,ema38,ema100,prev_ema38,prev_ema100";

    private final BufferedWriter emaFile;
    private final BufferedWriter advisoryFile;
    private long emaRows;
    private long advisories;
    private long lateTicks;

    public ResultFiles(String outDir) throws IOException {
        File dir = new File(outDir);
        Files.createDirectories(dir.toPath());
        emaFile = Files.newBufferedWriter(new File(dir, "ema.csv").toPath());
        advisoryFile = Files.newBufferedWriter(new File(dir, "advisories.csv").toPath());
        writeLine(emaFile, EMA_HEADER);
        writeLine(advisoryFile, ADVISORY_HEADER);
    }

    @Override
    public void write(String taggedResult, Context context) throws IOException {
        int separator = taggedResult.indexOf(',');
        String tag = taggedResult.substring(0, separator);
        String line = taggedResult.substring(separator + 1);
        switch (tag) {
            case EMA_TAG -> {
                writeLine(emaFile, line);
                emaRows++;
            }
            case ADVISORY_TAG -> {
                writeLine(advisoryFile, line);
                advisories++;
            }
            case LATE_TAG -> lateTicks++;
            default -> throw new IllegalStateException("unknown result tag: " + tag);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        emaFile.flush();
        advisoryFile.flush();
        if (endOfInput) {
            System.out.println("ema_rows=" + emaRows + " advisories=" + advisories + " late_ticks=" + lateTicks);
        }
    }

    @Override
    public void close() throws IOException {
        emaFile.close();
        advisoryFile.close();
    }

    private static void writeLine(BufferedWriter writer, String line) throws IOException {
        writer.write(line);
        writer.newLine();
    }
}
