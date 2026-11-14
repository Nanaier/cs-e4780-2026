package fi.aalto.cse4780.trading;

import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

public class TickDeserializer extends AbstractDeserializationSchema<Tick> {

    @Override
    public Tick deserialize(byte[] message) {
        return Tick.parse(new String(message, StandardCharsets.UTF_8));
    }
}
