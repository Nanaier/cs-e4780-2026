package fi.aalto.cse4780.trading;

import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ControlDeserializer implements KafkaRecordDeserializationSchema<Control> {

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Control> out) {
        String split = split(record.topic(), record.partition());
        String key = record.key() == null ? null : new String(record.key(), StandardCharsets.UTF_8);
        String value = record.value() == null ? null : new String(record.value(), StandardCharsets.UTF_8);
        int bar = key == null ? -1 : key.indexOf('|');
        String symbol = bar < 0 ? null : key.substring(bar + 1);
        out.collect(new Control(symbol, bar < 0 ? null : key.substring(0, bar), value, split, record.offset()));
    }

    @Override
    public TypeInformation<Control> getProducedType() {
        return TypeInformation.of(Control.class);
    }

    public static String split(String topic, int partition) {
        return topic + "-" + partition;
    }
}
