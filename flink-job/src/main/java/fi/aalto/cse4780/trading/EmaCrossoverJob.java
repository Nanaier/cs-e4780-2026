package fi.aalto.cse4780.trading;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public final class EmaCrossoverJob {

    private EmaCrossoverJob() {
    }

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(faultTolerance(params));
        env.setParallelism(params.getInt("parallelism", 1));

        WatermarkStrategy<Tick> watermarks = WatermarkStrategy
                .<Tick>forBoundedOutOfOrderness(Duration.ofMillis(params.getLong("out-of-orderness-ms", 0L)))
                .withTimestampAssigner((tick, recordTimestamp) -> tick.watermarkTs());
        Set<String> subscribedSymbols = loadSubscriptions(params.get("subscriptions", "all"));
        boolean suppressFirst = params.has("suppress-first");
        if (params.has("instruments-topic")) {
            throw new IllegalArgumentException("--instruments-topic is gone: metadata now travels in the ticks topic "
                    + "(replay.py --with-metadata) and --enrich adds it to advisories");
        }

        switch (params.get("source", "file")) {
            case "file" -> runFiles(env, params, watermarks, new EmaCrossoverFunction(subscribedSymbols, suppressFirst,
                    false, params.has("enrich"), params.getLong("out-of-orderness-ms", 0L)));
            case "kafka" -> runKafka(env, params, watermarks, subscribedSymbols, suppressFirst);
            default -> throw new IllegalArgumentException("--source must be file or kafka");
        }
    }

    private static void runFiles(StreamExecutionEnvironment env, ParameterTool params,
                                 WatermarkStrategy<Tick> watermarks, EmaCrossoverFunction function) throws Exception {
        File inputDir = new File(params.getRequired("input"));
        File[] csvFiles = inputDir.listFiles((dir, name) -> name.endsWith(".csv"));
        if (csvFiles == null || csvFiles.length == 0) {
            throw new IllegalArgumentException("no .csv files in " + inputDir);
        }
        FileSource<String> source = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(),
                        Arrays.stream(csvFiles).map(Path::fromLocalFile).toArray(Path[]::new))
                .setSplitAssigner(PathOrderSplitAssigner::new)
                .build();

        SingleOutputStreamOperator<String> emaLines = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "tick-files")
                .map(Tick::parse)
                .assignTimestampsAndWatermarks(watermarks)
                .keyBy(Tick::symbol)
                .process(function);

        String outDir = params.getRequired("out");
        emaLines.map(line -> ResultFiles.EMA_TAG + "," + line)
                .union(
                        emaLines.getSideOutput(EmaCrossoverFunction.ADVISORIES)
                                .map(line -> ResultFiles.ADVISORY_TAG + "," + line),
                        emaLines.getSideOutput(EmaCrossoverFunction.LATE_TICKS)
                                .map(tick -> ResultFiles.LATE_TAG + "," + tick.symbol()))
                .sinkTo(context -> new ResultFiles(outDir, params.has("enrich")))
                .name("result-files")
                .setParallelism(1);

        executeTimed(env, "ema-crossover-files");
    }

    private static void runKafka(StreamExecutionEnvironment env, ParameterTool params, WatermarkStrategy<Tick> watermarks,
                                 Set<String> subscribedSymbols, boolean suppressFirst) throws Exception {
        String bootstrap = params.get("bootstrap", "localhost:9092");
        String subscriptionsTopic = params.get("subscriptions-topic");
        boolean enrich = params.has("enrich");
        long boundMs = params.getLong("out-of-orderness-ms", 0L);
        if (subscriptionsTopic != null && subscribedSymbols != null) {
            throw new IllegalArgumentException("give --subscriptions or --subscriptions-topic, not both");
        }
        KafkaSourceBuilder<Tick> sourceBuilder = KafkaSource.<Tick>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(params.get("topic", "ticks"))
                .setGroupId(params.get("group", "flink-ema-crossover"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new TickDeserializer());
        if (params.has("bounded")) {
            sourceBuilder.setBounded(OffsetsInitializer.latest());
        }

        DataStream<Tick> ticks = env.fromSource(sourceBuilder.build(), watermarks, "tick-topic")
                .filter(tick -> !tick.heartbeat())
                .name("drop-heartbeats");
        SingleOutputStreamOperator<String> emaLines;
        if (subscriptionsTopic == null) {
            emaLines = ticks.keyBy(Tick::symbol)
                    .process(new EmaCrossoverFunction(subscribedSymbols, suppressFirst, true, enrich, boundMs));
        } else {
            emaLines = ticks.keyBy(Tick::symbol)
                    .connect(controlStream(env, params, bootstrap, subscriptionsTopic).keyBy(Control::symbol))
                    .process(new ControlledEmaCrossoverFunction(suppressFirst, true, enrich, boundMs));
        }
        DeliveryGuarantee delivery = deliveryGuarantee(params);
        String transactionalIdPrefix = params.get("transactional-id-prefix", "ema-crossover-" + UUID.randomUUID());
        long transactionTimeoutMs = params.getLong("transaction-timeout-ms", 600_000L);
        emaLines.sinkTo(kafkaSink(bootstrap, params.get("ema-topic", "ema"), delivery, transactionalIdPrefix,
                transactionTimeoutMs));
        emaLines.getSideOutput(EmaCrossoverFunction.ADVISORIES)
                .sinkTo(kafkaSink(bootstrap, params.get("advisories-topic", "advisories"), delivery,
                        transactionalIdPrefix, transactionTimeoutMs));
        executeTimed(env, "ema-crossover-kafka");
    }

    private static DataStream<Control> controlStream(StreamExecutionEnvironment env, ParameterTool params, String bootstrap,
                                                     String subscriptionsTopic) {
        List<String> topics = List.of(subscriptionsTopic);
        KafkaSourceBuilder<Control> builder = KafkaSource.<Control>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topics)
                .setGroupId(params.get("group", "flink-ema-crossover") + "-control")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new ControlDeserializer());
        if (params.has("bounded")) {
            builder.setBounded(OffsetsInitializer.latest());
        }
        Map<String, Long> lastOffsets = lastOffsets(bootstrap, topics);
        return env.fromSource(builder.build(), WatermarkStrategy.noWatermarks(), "control-topics")
                .setParallelism(1)
                .assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator(context -> new ControlCatchUp(lastOffsets)))
                .setParallelism(1)
                .filter(Control::valid)
                .setParallelism(1);
    }

    private static Map<String, Long> lastOffsets(String bootstrap, List<String> topics) {
        Properties config = new Properties();
        config.put("bootstrap.servers", bootstrap);
        config.put("key.deserializer", ByteArrayDeserializer.class.getName());
        config.put("value.deserializer", ByteArrayDeserializer.class.getName());
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(config)) {
            List<TopicPartition> partitions = new ArrayList<>();
            for (String topic : topics) {
                for (PartitionInfo info : consumer.partitionsFor(topic, Duration.ofSeconds(30))) {
                    partitions.add(new TopicPartition(topic, info.partition()));
                }
            }
            Map<TopicPartition, Long> first = consumer.beginningOffsets(partitions);
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
            Map<String, Long> last = new HashMap<>();
            for (TopicPartition partition : partitions) {
                if (end.get(partition) > first.get(partition)) {
                    last.put(ControlDeserializer.split(partition.topic(), partition.partition()), end.get(partition) - 1);
                }
            }
            return last;
        }
    }

    private static Configuration faultTolerance(ParameterTool params) {
        Configuration config = new Configuration();
        if (!params.has("checkpoint-ms")) {
            for (String option : new String[] {"checkpoint-mode", "checkpoint-dir"}) {
                if (params.has(option)) {
                    throw new IllegalArgumentException("--" + option + " needs --checkpoint-ms");
                }
            }
            return config;
        }
        if (!"kafka".equals(params.get("source", "file"))) {
            throw new IllegalArgumentException("--checkpoint-ms needs --source kafka");
        }
        if (!params.has("bounded") && params.has("subscriptions-topic")) {
            throw new IllegalArgumentException("--checkpoint-ms with --subscriptions-topic needs --bounded: "
                    + "ControlCatchUp is not part of the checkpoint and would hold event time back after a restore");
        }
        config.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMillis(params.getLong("checkpoint-ms")));
        config.set(CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE,
                switch (params.get("checkpoint-mode", "exactly-once")) {
                    case "exactly-once" -> CheckpointingMode.EXACTLY_ONCE;
                    case "at-least-once" -> CheckpointingMode.AT_LEAST_ONCE;
                    default -> throw new IllegalArgumentException("--checkpoint-mode must be exactly-once or at-least-once");
                });
        if (params.has("checkpoint-dir")) {
            config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
            config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, params.get("checkpoint-dir"));
        }
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, params.getInt("restart-attempts", 10));
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY,
                Duration.ofMillis(params.getLong("restart-delay-ms", 10_000L)));
        return config;
    }

    private static DeliveryGuarantee deliveryGuarantee(ParameterTool params) {
        DeliveryGuarantee delivery = switch (params.get("delivery", "none")) {
            case "none" -> DeliveryGuarantee.NONE;
            case "at-least-once" -> DeliveryGuarantee.AT_LEAST_ONCE;
            case "exactly-once" -> DeliveryGuarantee.EXACTLY_ONCE;
            default -> throw new IllegalArgumentException("--delivery must be none, at-least-once or exactly-once");
        };
        if (delivery == DeliveryGuarantee.EXACTLY_ONCE && !params.has("checkpoint-ms")) {
            throw new IllegalArgumentException("--delivery exactly-once needs --checkpoint-ms");
        }
        return delivery;
    }

    private static void executeTimed(StreamExecutionEnvironment env, String jobName) throws Exception {
        long started = System.nanoTime();
        JobExecutionResult result = env.execute(jobName);
        if (result.isJobExecutionResult()) {
            System.out.printf("seconds=%.3f job_runtime_ms=%d%n",
                    (System.nanoTime() - started) / 1e9, result.getNetRuntime());
        }
    }

    private static KafkaSink<String> kafkaSink(String bootstrap, String topic, DeliveryGuarantee delivery,
                                               String transactionalIdPrefix, long transactionTimeoutMs) {
        KafkaSinkBuilder<String> builder = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setDeliveryGuarantee(delivery)
                .setRecordSerializer((KafkaRecordSerializationSchema<String>) (line, context, eventTs) ->
                        new ProducerRecord<>(topic, symbolKey(line), line.getBytes(StandardCharsets.UTF_8)));
        if (delivery == DeliveryGuarantee.EXACTLY_ONCE) {
            builder.setTransactionalIdPrefix(transactionalIdPrefix + "-" + topic)
                    .setProperty("transaction.timeout.ms", String.valueOf(transactionTimeoutMs));
        }
        return builder.build();
    }

    private static byte[] symbolKey(String csvLine) {
        return csvLine.substring(0, csvLine.indexOf(',')).getBytes(StandardCharsets.UTF_8);
    }

    private static Set<String> loadSubscriptions(String spec) throws IOException {
        if (spec.equals("all")) {
            return null;
        }
        try (Stream<String> lines = Files.lines(Paths.get(spec))) {
            return lines.map(String::strip)
                    .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                    .collect(Collectors.toCollection(HashSet::new));
        }
    }
}
