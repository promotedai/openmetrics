package ai.promoted.metrics.logprocessor.common.job;

import ai.promoted.metrics.logprocessor.common.constant.Constants;
import ai.promoted.metrics.logprocessor.common.functions.LocalProcAndEventBoundedOutOfOrdernessWatermarkStrategy;
import ai.promoted.metrics.logprocessor.common.functions.SerializableFunction;
import ai.promoted.metrics.logprocessor.common.functions.SerializableParseFrom;
import ai.promoted.metrics.logprocessor.common.records.ParseFromProtoDeserializer;
import ai.promoted.metrics.logprocessor.common.records.ProtoDeserializationSchema;
import ai.promoted.metrics.logprocessor.common.records.PlatformLogUserKeySerializationSchema;
import ai.promoted.metrics.logprocessor.common.records.ProtoKafkaValueSerializationSchema;
import ai.promoted.proto.event.FlatResponseInsertion;
import ai.promoted.proto.event.JoinedEvent;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.formats.avro.AvroSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.promoted.metrics.logprocessor.common.util.StringUtil.firstNotEmpty;
import static ai.promoted.metrics.logprocessor.common.util.StringUtil.isBlank;

// TODO: https://github.com/remkop/picocli/issues/1527
public class KafkaSegment implements FlinkSegment {
    private static final Logger LOGGER = LogManager.getLogger(KafkaSegment.class);

    public static final String MAX_POLL_RECORDS_KEY = "max.poll.records";

    @CommandLine.Option(names = {"--kafkaDataset"}, defaultValue = Constants.DEFAULT_KAFKA_DATASET,
            description = "The middle part of the Kafka topic name. Default=default")
    public String kafkaDataset = Constants.DEFAULT_KAFKA_DATASET;

    @Option(names = {"-b", "--bootstrap.servers"}, defaultValue = Constants.DEFAULT_BOOTSTRAP_SERVERS,
            description = "Kafka bootstrap servers.")
    public String bootstrapServers = Constants.DEFAULT_BOOTSTRAP_SERVERS;

    // Prod value = "SSL"
    @Option(names = {"--kafkaSecurityProtocol"}, defaultValue = "",
            description = "Kafka Source Security Protocol.")
    public String kafkaSecurityProtocol = "";

    @Option(names = {"--startFromEarliest"}, negatable = true,
            description = "Whether to start from the earliest Kafka offset.")
    public boolean startFromEarliest = false;

    /* From Flink Kafka docs
    https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/connectors/datastream/kafka/

    Start from the specified timestamp. For each partition, the record whose timestamp is larger than or equal to
    the specified timestamp will be used as the start position. If a partition’s latest record is earlier than the
    timestamp, the partition will simply be read from the latest record. Under this mode, committed offsets in Kafka
    will be ignored and not used as starting positions.
    */
    @Option(names = {"--startFromTimestamp"}, defaultValue = "0", description = "Start from a specific Kafka offset.")
    public long startFromTimestamp = 0;

    @Option(names = {"--endAtTimestamp"}, defaultValue = "0", description = "DEBUGGING ONLY: End at a specific Kafka " +
            "offset.  Only positive values have an effect.  This option is not validated, and can cause sources " +
            "seemingly not produce any messages.")
    public long endAtTimestamp = 0;

    @Option(names = {"--kafkaCompressionType"}, defaultValue = "gzip",
            description = "The Kafka producer `compression.type` value.  Examples: none, gzip, snapper, lz4 or zstd.")
    public String kafkaCompressionType = "gzip";

    @Option(names = {"--maxPollRecords"}, defaultValue = "500", description = "Max number of Kafka records to poll in" +
            " a single call.")
    public String maxPollRecords = "500";

    @Option(names = {"--partitionDiscoveryInterval"}, defaultValue = "PT30S", description = "Interval for discovering" +
            " new partitions. Default=30S.")
    public Duration partitionDiscoveryInterval = Duration.parse("PT30S");

    // Watermark generator

    @Option(names = {"--procAndEventWatermarkGenerator"}, negatable = true, description = "Whether to use a " +
            "WatermarkGenerator that supports both proc and event times.  Meant for local development.  This option " +
            "gets ORed with specific-topic versions of this flag.  Default=false")
    public boolean procAndEventWatermarkGenerator = false;

    @Option(names = {"--watermarkIdleness"}, defaultValue = "PT1M", description = "Idleness threshold for " +
            "WatermarkGenerator. Default=PT1M")
    public Duration watermarkIdleness = Duration.parse("PT1M");

    @Option(names = {"--watermarkIdlenessProcessingTimeTrailingDuration"}, defaultValue = "PT15S", description =
            "processingTimeTrailingDuration when using the idle watermark generator.  The value is pretty arbitrary " +
                    "right now.  It might catch small Kafka connection issues.  Default=PT15S")
    public Duration watermarkIdlenessProcessingTimeTrailingDuration = Duration.parse("PT15S");

    private final BaseFlinkJob job;

    public KafkaSegment(BaseFlinkJob job) {
        this.job = job;
    }

    @Override
    public void validateArgs() {
        if (startFromEarliest) {
            Preconditions.checkArgument(
                    startFromTimestamp == 0,
                    "Do not set both startFromEarliest and startFromTimestamp");
        }
    }

    @Override
    public List<Class<? extends GeneratedMessageV3>> getProtoClasses() {
        return ImmutableList.of(
                FlatResponseInsertion.class,
                JoinedEvent.class);
    }

    /**
     * Returns a Kafka topic name from the given topic format and dataset specification(s).
     * Generally, if this is for an input source, don't set any specs to get the default/production
     * topic.  For output sinks, you should set something for non-production job runs.
     * <p>
     * Kafka topic naming convention - see the comment in Constants.
     *
     * @param messageGroup first part of the kafka topic name.
     * @param datasets     the middle part of the kafka topic.
     * @param dataName     the last part of the kafka topic name.
     */
    public String getKafkaTopic(String messageGroup, String datasets, String dataName) {
        return messageGroup + "." + datasets + "." + dataName;
    }

    public String formatDatasets(String... datasets) {
        return Stream.of(datasets).filter(s -> !isBlank(s)).collect(Collectors.joining("."));
    }

    protected <T extends GeneratedMessageV3> SingleOutputStreamOperator<T> addProtobufSource(
            StreamExecutionEnvironment env,
            String topic,
            String kafkaGroupId,
            Class<T> clazz,
            SerializableParseFrom<T> parseFrom,
            Duration maxOutOfOrderness,
            SerializableFunction<T, Long> getTimestamp) {
        return addSource(
                env,
                topic,
                kafkaGroupId,
                getProtoDeserializationSchema(clazz, parseFrom),
                getWatermarkStrategy(this.procAndEventWatermarkGenerator, maxOutOfOrderness, getTimestamp));
    }

    protected <T extends SpecificRecord> SingleOutputStreamOperator<T> addAvroSource(
            StreamExecutionEnvironment env,
            String topic,
            String kafkaGroupId,
            Class<T> clazz,
            Duration maxOutOfOrderness,
            SerializableFunction<T, Long> getTimestamp) {
        return addSource(
                env,
                topic,
                kafkaGroupId,
                AvroDeserializationSchema.forSpecific(clazz),
                getWatermarkStrategy(this.procAndEventWatermarkGenerator, maxOutOfOrderness, getTimestamp));
    }

    private <T> SingleOutputStreamOperator<T> addSource(
            StreamExecutionEnvironment env,
            String topic,
            String kafkaGroupId,
            DeserializationSchema<T> deserializationSchema,
            WatermarkStrategy<T> watermarkStrategy) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, kafkaGroupId);
        properties.setProperty("max.poll.records", maxPollRecords);
        if (!"".equals(kafkaSecurityProtocol)) {
            properties.setProperty("security.protocol", kafkaSecurityProtocol);
        }

        if (partitionDiscoveryInterval.toMillis() > 0) {
            properties.setProperty(
                    FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS,
                    Long.toString(partitionDiscoveryInterval.toMillis()));
        }

        // We use the old-style kafka source/consumer pattern due to https://issues.apache.org/jira/browse/FLINK-26018
        // TODO: remove after 1.14.5
        @SuppressWarnings("deprecation")
        FlinkKafkaConsumer<T> consumer = new FlinkKafkaConsumer<>(
                topic, deserializationSchema, properties);
        if (startFromEarliest) {
            consumer.setStartFromEarliest();
        } else if (startFromTimestamp != 0) {
            consumer.setStartFromTimestamp(startFromTimestamp);
        }

        String uid = "source-kafka-" + topic;
        SingleOutputStreamOperator<T> src =
                // WARNING - FlinkKafkaConsumer.assignTimestampsAndWatermarks must be called
                // directly to get Kafka per-partition watermarks.
                job.add(env.addSource(consumer.assignTimestampsAndWatermarks(watermarkStrategy)), uid);

        // This is for debugging scenarios where we want to stop processing new input to avoid extra output.
        if (endAtTimestamp > 0) {
            final long endTimestamp = endAtTimestamp;
            src = job.add(
                    src.process(new ProcessFunction<T, T>() {
                        @Override
                        public void processElement(T in, Context ctx, Collector<T> out) {
                            if (ctx.timestamp() < endTimestamp) out.collect(in);
                        }
                    }),
                    "end-" + uid);
        }
        return src;
    }

    // If we want to try exactly once, you need to change our kafka consumer (source) properties
    // as well to honor only reading committed messages.
    // TODO: look at https://stackoverflow.com/a/59740301 for EXACTLY_ONCE consumption params
    protected <T> DataStreamSink<T> sinkTo(
            DataStream<T> stream, String topic, KafkaSink<T> sink) {
        String uid = String.format("sink-kafka-%s", topic);
        return job.add(stream.sinkTo(sink), uid);
    }

    /**
     * Returns the maxPollRecords settings (string) value.
     * It will be either the given override or defaults to --maxPollRecords.
     */
    protected String getMaxPollRecords(String override) {
        return firstNotEmpty(override, maxPollRecords);
    }

    protected <T> KafkaSourceBuilder<T> getBaseKafkaSourceBuilder() {
        return getBaseKafkaSourceBuilder(getOffsetsInitializer());
    }

    protected <T> KafkaSourceBuilder<T> getBaseKafkaSourceBuilder(OffsetsInitializer offsetsInitializer) {
        KafkaSourceBuilder<T> builder = KafkaSource.<T>builder()
                .setBootstrapServers(bootstrapServers)
                .setProperty(MAX_POLL_RECORDS_KEY, maxPollRecords)
                .setStartingOffsets(offsetsInitializer);
        if (!"".equals(kafkaSecurityProtocol)) {
            builder.setProperty("security.protocol", kafkaSecurityProtocol);
        }
        if (partitionDiscoveryInterval.toMillis() > 0L) {
            builder.setProperty(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS,
                    Long.toString(partitionDiscoveryInterval.toMillis()));
        }
        if (offsetsInitializer.getAutoOffsetResetStrategy() == OffsetResetStrategy.NONE) {
            // If we are using "startFromTimestamp" and the timestamp does not exist in Kafka, use the earliest offset.
            builder.setProperty("auto.offset.reset", "earliest");
        }
        return builder;
    }

    protected <T extends GeneratedMessageV3> KafkaSink<T> getProtoKafkaSink(
            String topic,
            SerializableFunction<T, Long> getPlatformId,
            SerializableFunction<T, String> getLogUserId) {
        return getKafkaSink(topic, getPlatformId, getLogUserId, new ProtoKafkaValueSerializationSchema<>());
    }

    protected <T extends SpecificRecord> KafkaSink<T> getAvroKafkaSink(
            String topic,
            SerializableFunction<T, Long> getPlatformId,
            SerializableFunction<T, String> getLogUserId,
            Class<T> clazz) {
        SerializationSchema<T> valueSerializationSchema = AvroSerializationSchema.forSpecific(clazz);
        return getKafkaSink(topic, getPlatformId, getLogUserId, valueSerializationSchema);
    }

    protected <T> KafkaSink<T> getKafkaSink(
            String topic,
            SerializableFunction<T, Long> getPlatformId,
            SerializableFunction<T, String> getLogUserId,
            SerializationSchema<T> valueSerializationSchema) {

        KafkaRecordSerializationSchema<T> serializationSchema = KafkaRecordSerializationSchema.<T>builder()
                .setTopic(topic)
                .setKeySerializationSchema(new PlatformLogUserKeySerializationSchema<>(getPlatformId, getLogUserId))
                .setValueSerializationSchema(valueSerializationSchema)
                .build();

        return this.<T>getBaseKafkaSinkBuilder()
                .setRecordSerializer(serializationSchema)
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    protected <T> KafkaSinkBuilder<T> getBaseKafkaSinkBuilder() {
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, kafkaCompressionType);
        if (!"".equals(kafkaSecurityProtocol)) {
            kafkaProps.setProperty("security.protocol", kafkaSecurityProtocol);
        }
        // TODO - info log properties.
        return KafkaSink.<T>builder()
                .setBootstrapServers(bootstrapServers)
                .setKafkaProducerConfig(kafkaProps)
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE);
    }

    @VisibleForTesting
    OffsetsInitializer getOffsetsInitializer() {
        return getOffsetsInitializer(0L);
    }

    @VisibleForTesting
    OffsetsInitializer getOffsetsInitializer(long startFromTimestampOverride) {
        if (startFromEarliest) {
            Preconditions.checkArgument(
                    startFromTimestampOverride == 0,
                    "Do not set both startFromEarliest and startFromTimestamp override");
            return OffsetsInitializer.earliest();
        } else if (startFromTimestampOverride != 0) {
            return OffsetsInitializer.timestamp(startFromTimestampOverride);
        } else if (startFromTimestamp != 0) {
            return OffsetsInitializer.timestamp(startFromTimestamp);
        } else {
            // The default value if 'earliest' so we need to override it.
            // TODO - should this be committed offset or latest?
            return OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST);
        }
    }

    // Override the timestamp assigner of our "standard" watermark strategy to extract timestamps
    // from the message itself w/in Flink instead of kafka insertion time.
    //
    // Specifically, we use a flat message's log timestamp to account for backfills of kafka sources
    // where the kafka insertion timestamp will likely be much later than the flat message
    // timestamps.  When a consumer job is not backfilling, the difference between the event and
    // kafka insertion timestamps should be (relatively) very small.
    private <T> WatermarkStrategy<T> getWatermarkStrategy(
            boolean useProcAndEventWatermarkGenerator,
            Duration maxOutOfOrderness,
            @Nullable SerializableFunction<T, Long> getTimestamp) {
        WatermarkStrategy<T> watermarkStrategy;
        if (useProcAndEventWatermarkGenerator) {
            watermarkStrategy = new LocalProcAndEventBoundedOutOfOrdernessWatermarkStrategy<T>(
                    maxOutOfOrderness, watermarkIdleness, watermarkIdlenessProcessingTimeTrailingDuration);
        } else {
            watermarkStrategy = WatermarkStrategy.forBoundedOutOfOrderness(maxOutOfOrderness);
        }
        if (null == getTimestamp) {
            return watermarkStrategy;
        }
        return watermarkStrategy.withTimestampAssigner(
                (SerializableTimestampAssigner<T>) (message, unused) -> getTimestamp.apply(message));
    }

    /**
     * Returns a Protobuf deserialization schema.
     */
    protected static <T extends GeneratedMessageV3> ProtoDeserializationSchema<T> getProtoDeserializationSchema(
            Class<T> clazz, SerializableParseFrom<T> parseFrom) {
        return new ProtoDeserializationSchema<T>(clazz, new ParseFromProtoDeserializer<T>(parseFrom));
    }

    protected static <T extends GeneratedMessageV3> KafkaRecordDeserializationSchema<T> deserializationSchema(
            Class<T> clazz, SerializableParseFrom<T> parseFrom) {
        return KafkaRecordDeserializationSchema.valueOnly(getProtoDeserializationSchema(clazz, parseFrom));
    }
}
