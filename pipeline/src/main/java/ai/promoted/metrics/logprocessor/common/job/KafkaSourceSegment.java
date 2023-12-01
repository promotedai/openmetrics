package ai.promoted.metrics.logprocessor.common.job;

import ai.promoted.metrics.logprocessor.common.functions.LocalProcAndEventBoundedOutOfOrdernessWatermarkStrategy;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableFunction;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableParseFrom;
import ai.promoted.metrics.logprocessor.common.records.ParseFromProtoDeserializer;
import ai.promoted.metrics.logprocessor.common.records.flink.ProtoFlinkDeserializationSchema;
import ai.promoted.metrics.logprocessor.common.util.StringUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.GeneratedMessageV3;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine.Option;

/** Common Kafka source functions. */
public class KafkaSourceSegment implements FlinkSegment {
  private static final Logger LOGGER = LogManager.getLogger(KafkaSourceSegment.class);
  private final BaseFlinkJob job;
  private final KafkaSegment kafkaSegment;

  @Option(
      names = {"--startFromEarliest"},
      negatable = true,
      description = "Whether to start from the earliest Kafka offset.")
  public boolean startFromEarliest = false;

  /* From Flink Kafka docs
  https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/connectors/datastream/kafka/

  Start from the specified timestamp. For each partition, the record whose timestamp is larger than or equal to
  the specified timestamp will be used as the start position. If a partition’s latest record is earlier than the
  timestamp, the partition will simply be read from the latest record. Under this mode, committed offsets in Kafka
  will be ignored and not used as starting positions.
  */
  @Option(
      names = {"--startFromTimestamp"},
      defaultValue = "0",
      description = "Start from a specific Kafka offset.")
  public long startFromTimestamp = 0;

  /**
   * @deprecated Use --endAt instead.
   */
  @Option(
      names = {"--endAtTimestamp"},
      defaultValue = "0",
      description =
          "DEBUGGING ONLY: End at a specific Kafka "
              + "offset.  Only positive values have an effect.  This option is not validated, and can cause sources "
              + "seemingly not produce any messages.")
  @Deprecated
  public long endAtTimestamp = 0;

  @Option(
      names = {"--endAt"},
      description =
          "Set an end offsets initializer for Kafka consumer so that the Kafka source becomes bounded."
              + " Valid values are EARLIEST, LATEST, COMMITTED_OFFSETS, TIMESTAMP:timestamp_epoch_millis."
              + "Will overwrite endAtTimestamp. Default to empty string.")
  public String endAt = "";

  @Option(
      names = {"--maxPollRecords"},
      defaultValue = "500",
      description = "Max number of Kafka records to poll in" + " a single call.")
  public String maxPollRecords = "500";

  @Option(
      names = {"--kafkaSourceUidPrefix"},
      defaultValue = "source-kafka-",
      description = "The prefix string for the Kafka source uid.  Used to override a state offset.")
  public String kafkaSourceUidPrefix = "source-kafka-";

  @Option(
      names = {"--partitionDiscoveryInterval"},
      defaultValue = "PT30S",
      description = "Interval for discovering new partitions. Default=30S.")
  public Duration partitionDiscoveryInterval = Duration.parse("PT30S");

  @Option(
      names = {"--watermarkIdlenessForTopic"},
      description = "A topic specific version of --watermarkIdleness.  Default=empty")
  public Map<String, Duration> watermarkIdlenessForTopic = new HashMap<>();

  @Option(
      names = {"--watermarkIdleness"},
      description = "Idleness threshold in seconds for WatermarkGenerator. Default=null")
  public Duration watermarkIdleness = null;

  @Option(
      names = {"--watermarkAlignment"},
      negatable = true,
      description = "Used to align all source watermarks.  Defaults to off")
  public boolean watermarkAlignment = false;

  @Option(
      names = {"--watermarkAlignmentMaxAllowedWatermarkDrift"},
      defaultValue = "PT5M",
      description = "The max allowed watermark drift.  Default=PT5M")
  public Duration watermarkAlignmentMaxAllowedWatermarkDrift = Duration.parse("PT5M");

  @Option(
      names = {"--watermarkAlignmentUpdateInterval"},
      defaultValue = "PT1S",
      description =
          "How frequently to update the watermark alignments.  Default=PT1S.  The Flink default is 1s")
  public Duration watermarkAlignmentUpdateInterval = Duration.parse("PT1S");

  public KafkaSourceSegment(BaseFlinkJob job, KafkaSegment kafkaSegment) {
    this.job = job;
    this.kafkaSegment = kafkaSegment;
  }

  /** Returns a Protobuf deserialization schema. */
  protected static <T extends GeneratedMessageV3>
      ProtoFlinkDeserializationSchema<T> getProtoDeserializationSchema(
          Class<T> clazz, SerializableParseFrom<T> parseFrom) {
    return new ProtoFlinkDeserializationSchema<T>(
        clazz, new ParseFromProtoDeserializer<T>(parseFrom));
  }

  @Override
  public void validateArgs() {
    if (startFromEarliest) {
      Preconditions.checkArgument(
          startFromTimestamp == 0, "Do not set both startFromEarliest and startFromTimestamp");
    }
    if (null != watermarkIdleness) {
      Preconditions.checkArgument(
          !(watermarkIdleness.isNegative() || watermarkIdleness.isZero()),
          "Watermark idleness must be positive");
    }
    watermarkIdlenessForTopic.forEach(
        (k, v) ->
            Preconditions.checkArgument(
                !(v.isNegative() || v.isZero()),
                "Watermark idleness should be positive for topic %s",
                k));
    if (!StringUtil.isBlank(endAt)) {
      endAt = endAt.toUpperCase().trim();
      String endAtPattern = "^(EARLIEST|LATEST|COMMITTED_OFFSETS|TIMESTAMP:\\s*\\d{1,19})$";
      Pattern pattern = Pattern.compile(endAtPattern);
      if (!pattern.asPredicate().test(endAt)) {
        throw new IllegalArgumentException("endAt must match " + endAtPattern);
      }
    }
  }

  @Override
  public Set<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    return kafkaSegment.getProtoClasses();
  }

  public <T extends GeneratedMessageV3> SingleOutputStreamOperator<T> addProtobufSource(
      StreamExecutionEnvironment env,
      String topic,
      String kafkaGroupId,
      Class<T> clazz,
      SerializableParseFrom<T> parseFrom,
      Duration maxOutOfOrderness,
      SerializableFunction<T, Long> getTimestamp) {
    // TODO Only keep getTimestamp for `log_request` after fully switch to createTime
    return addSource(
        env,
        topic,
        kafkaGroupId,
        getProtoDeserializationSchema(clazz, parseFrom),
        getWatermarkStrategy(topic, maxOutOfOrderness, getTimestamp));
  }

  public <T extends SpecificRecord> SingleOutputStreamOperator<T> addAvroSource(
      StreamExecutionEnvironment env,
      String topic,
      String kafkaGroupId,
      Class<T> clazz,
      Duration maxOutOfOrderness,
      SerializableFunction<T, Long> getTimestamp) {
    // TODO Only keep getTimestamp for `log_request` after fully switch to createTime
    return addSource(
        env,
        topic,
        kafkaGroupId,
        AvroDeserializationSchema.forSpecific(clazz),
        getWatermarkStrategy(topic, maxOutOfOrderness, getTimestamp));
  }

  private <T> SingleOutputStreamOperator<T> addSource(
      StreamExecutionEnvironment env,
      String topic,
      String kafkaGroupId,
      DeserializationSchema<T> deserializationSchema,
      WatermarkStrategy<T> watermarkStrategy) {
    Properties properties = new Properties();
    properties.setProperty("max.poll.records", maxPollRecords);
    if (!"".equals(kafkaSegment.kafkaSecurityProtocol)) {
      properties.setProperty("security.protocol", kafkaSegment.kafkaSecurityProtocol);
    }

    if (partitionDiscoveryInterval.toMillis() > 0) {
      properties.setProperty(
          FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS,
          Long.toString(partitionDiscoveryInterval.toMillis()));
    }

    KafkaSourceBuilder<T> sourceBuilder =
        KafkaSource.<T>builder()
            .setBootstrapServers(kafkaSegment.bootstrapServers)
            .setGroupId(kafkaGroupId)
            .setTopics(topic)
            .setValueOnlyDeserializer(deserializationSchema)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setProperties(properties);

    if (endAtTimestamp > 0 && StringUtil.isBlank(endAt)) {
      LOGGER.info(
          "Set endAtTimestamp {} for topic {}. The job can run in BATCH mode!",
          endAtTimestamp,
          topic);
      sourceBuilder.setBounded(OffsetsInitializer.timestamp(endAtTimestamp));
    }

    if (!StringUtil.isBlank(endAt)) {
      sourceBuilder.setBounded(getEndAtOffsetsInitializer());
    }

    if (startFromEarliest) {
      LOGGER.info("Use offset initializer 'startFromEarliest' for topic {}", topic);
      sourceBuilder.setStartingOffsets(OffsetsInitializer.earliest());
    } else if (startFromTimestamp != 0) {
      LOGGER.info(
          "Use offset initializer with timestamp = {} for topic {}", startFromTimestamp, topic);
      sourceBuilder.setStartingOffsets(OffsetsInitializer.timestamp(startFromTimestamp));
    }
    LOGGER.info("Other properties for topic {}: {}", topic, properties);

    String uid = kafkaSourceUidPrefix + topic;
    SingleOutputStreamOperator<T> op =
        job.add(env.fromSource(sourceBuilder.build(), watermarkStrategy, uid), uid);
    if (startFromTimestamp != 0) {
      // After switching to CreateTime for Kafka topics, the source will receive records whose
      // timestamp is less than the startFromTimestamp due to our-of-order.
      // We append a filter to get rid of the old events.
      op =
          job.add(
              op.process(new StartFromTimestampFilter<>(topic, startFromTimestamp)),
              "start-from-ts-filter-" + topic);
    }
    return op;
  }

  @VisibleForTesting
  OffsetsInitializer getEndAtOffsetsInitializer() {
    switch (endAt) {
      case "EARLIEST":
        return OffsetsInitializer.earliest();
      case "LATEST":
        return OffsetsInitializer.latest();
      case "COMMITTED_OFFSETS":
        return OffsetsInitializer.committedOffsets();
      default:
        if (endAt.startsWith("TIMESTAMP:")) {
          return OffsetsInitializer.timestamp(Long.parseLong(endAt.substring(10).trim()));
        } else {
          throw new IllegalArgumentException("Unknown endAt option: " + endAt);
        }
    }
  }

  static class StartFromTimestampFilter<T> extends ProcessFunction<T, T> {
    private static final Logger LOGGER = LogManager.getLogger(StartFromTimestampFilter.class);
    private final long startFromTimestamp;

    public StartFromTimestampFilter(String topic, long startFromTimestamp) {
      this.startFromTimestamp = startFromTimestamp;
      LOGGER.info(
          "Will filter out Kafka records whose timestamp is less than {} for topic {}",
          startFromTimestamp,
          topic);
    }

    @Override
    public void processElement(T t, ProcessFunction<T, T>.Context context, Collector<T> collector)
        throws Exception {

      if (context.timestamp() >= startFromTimestamp) {
        collector.collect(t);
      }
    }
  }

  // TODO Remove this method since it's not used.
  public OffsetsInitializer getOffsetsInitializer(long startFromTimestampOverride) {
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
  @VisibleForTesting
  <T> WatermarkStrategy<T> getWatermarkStrategy(
      String topic,
      Duration maxOutOfOrderness,
      @Nullable SerializableFunction<T, Long> getTimestamp) {
    WatermarkStrategy<T> watermarkStrategy;
    Duration wmIdleness =
        ObjectUtils.firstNonNull(watermarkIdlenessForTopic.get(topic), watermarkIdleness);
    if (null != wmIdleness) {
      LOGGER.info("Set watermark idleness {} for topic {}", wmIdleness, topic);
      watermarkStrategy =
          new LocalProcAndEventBoundedOutOfOrdernessWatermarkStrategy<>(
              maxOutOfOrderness,
              watermarkIdleness,
              // Just force the same delay for now.
              watermarkIdleness);
    } else {
      watermarkStrategy = WatermarkStrategy.forBoundedOutOfOrderness(maxOutOfOrderness);
    }
    if (watermarkAlignment) {
      LOGGER.info(
          "Enable watermark alignment with allowed max drift {} and update interval {} for topic {}",
          watermarkAlignmentMaxAllowedWatermarkDrift,
          watermarkAlignmentUpdateInterval,
          topic);
      watermarkStrategy =
          watermarkStrategy.withWatermarkAlignment(
              "alignment-group",
              watermarkAlignmentMaxAllowedWatermarkDrift,
              watermarkAlignmentUpdateInterval);
    }
    if (null == getTimestamp) {
      LOGGER.info("Use default timestamp assigner for topic {}", topic);
      return watermarkStrategy;
    }
    LOGGER.info("Use provided timestamp assigner for topic {}", topic);
    return watermarkStrategy.withTimestampAssigner(
        (message, unused) -> getTimestamp.apply(message));
  }
}
