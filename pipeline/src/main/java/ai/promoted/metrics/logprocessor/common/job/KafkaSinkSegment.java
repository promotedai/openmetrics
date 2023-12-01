package ai.promoted.metrics.logprocessor.common.job;

import ai.promoted.metrics.logprocessor.common.functions.SortWithWatermark;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableFunction;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableToLongFunction;
import ai.promoted.metrics.logprocessor.common.records.flink.PlatformKeyFlinkSerializationSchema;
import ai.promoted.metrics.logprocessor.common.records.flink.ProtoKafkaFlinkValueSerializationSchema;
import ai.promoted.proto.common.UserInfo;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.GeneratedMessageV3;
import java.util.Properties;
import java.util.Set;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.formats.avro.AvroSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerConfig;
import picocli.CommandLine;
import picocli.CommandLine.Option;

/** Common functions for Kafka sinks. */
public class KafkaSinkSegment implements FlinkSegment {
  private final BaseFlinkJob job;
  private final KafkaSegment kafkaSegment;

  @Option(
      names = {"--kafkaCompressionType"},
      defaultValue = "snappy",
      description =
          "The Kafka producer `compression.type` value.  Examples: none, gzip, snappy, lz4 or zstd.")
  public String kafkaCompressionType = "snappy";

  @FeatureFlag
  @CommandLine.Option(
      names = {"--sortBeforeSink"},
      description = "Sorts the events before writing them to Kafka. Default=false")
  public boolean sortBeforeSink = false;

  @FeatureFlag
  @CommandLine.Option(
      names = {"--sideOutputLateEvents"},
      description =
          "Side outputs the late events instead of writing them to Kafka. Only works when --sortBeforeSink is enabled. Default=false")
  public boolean sideOutputLateEvents = false;

  public KafkaSinkSegment(BaseFlinkJob job, KafkaSegment kafkaSegment) {
    this.job = job;
    this.kafkaSegment = kafkaSegment;
  }

  /**
   * Returns a Function that returns the keyId part of the Kafka key (usually {@code (platformId,
   * keyId)}.
   */
  public static <T> SerializableFunction<T, String> toKeyId(
      SerializableFunction<T, UserInfo> getUserInfo, SerializableFunction<T, String> getEventId) {
    return (t) -> {
      UserInfo userInfo = getUserInfo.apply(t);
      if (!userInfo.getAnonUserId().isEmpty()) {
        return userInfo.getAnonUserId();
      } else if (!userInfo.getRetainedUserId().isEmpty()) {
        return userInfo.getRetainedUserId();
      } else {
        return getEventId.apply(t);
      }
    };
  }

  @Override
  public void validateArgs() {}

  @Override
  public Set<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    return ImmutableSet.of();
  }

  // If we want to try exactly once, you need to change our kafka consumer (source) properties
  // as well to honor only reading committed messages.
  // TODO: look at https://stackoverflow.com/a/59740301 for EXACTLY_ONCE consumption params
  public <T> DataStreamSink<T> sinkTo(DataStream<T> stream, String topic, KafkaSink<T> sink) {
    String uid = String.format("sink-kafka-%s", topic);
    return job.add(stream.sinkTo(sink), uid);
  }

  public <T extends GeneratedMessageV3> StreamSinkWithLateOutput<T> sinkTo(
      DataStream<T> stream,
      String topic,
      SerializableToLongFunction<T> getPlatformId,
      SerializableFunction<T, String> getKeyId) {
    String uid = String.format("sink-kafka-%s", topic);
    if (sortBeforeSink) {
      return sortAndSinkTo(stream, topic, getPlatformId, getKeyId, uid);
    } else {
      return new StreamSinkWithLateOutput<>(
          job.add(
              stream.sinkTo(this.getProtoKafkaSinkForKeyId(topic, getPlatformId, getKeyId)), uid),
          null);
    }
  }

  /**
   * Use the same key to sort the events and then write to Kafka. No data shuffling happens between
   * the sort operator and the Kafka sink.
   */
  private <T extends GeneratedMessageV3> StreamSinkWithLateOutput<T> sortAndSinkTo(
      DataStream<T> stream,
      String topic,
      SerializableToLongFunction<T> getPlatformId,
      SerializableFunction<T, String> getKeyId,
      String sinkUid) {
    OutputTag<T> lateEventTag = new OutputTag<>("late_event", stream.getType()) {};
    SingleOutputStreamOperator<Tuple2<byte[], T>> sortedStream =
        stream
            .keyBy(
                new KeySelector<T, SortWithWatermark.ByteArrayKey>() {
                  final SerializationSchema<T> keySerializationSchema =
                      new PlatformKeyFlinkSerializationSchema<T>(getPlatformId, getKeyId);

                  @Override
                  public SortWithWatermark.ByteArrayKey getKey(T t) {
                    return new SortWithWatermark.ByteArrayKey(keySerializationSchema.serialize(t));
                  }
                })
            .process(new SortWithWatermark<>(stream.getType(), sideOutputLateEvents, lateEventTag))
            .returns(Types.TUPLE(TypeInformation.of(byte[].class), stream.getType()))
            .uid("sort-" + sinkUid)
            .name("sort-" + sinkUid)
            .setParallelism(job.getSinkParallelism(sinkUid));
    sortedStream.forward();
    return new StreamSinkWithLateOutput<>(
        job.add(sortedStream.sinkTo(this.getProtoKafkaSink(topic)), sinkUid),
        sortedStream.getSideOutput(lateEventTag));
  }

  private <T extends GeneratedMessageV3> KafkaSink<Tuple2<byte[], T>> getProtoKafkaSink(
      String topic) {

    KafkaRecordSerializationSchema<Tuple2<byte[], T>> serializationSchema =
        KafkaRecordSerializationSchema.<Tuple2<byte[], T>>builder()
            .setTopic(topic)
            .setKeySerializationSchema((SerializationSchema<Tuple2<byte[], T>>) tuple2 -> tuple2.f0)
            .setValueSerializationSchema(
                (SerializationSchema<Tuple2<byte[], T>>) tuple2 -> tuple2.f1.toByteArray())
            .build();

    return this.<Tuple2<byte[], T>>getBaseKafkaSinkBuilder()
        .setRecordSerializer(serializationSchema)
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build();
  }

  private <T extends GeneratedMessageV3> KafkaSink<T> getProtoKafkaSinkForKeyId(
      String topic,
      SerializableToLongFunction<T> getPlatformId,
      SerializableFunction<T, String> getKeyIdId) {
    return getKafkaSink(
        topic, getPlatformId, getKeyIdId, new ProtoKafkaFlinkValueSerializationSchema<>());
  }

  protected <T extends SpecificRecord> KafkaSink<T> getAvroKafkaSink(
      String topic,
      SerializableToLongFunction<T> getPlatformId,
      SerializableFunction<T, String> getLogUserId,
      Class<T> clazz) {
    SerializationSchema<T> valueSerializationSchema = AvroSerializationSchema.forSpecific(clazz);
    return getKafkaSink(topic, getPlatformId, getLogUserId, valueSerializationSchema);
  }

  protected <T> KafkaSink<T> getKafkaSink(
      String topic,
      SerializableToLongFunction<T> getPlatformId,
      SerializableFunction<T, String> getKeyId,
      SerializationSchema<T> valueSerializationSchema) {

    KafkaRecordSerializationSchema<T> serializationSchema =
        KafkaRecordSerializationSchema.<T>builder()
            .setTopic(topic)
            .setKeySerializationSchema(
                new PlatformKeyFlinkSerializationSchema<T>(getPlatformId, getKeyId))
            .setValueSerializationSchema(valueSerializationSchema)
            .build();

    return this.<T>getBaseKafkaSinkBuilder()
        .setRecordSerializer(serializationSchema)
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build();
  }

  protected <T> KafkaSinkBuilder<T> getBaseKafkaSinkBuilder() {
    Properties kafkaProps = new Properties();
    kafkaProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, kafkaCompressionType);
    if (!"".equals(kafkaSegment.kafkaSecurityProtocol)) {
      kafkaProps.setProperty("security.protocol", kafkaSegment.kafkaSecurityProtocol);
    }
    // TODO - info log properties.
    return KafkaSink.<T>builder()
        .setBootstrapServers(kafkaSegment.bootstrapServers)
        .setKafkaProducerConfig(kafkaProps)
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE);
  }
}
