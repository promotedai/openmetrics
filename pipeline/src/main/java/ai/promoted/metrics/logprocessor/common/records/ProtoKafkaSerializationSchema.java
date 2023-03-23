package ai.promoted.metrics.logprocessor.common.records;

import ai.promoted.metrics.logprocessor.common.functions.SerializableFunction;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Serializes Protobufs in a record format that supports Kafka's Java client. This is used in fake
 * data generator. Flink uses the key and value serialization schemas directly.
 */
public class ProtoKafkaSerializationSchema<T extends GeneratedMessageV3> {
  private final String topic;
  private final PlatformLogUserKeySerializationSchema<T> keySerializationSchema;
  private final ProtoKafkaValueSerializationSchema<T> valueSerializationSchema;

  public ProtoKafkaSerializationSchema(
      String topic,
      SerializableFunction<T, Long> getPlatformId,
      SerializableFunction<T, String> getLogUserId) {
    this(topic, new PlatformLogUserKeySerializationSchema<T>(getPlatformId, getLogUserId));
  }

  public ProtoKafkaSerializationSchema(
      String topic, PlatformLogUserKeySerializationSchema<T> keySerializationSchema) {
    this.topic = topic;
    this.keySerializationSchema = keySerializationSchema;
    this.valueSerializationSchema = new ProtoKafkaValueSerializationSchema<>();
  }

  public ProducerRecord<byte[], byte[]> serialize(T message) {
    return new ProducerRecord<byte[], byte[]>(
        topic,
        keySerializationSchema.serialize(message),
        valueSerializationSchema.serialize(message));
  }
}
