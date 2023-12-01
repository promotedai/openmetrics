package ai.promoted.metrics.logprocessor.common.records.flink;

import ai.promoted.metrics.logprocessor.common.functions.base.SerializableFunction;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableToLongFunction;
import ai.promoted.metrics.logprocessor.common.records.PlatformKeySerializationSchema;
import ai.promoted.metrics.logprocessor.common.records.ProtoKafkaValueSerializationSchema;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Serializes Protobufs in a record format that supports Kafka's Java client. This is used in fake
 * data generator. Flink uses the key and value serialization schemas directly.
 */
public class ProtoKafkaFlinkSerializationSchema<T extends GeneratedMessageV3> {
  private final String topic;
  private final PlatformKeySerializationSchema<T> keySerializationSchema;
  private final ProtoKafkaValueSerializationSchema<T> valueSerializationSchema;

  public ProtoKafkaFlinkSerializationSchema(
      String topic,
      SerializableToLongFunction<T> getPlatformId,
      SerializableFunction<T, String> getLogUserId) {
    this(topic, new PlatformKeySerializationSchema<T>(getPlatformId, getLogUserId));
  }

  public ProtoKafkaFlinkSerializationSchema(
      String topic, PlatformKeySerializationSchema<T> keySerializationSchema) {
    this.topic = topic;
    this.keySerializationSchema = keySerializationSchema;
    this.valueSerializationSchema = new ProtoKafkaValueSerializationSchema<>();
  }

  public ProducerRecord<byte[], byte[]> serialize(T message) {
    return new ProducerRecord<>(
        topic,
        keySerializationSchema.serialize(message),
        valueSerializationSchema.serialize(message));
  }
}
