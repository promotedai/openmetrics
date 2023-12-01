package ai.promoted.metrics.logprocessor.common.records.flink;

import ai.promoted.metrics.logprocessor.common.functions.base.SerializableFunction;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableToLongFunction;
import ai.promoted.metrics.logprocessor.common.records.PlatformKeySerializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * Used to serialize Kafka keys using {@code (platform id, key id)} from records. Has Flink
 * dependency.
 */
public class PlatformKeyFlinkSerializationSchema<T> implements SerializationSchema<T> {

  private final PlatformKeySerializationSchema schema;

  public PlatformKeyFlinkSerializationSchema(
      SerializableToLongFunction<T> getPlatformId, SerializableFunction<T, String> getKeyId) {
    this.schema = new PlatformKeySerializationSchema(getPlatformId, getKeyId);
  }

  @Override
  public byte[] serialize(T message) {
    return schema.serialize(message);
  }
}
