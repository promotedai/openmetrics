package ai.promoted.metrics.logprocessor.common.records;

import ai.promoted.metrics.logprocessor.common.functions.base.SerializableFunction;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableToLongFunction;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Used to serialize Kafka keys using {@code (platform id, key id)} from records. No Flink
 * dependency.
 */
public class PlatformKeySerializationSchema<T> implements Serializable {

  private final SerializableToLongFunction<T> getPlatformId;
  private final SerializableFunction<T, String> getKeyId;

  public PlatformKeySerializationSchema(
      SerializableToLongFunction<T> getPlatformId, SerializableFunction<T, String> getKeyId) {
    this.getPlatformId = getPlatformId;
    this.getKeyId = getKeyId;
  }

  public byte[] serialize(T message) {
    byte[] keyIdBytes = getKeyId.apply(message).getBytes();
    final ByteBuffer bb = ByteBuffer.allocate(8 + keyIdBytes.length);
    bb.order(ByteOrder.LITTLE_ENDIAN);
    bb.putLong(getPlatformId.applyAsLong(message));
    bb.put(keyIdBytes);
    return bb.array();
  }
}
