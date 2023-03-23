package ai.promoted.metrics.logprocessor.common.records;

import ai.promoted.metrics.logprocessor.common.functions.SerializableFunction;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.flink.api.common.serialization.SerializationSchema;

/** Used to serialize platform id and log user id of records as keys. */
public class PlatformLogUserKeySerializationSchema<T> implements SerializationSchema<T> {

  private final SerializableFunction<T, Long> getPlatformId;
  private final SerializableFunction<T, String> getLogUserId;

  public PlatformLogUserKeySerializationSchema(
      SerializableFunction<T, Long> getPlatformId, SerializableFunction<T, String> getLogUserId) {
    this.getPlatformId = getPlatformId;
    this.getLogUserId = getLogUserId;
  }

  @Override
  public byte[] serialize(T message) {
    byte[] logUserIdBytes = getLogUserId.apply(message).getBytes();
    final ByteBuffer bb = ByteBuffer.allocate(8 + logUserIdBytes.length);
    bb.order(ByteOrder.LITTLE_ENDIAN);
    bb.putLong(getPlatformId.apply(message));
    bb.put(logUserIdBytes);
    return bb.array();
  }
}
