package ai.promoted.metrics.logprocessor.common.util;

import java.nio.ByteBuffer;
import java.util.UUID;

public class UuidConverter {
  public static byte[] toByteArray(UUID uuid) {
    ByteBuffer buffer = ByteBuffer.wrap(new byte[16]);
    buffer.putLong(uuid.getMostSignificantBits());
    buffer.putLong(uuid.getLeastSignificantBits());
    return buffer.array();
  }

  public static UUID toUUID(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    long high = buffer.getLong();
    long low = buffer.getLong();
    return new UUID(high, low);
  }

  public static byte[] toByteArray(String uuid) {
    return UuidConverter.toByteArray(UUID.fromString(uuid));
  }

  public static ByteBuffer toByteBuffer(String uuid) {
    return ByteBuffer.wrap(toByteArray(uuid));
  }
}
