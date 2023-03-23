package ai.promoted.metrics.logprocessor.common.fakedatagenerator;

import ai.promoted.metrics.logprocessor.common.util.UuidConverter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * A Supplier that returns UUIDs that increment after each get. This should only be used for
 * testing.
 */
public class IncrementingUUIDSupplier implements Supplier<String> {
  private final byte[] baseUuidBytes;
  private final Supplier<Long> incrementingSupplier;

  public IncrementingUUIDSupplier(String uuidString) {
    this.baseUuidBytes = UuidConverter.toByteArray(uuidString);
    incrementingSupplier = new AtomicLong(0)::incrementAndGet;
  }

  @Override
  public String get() {
    byte[] copy = Arrays.copyOf(baseUuidBytes, baseUuidBytes.length);
    ByteBuffer buffer = ByteBuffer.wrap(copy);
    buffer.putLong(8, buffer.getLong(8) + incrementingSupplier.get());
    return UuidConverter.toUUID(buffer.array()).toString();
  }
}
