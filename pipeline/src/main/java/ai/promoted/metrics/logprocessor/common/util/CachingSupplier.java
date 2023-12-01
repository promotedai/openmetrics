package ai.promoted.metrics.logprocessor.common.util;

import java.util.function.Supplier;

/** A {@code Supplier} where the result can be cached. */
public abstract class CachingSupplier<T> implements Supplier<T> {
  private transient T value;

  @Override
  public T get() {
    if (value == null) {
      value = supplyValue();
    }
    return value;
  }

  protected abstract T supplyValue();
}
