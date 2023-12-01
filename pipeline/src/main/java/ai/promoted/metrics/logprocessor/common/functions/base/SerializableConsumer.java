package ai.promoted.metrics.logprocessor.common.functions.base;

import java.io.Serializable;
import java.util.function.Consumer;

@FunctionalInterface
public interface SerializableConsumer<I> extends Consumer<I>, Serializable {
  void accept(I var1);
}
