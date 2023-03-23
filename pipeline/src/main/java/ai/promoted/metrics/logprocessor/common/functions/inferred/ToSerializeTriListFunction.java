package ai.promoted.metrics.logprocessor.common.functions.inferred;

import ai.promoted.metrics.logprocessor.common.functions.SerializableTriFunction;
import com.google.common.collect.ImmutableList;
import java.util.List;

public class ToSerializeTriListFunction<A, B, C, R>
    implements SerializableTriFunction<A, B, C, List<R>> {
  private final SerializableTriFunction<A, B, C, R> delegate;

  public ToSerializeTriListFunction(SerializableTriFunction<A, B, C, R> delegate) {
    this.delegate = delegate;
  }

  @Override
  public List<R> apply(A a, B b, C c) {
    R output = delegate.apply(a, b, c);
    if (output != null) {
      return ImmutableList.of(output);
    } else {
      return ImmutableList.of();
    }
  }
}
