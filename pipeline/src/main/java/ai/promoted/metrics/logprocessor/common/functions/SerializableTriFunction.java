package ai.promoted.metrics.logprocessor.common.functions;

import java.io.Serializable;

public interface SerializableTriFunction<A, B, C, R> extends Serializable {
  R apply(A a, B b, C c);
}
