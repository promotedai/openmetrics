package ai.promoted.metrics.logprocessor.common.functions;

import org.apache.flink.api.common.functions.AggregateFunction;

/** Simple counter that counts non-Long T's. */
public class Counter<T> implements AggregateFunction<T, Long, Long> {
  @Override
  public Long createAccumulator() {
    return 0L;
  }

  @Override
  public Long add(T in, Long accumulator) {
    if (in instanceof Long) {
      return accumulator + (Long) in;
    } else {
      return accumulator + 1;
    }
  }

  @Override
  public Long merge(Long a, Long b) {
    return a + b;
  }

  @Override
  public Long getResult(Long accumulator) {
    return accumulator;
  }
}
