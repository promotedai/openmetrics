package ai.promoted.metrics.logprocessor.common.counter;

import org.apache.flink.api.java.tuple.Tuple;

public interface RedisHashKeySupplier {
  Tuple toRedisHashKey();
}
