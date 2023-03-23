package ai.promoted.metrics.logprocessor.common.counter;

import org.apache.flink.api.java.tuple.Tuple;

public interface LastUserEventRedisHashSupplier extends RedisHashKeySupplier {
  Tuple getCount90DayHashField();

  Tuple getTimestampHashField();
}
