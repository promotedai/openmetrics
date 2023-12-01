package ai.promoted.metrics.logprocessor.common.counter;

import org.apache.flink.api.java.tuple.Tuple;

public interface UserEventRedisHashSupplier extends RedisHashKeySupplier {
  Tuple toCount90DayRedisHashField();

  Tuple toLastTimestampRedisHashField();
}
