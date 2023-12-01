package ai.promoted.metrics.logprocessor.common.counter;

import java.time.temporal.ChronoUnit;
import org.apache.flink.api.java.tuple.Tuple;

public interface JoinedEventRedisHashSupplier extends RedisHashKeySupplier {
  Tuple toRedisHashField(int windowSize, ChronoUnit windowUnit);
}
