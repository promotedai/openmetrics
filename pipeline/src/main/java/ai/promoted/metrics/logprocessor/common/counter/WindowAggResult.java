package ai.promoted.metrics.logprocessor.common.counter;

import java.time.temporal.ChronoUnit;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

public class WindowAggResult<KEY> extends Tuple5<KEY, Integer, ChronoUnit, Long, Integer> {

  public WindowAggResult() {}

  public WindowAggResult(
      KEY key, Integer windowSize, ChronoUnit windowUnit, Long count, Integer expiry) {
    super(key, windowSize, windowUnit, count, expiry);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static <T> TypeInformation<WindowAggResult<T>> getTypeInformation(
      TypeInformation<T> keyTypeInfo) {
    return (TypeInformation)
        new TupleTypeInfo<>(
            WindowAggResult.class,
            keyTypeInfo,
            Types.INT,
            TypeInformation.of(ChronoUnit.class),
            Types.LONG,
            Types.INT);
  }

  public KEY getKey() {
    return f0;
  }

  public Integer getWindowSize() {
    return f1;
  }

  public ChronoUnit getWindowUnit() {
    return f2;
  }

  public Long getCount() {
    return f3;
  }

  public Integer getTtl() {
    return f4;
  }
}
