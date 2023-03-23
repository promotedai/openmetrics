package ai.promoted.metrics.logprocessor.common.counter;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

public class LastTimeAggResult<KEY> extends Tuple4<KEY, Long, Long, Integer> {

  public LastTimeAggResult() {}

  public LastTimeAggResult(KEY key, Long timestamp, Long count, Integer expiry) {
    super(key, timestamp, count, expiry);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static <T> TypeInformation<LastTimeAggResult<T>> getTypeInformation(
      TypeInformation<T> keyTypeInfo) {
    return (TypeInformation)
        new TupleTypeInfo<>(
            LastTimeAggResult.class, keyTypeInfo, Types.LONG, Types.LONG, Types.INT);
  }

  public KEY getKey() {
    return f0;
  }

  /**
   * @return last timestamp millis
   */
  public Long getTimestamp() {
    return f1;
  }

  /**
   * @return 90 day count
   */
  public Long getCount() {
    return f2;
  }

  /**
   * @return expiry
   */
  public Integer getTtl() {
    return f3;
  }
}
