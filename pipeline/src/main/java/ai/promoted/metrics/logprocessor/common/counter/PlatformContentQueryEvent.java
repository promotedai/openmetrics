package ai.promoted.metrics.logprocessor.common.counter;

import static ai.promoted.metrics.logprocessor.common.counter.Constants.QUERY_TYPE;

import ai.promoted.proto.delivery.internal.features.AggMetric;
import java.time.temporal.ChronoUnit;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple4;

/** platformId, queryHash, contentId , aggMetric */
public class PlatformContentQueryEvent extends Tuple4<Long, Long, String, String>
    implements JoinedEventRedisHashSupplier {
  public static final String NAME = "plat-cont-qry_$evt";
  public static final String DEPRECATED_NAME = "content-query";

  public PlatformContentQueryEvent() {}

  public PlatformContentQueryEvent(
      Long platformId, Long queryHash, String contentId, String countAggValue) {
    super(platformId, queryHash, contentId, countAggValue);
  }

  public Long getPlatformId() {
    return f0;
  }

  public Long getQueryHash() {
    return f1;
  }

  public String getContentId() {
    return f2;
  }

  public String getCountAggValue() {
    return f3;
  }

  /** (platformId, contentId, QUERY_TYPE, queryHash) */
  @Override
  public Tuple toRedisHashKey() {
    return Tuple4.of(getPlatformId(), getContentId(), QUERY_TYPE, Long.toHexString(getQueryHash()));
  }

  @Override
  public Tuple toRedisHashField(int windowSize, ChronoUnit windowUnit) {
    return Tuple1.of(
        FeatureId.itemQueryCount(AggMetric.valueOf(getCountAggValue()), windowSize, windowUnit));
  }
}
