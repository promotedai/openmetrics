package ai.promoted.metrics.logprocessor.common.counter;

import static ai.promoted.metrics.logprocessor.common.counter.Constants.QUERY_TYPE;

import ai.promoted.proto.delivery.internal.features.AggMetric;
import java.time.temporal.ChronoUnit;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;

/** platformId, queryHash, aggMetric */
public class PlatformQueryEvent extends Tuple3<Long, Long, String>
    implements JoinedEventRedisHashSupplier {
  public static final String NAME = "plat-qry_$evt";
  public static final String DEPRECATED_NAME = "query";

  public PlatformQueryEvent() {}

  public PlatformQueryEvent(Long platformId, Long queryHash, String aggMetric) {
    super(platformId, queryHash, aggMetric);
  }

  public Long getPlatformId() {
    return f0;
  }

  public Long getQueryHash() {
    return f1;
  }

  public String getAggMetric() {
    return f2;
  }

  /** (platformId, QUERY_TYPE, queryHashHex) */
  @Override
  public Tuple toRedisHashKey() {
    return Tuple3.of(getPlatformId(), QUERY_TYPE, Long.toHexString(getQueryHash()));
  }

  /** (featId) */
  @Override
  public Tuple toRedisHashField(int windowSize, ChronoUnit windowUnit) {
    return Tuple1.of(
        FeatureId.queryCount(AggMetric.valueOf(getAggMetric()), windowSize, windowUnit));
  }
}
