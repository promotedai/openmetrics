package ai.promoted.metrics.logprocessor.common.counter;

import static ai.promoted.metrics.logprocessor.common.counter.Constants.QUERY_TYPE;

import ai.promoted.proto.delivery.internal.features.AggMetric;
import java.time.temporal.ChronoUnit;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;

/** platformId, queryHash, aggMetric */
public class QueryEvent extends Tuple3<Long, Long, String> implements JoinedEventRedisHashSupplier {
  public static final String NAME = "query";

  public QueryEvent() {}

  public QueryEvent(Long platformId, Long queryHash, String aggMetric) {
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
  public Tuple getHashKey() {
    return Tuple3.of(getPlatformId(), QUERY_TYPE, Long.toHexString(getQueryHash()));
  }

  /** (featId) */
  @Override
  public Tuple getHashField(int windowSize, ChronoUnit windowUnit) {
    return Tuple1.of(
        FeatureId.queryCount(AggMetric.valueOf(getAggMetric()), windowSize, windowUnit));
  }
}
