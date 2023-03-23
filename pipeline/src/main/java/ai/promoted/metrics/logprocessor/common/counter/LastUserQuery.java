package ai.promoted.metrics.logprocessor.common.counter;

import static ai.promoted.metrics.logprocessor.common.counter.Constants.QUERY_TYPE;
import static ai.promoted.metrics.logprocessor.common.counter.Constants.USER_TYPE;

import ai.promoted.proto.delivery.internal.features.AggMetric;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

/** platformId, userId, queryHash, aggMetric */
public class LastUserQuery extends Tuple4<Long, String, Long, String>
    implements LastUserEventRedisHashSupplier {
  public static final String NAME = "last-time-user-query";

  public LastUserQuery() {}

  public LastUserQuery(Long platformId, String user, Long queryHash, String aggMetric) {
    super(platformId, user, queryHash, aggMetric);
  }

  public Long getPlatformId() {
    return f0;
  }

  public String getUser() {
    return f1;
  }

  public Long getQueryHash() {
    return f2;
  }

  public String getAggMetric() {
    return f3;
  }

  /** (platformId, USER_TYPE, user, QUERY_TYPE, queryHashHex) */
  @Override
  public Tuple getHashKey() {
    return Tuple5.of(
        getPlatformId(), USER_TYPE, getUser(), QUERY_TYPE, Long.toHexString(getQueryHash()));
  }

  /** (fid) */
  @Override
  public Tuple getCount90DayHashField() {
    return Tuple1.of(FeatureId.lastUserQueryCount(false, AggMetric.valueOf(getAggMetric())));
  }

  /** (fid) */
  @Override
  public Tuple getTimestampHashField() {
    return Tuple1.of(FeatureId.lastUserQueryTimestamp(false, AggMetric.valueOf(getAggMetric())));
  }
}
