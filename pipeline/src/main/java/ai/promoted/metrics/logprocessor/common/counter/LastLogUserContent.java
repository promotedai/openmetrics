package ai.promoted.metrics.logprocessor.common.counter;

import static ai.promoted.metrics.logprocessor.common.counter.Constants.USER_TYPE;

import ai.promoted.proto.delivery.internal.features.AggMetric;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple4;

/** platformId, logUserId, contentId, aggMetric */
public class LastLogUserContent extends Tuple4<Long, String, String, String>
    implements LastUserEventRedisHashSupplier {
  public static final String NAME = "last-time-log-user-event";

  public LastLogUserContent() {}

  public LastLogUserContent(Long platformId, String logUser, String contentId, String aggMetric) {
    super(platformId, logUser, contentId, aggMetric);
  }

  public Long getPlatformId() {
    return f0;
  }

  public String getLogUser() {
    return f1;
  }

  public String getContentId() {
    return f2;
  }

  public String getAggMetric() {
    return f3;
  }

  @Override
  public Tuple getHashKey() {
    return Tuple4.of(getPlatformId(), USER_TYPE, getLogUser(), getContentId());
  }

  /** (fid) */
  @Override
  public Tuple getCount90DayHashField() {
    return Tuple1.of(FeatureId.lastUserContentCount(true, AggMetric.valueOf(getAggMetric())));
  }

  /** (fid) */
  @Override
  public Tuple getTimestampHashField() {
    return Tuple1.of(FeatureId.lastUserContentTimestamp(true, AggMetric.valueOf(getAggMetric())));
  }
}
