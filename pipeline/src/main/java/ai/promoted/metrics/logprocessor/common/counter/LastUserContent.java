package ai.promoted.metrics.logprocessor.common.counter;

import static ai.promoted.metrics.logprocessor.common.counter.Constants.USER_TYPE;

import ai.promoted.proto.delivery.internal.features.AggMetric;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple4;

/** platformId, userId, contentId, aggMetric */
public class LastUserContent extends Tuple4<Long, String, String, String>
    implements LastUserEventRedisHashSupplier {
  public static final String NAME = "last-time-user-event";

  public LastUserContent() {}

  public LastUserContent(Long platformId, String user, String contendId, String aggMetric) {
    super(platformId, user, contendId, aggMetric);
  }

  public Long getPlatformId() {
    return f0;
  }

  public String getUser() {
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
    return Tuple4.of(getPlatformId(), USER_TYPE, getUser(), getContentId());
  }

  /** (fid) */
  @Override
  public Tuple getCount90DayHashField() {
    return Tuple1.of(FeatureId.lastUserContentCount(false, AggMetric.valueOf(getAggMetric())));
  }

  /** (fid) */
  @Override
  public Tuple getTimestampHashField() {
    return Tuple1.of(FeatureId.lastUserContentTimestamp(false, AggMetric.valueOf(getAggMetric())));
  }
}
