package ai.promoted.metrics.logprocessor.common.counter;

import static ai.promoted.metrics.logprocessor.common.counter.Constants.USER_TYPE;

import ai.promoted.proto.delivery.internal.features.AggMetric;
import java.time.temporal.ChronoUnit;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;

/** platformId, logUserId, aggMetric */
public class LogUserEvent extends Tuple3<Long, String, String>
    implements JoinedEventRedisHashSupplier {
  public static final String NAME = "log-user";

  public LogUserEvent() {}

  public LogUserEvent(Long platformId, String user, String aggMetric) {
    super(platformId, user, aggMetric);
  }

  public Long getPlatformId() {
    return f0;
  }

  public String getUser() {
    return f1;
  }

  public String getAggMetric() {
    return f2;
  }

  protected boolean isLogUser() {
    return true;
  }

  /** (platformId, USER_TYPE, userId) */
  @Override
  public Tuple getHashKey() {
    return Tuple3.of(getPlatformId(), USER_TYPE, getUser());
  }

  /** (fid) */
  @Override
  public Tuple getHashField(int windowSize, ChronoUnit windowUnit) {
    return Tuple1.of(
        FeatureId.userCount(
            isLogUser(), AggMetric.valueOf(getAggMetric()), windowSize, windowUnit));
  }
}
