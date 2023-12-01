package ai.promoted.metrics.logprocessor.job.validateenrich;

import java.io.Serializable;
import java.time.Instant;
import org.apache.flink.table.annotation.DataTypeHint;

/** RetainedUser lookup results. */
public class RetainedUserLookupRow implements Serializable {
  static AuthUserIdKey toKey(RetainedUserLookupRow row) {
    return new AuthUserIdKey(row.getPlatformId(), row.getUserId());
  }

  public Long platform_id;
  public String user_id;
  public String anon_user_id;
  public String retained_user_id;
  public Long last_forgotten_time_millis;
  public @DataTypeHint(value = "TIMESTAMP_LTZ(3)") Instant rowtime;

  // For Flink serialization.
  public RetainedUserLookupRow() {
    super();
  }

  public RetainedUserLookupRow(
      Long platform_id,
      String userId,
      String anonUserId,
      String retainedUserId,
      Long lastForgottenTimeMillis,
      Instant eventTime) {
    this.platform_id = platform_id;
    this.user_id = userId;
    this.anon_user_id = anonUserId;
    this.retained_user_id = retainedUserId;
    this.last_forgotten_time_millis = lastForgottenTimeMillis;
    this.rowtime = eventTime;
  }

  public Long getPlatformId() {

    return platform_id;
  }

  public String getUserId() {
    return user_id;
  }

  public String getAnonUserId() {
    return anon_user_id;
  }

  public String getRetainedUserId() {
    return retained_user_id;
  }
}
