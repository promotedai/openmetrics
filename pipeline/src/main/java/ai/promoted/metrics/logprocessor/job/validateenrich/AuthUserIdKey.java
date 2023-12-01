package ai.promoted.metrics.logprocessor.job.validateenrich;

import org.apache.flink.api.java.tuple.Tuple2;

public class AuthUserIdKey extends Tuple2<Long, String> {
  public static AuthUserIdKey toKey(SelectRetainedUserChange retainedUserChange) {
    return new AuthUserIdKey(retainedUserChange.getPlatformId(), retainedUserChange.getUserId());
  }

  // For Flink serde.
  public AuthUserIdKey() {}

  public AuthUserIdKey(long platformId, String userId) {
    super(platformId, userId);
  }
}
