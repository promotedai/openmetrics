package ai.promoted.metrics.logprocessor.job.validateenrich;

import org.apache.flink.api.java.tuple.Tuple2;

/** Key for RetainedUserId lookup and create operators. */
public class RetainedUserIdKey extends Tuple2<Long, String> {
  // For Flink serde.
  public RetainedUserIdKey() {}

  public RetainedUserIdKey(long platformId, String userId) {
    super(platformId, userId);
  }
}
