package ai.promoted.metrics.logprocessor.job.validateenrich;

import org.apache.flink.api.java.tuple.Tuple3;

/** Input into the RetainedUserId Lookup and Create operators. */
public class RetainedUserIdInput extends Tuple3<Long, String, String> {
  // For Flink serde.
  public RetainedUserIdInput() {}

  public RetainedUserIdInput(long platformId, String userId, String anonUserId) {
    super(platformId, userId, anonUserId);
  }
}
