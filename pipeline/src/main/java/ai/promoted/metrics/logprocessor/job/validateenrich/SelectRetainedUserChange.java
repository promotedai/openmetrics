package ai.promoted.metrics.logprocessor.job.validateenrich;

// PR - would we rather have this as proto so it's easier to extend?

import org.apache.flink.api.java.tuple.Tuple4;

/** Smaller version of RetainedUserChange that is used in the join operator. */
public class SelectRetainedUserChange extends Tuple4<ChangeKind, Long, String, String> {

  // For Flink serde.
  public SelectRetainedUserChange() {}

  public SelectRetainedUserChange(
      ChangeKind kind, Long platformId, String userId, String retainedUserId) {
    super(kind, platformId, userId, retainedUserId);
  }

  public ChangeKind getKind() {
    return f0;
  }

  public Long getPlatformId() {
    return f1;
  }

  public String getUserId() {
    return f2;
  }

  public String getRetainedUserId() {
    return f3;
  }
}
