package ai.promoted.metrics.logprocessor.job.validateenrich;

import org.apache.flink.api.java.tuple.Tuple2;

public class AnonUserIdKey extends Tuple2<Long, String> {
  // For Flink serde.
  public AnonUserIdKey() {}

  public AnonUserIdKey(long platformId, String anonUserId) {
    super(platformId, anonUserId);
  }
}
