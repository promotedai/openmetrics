package ai.promoted.metrics.logprocessor.job.validateenrich;

import ai.promoted.metrics.common.LogUserUser;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.EnrichmentUnion;
import com.google.common.annotations.VisibleForTesting;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/** Converts {@code EnrichmentUnion} to @code LogUserUser}. */
public class ToLogUserUser extends ProcessFunction<EnrichmentUnion, LogUserUser> {

  /** Returns whether the {@code union} can be mapped to a {@code LogUserUser}. */
  @VisibleForTesting
  static boolean canMap(UserInfo userInfo) {
    return !userInfo.getLogUserId().isEmpty() && !userInfo.getUserId().isEmpty();
  }

  @Override
  public void processElement(
      EnrichmentUnion enrichmentUnion,
      ProcessFunction<EnrichmentUnion, LogUserUser>.Context context,
      Collector<LogUserUser> collector)
      throws Exception {
    UserInfo userInfo = EnrichmentUnionUtil.getUserInfo(enrichmentUnion);
    if (canMap(userInfo)) {
      collector.collect(
          LogUserUser.newBuilder()
              .setPlatformId(EnrichmentUnionUtil.getPlatformId(enrichmentUnion))
              .setEventTimeMillis(context.timestamp())
              .setLogUserId(userInfo.getLogUserId())
              .setUserId(userInfo.getUserId())
              .build());
    }
  }
}
