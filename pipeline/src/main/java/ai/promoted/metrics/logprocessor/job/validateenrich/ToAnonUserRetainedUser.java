package ai.promoted.metrics.logprocessor.job.validateenrich;

import ai.promoted.metrics.logprocessor.common.util.TrackingUtil;
import ai.promoted.proto.common.AnonUserRetainedUser;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.EnrichmentUnion;
import com.google.common.annotations.VisibleForTesting;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/** Converts {@code EnrichmentUnion} to @code AnonUserLogUser}. */
public class ToAnonUserRetainedUser extends ProcessFunction<EnrichmentUnion, AnonUserRetainedUser> {

  /** Returns whether the {@code union} can be mapped to a {@code AnonUserRetainedUser}. */
  @VisibleForTesting
  static boolean canMap(UserInfo userInfo) {
    return !userInfo.getAnonUserId().isEmpty() && !userInfo.getRetainedUserId().isEmpty();
  }

  @Override
  public void processElement(
      EnrichmentUnion enrichmentUnion,
      ProcessFunction<EnrichmentUnion, AnonUserRetainedUser>.Context context,
      Collector<AnonUserRetainedUser> collector)
      throws Exception {
    UserInfo userInfo = EnrichmentUnionUtil.getUserInfo(enrichmentUnion);
    if (canMap(userInfo)) {
      collector.collect(
          AnonUserRetainedUser.newBuilder()
              .setPlatformId(EnrichmentUnionUtil.getPlatformId(enrichmentUnion))
              .setAnonUserId(userInfo.getAnonUserId())
              .setRetainedUserId(userInfo.getRetainedUserId())
              .setEventTimeMillis(context.timestamp())
              .setProcessTimeMillis(TrackingUtil.getProcessingTime())
              .build());
    }
  }
}
