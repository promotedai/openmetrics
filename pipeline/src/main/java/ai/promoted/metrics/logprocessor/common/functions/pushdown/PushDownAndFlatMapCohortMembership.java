package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import ai.promoted.proto.event.CohortMembership;
import ai.promoted.proto.event.LogRequest;
import java.util.function.Consumer;
import org.apache.flink.util.Collector;

/**
 * Copies {@link LogRequest}'s {@link CohortMembership} and fills in default base values. See base
 * class for more details.
 */
public class PushDownAndFlatMapCohortMembership extends BasePushDownAndFlatMap<CohortMembership> {

  @Override
  public void flatMap(LogRequest logRequest, Collector<CohortMembership> out) {
    flatMap(logRequest, out::collect);
  }

  public void flatMap(LogRequest logRequest, Consumer<CohortMembership> out) {
    // Do this across all logUserIds.
    String lowerCaseBatchLogUserId = logRequest.getUserInfo().getLogUserId().toLowerCase();
    logRequest.getCohortMembershipList().stream()
        .map(
            cohortMembership ->
                pushDownFields(cohortMembership, logRequest, lowerCaseBatchLogUserId))
        .forEach(out);
  }

  public static CohortMembership pushDownFields(
      CohortMembership cohortMembership, LogRequest batchValue, String lowerCaseBatchLogUserId) {
    CohortMembership.Builder builder = cohortMembership.toBuilder();
    if (builder.getPlatformId() == 0) {
      builder.setPlatformId(batchValue.getPlatformId());
    }
    if (batchValue.hasUserInfo()) {
      pushDownLogUserIdFields(builder.getUserInfoBuilder(), lowerCaseBatchLogUserId);
    } else if (builder.hasUserInfo()) {
      // If the top-level `LogRequest.user_info` is not set, we still want to lowercase any
      // logUserIds.
      lowerCaseLogUserIdFields(builder.getUserInfoBuilder());
    }
    if (batchValue.hasTiming()) {
      pushDownTiming(builder.getTimingBuilder(), batchValue);
    }
    if (batchValue.hasClientInfo()) {
      pushDownClientInfo(builder.getClientInfoBuilder(), batchValue);
    }
    return builder.build();
  }
}
