package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.LogRequest;
import java.util.function.Consumer;
import org.apache.flink.util.Collector;

/**
 * Copies {@link LogRequest}'s {@link Impression} and fills in default base values. See base class
 * for more details.
 */
public class PushDownAndFlatMapImpression extends BasePushDownAndFlatMap<Impression> {

  @Override
  public void flatMap(LogRequest logRequest, Collector<Impression> out) {
    flatMap(logRequest, out::collect);
  }

  public void flatMap(LogRequest logRequest, Consumer<Impression> out) {
    // Do this across all logUserIds.
    String lowerCaseBatchLogUserId = logRequest.getUserInfo().getLogUserId().toLowerCase();
    logRequest.getImpressionList().stream()
        .map(impression -> pushDownFields(impression, logRequest, lowerCaseBatchLogUserId))
        .forEach(out);
  }

  public static Impression pushDownFields(
      Impression impression, LogRequest batchValue, String lowerCaseBatchLogUserId) {
    Impression.Builder builder = impression.toBuilder();
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
