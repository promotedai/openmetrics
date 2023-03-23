package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import ai.promoted.proto.event.LogRequest;
import ai.promoted.proto.event.Session;
import java.util.function.Consumer;
import org.apache.flink.util.Collector;

/**
 * Copies {@link LogRequest}'s {@link Session} and fills in default base values. See base class for
 * more details.
 */
public class PushDownAndFlatMapSession extends BasePushDownAndFlatMap<Session> {

  @Override
  public void flatMap(LogRequest logRequest, Collector<Session> out) {
    flatMap(logRequest, out::collect);
  }

  public void flatMap(LogRequest logRequest, Consumer<Session> out) {
    // Do this across all logUserIds.
    String lowerCaseBatchLogUserId = logRequest.getUserInfo().getLogUserId().toLowerCase();
    logRequest.getSessionList().stream()
        .map(session -> pushDownFields(session, logRequest, lowerCaseBatchLogUserId))
        .forEach(out);
  }

  private static Session pushDownFields(
      Session session, LogRequest batchValue, String lowerCaseBatchLogUserId) {
    Session.Builder builder = session.toBuilder();
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
