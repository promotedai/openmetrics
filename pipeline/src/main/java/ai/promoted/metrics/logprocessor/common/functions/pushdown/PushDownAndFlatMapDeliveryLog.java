package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.event.LogRequest;
import java.util.function.Consumer;
import org.apache.flink.util.Collector;

/**
 * Copies {@link LogRequest}'s {@link DeliveryLog} and fills in default base values. See base class
 * for more details.
 */
public class PushDownAndFlatMapDeliveryLog extends BasePushDownAndFlatMap<DeliveryLog> {

  @Override
  public void flatMap(LogRequest logRequest, Collector<DeliveryLog> out) {
    flatMap(logRequest, out::collect);
  }

  public void flatMap(LogRequest logRequest, Consumer<DeliveryLog> out) {
    // Do this across all logUserIds.
    String lowerCaseBatchLogUserId = logRequest.getUserInfo().getLogUserId().toLowerCase();
    logRequest.getDeliveryLogList().stream()
        .map(deliveryLog -> pushDownFields(deliveryLog, logRequest, lowerCaseBatchLogUserId))
        .forEach(out);
  }

  public static DeliveryLog pushDownFields(
      DeliveryLog deliveryLog, LogRequest batchValue, String lowerCaseBatchLogUserId) {
    DeliveryLog.Builder builder = deliveryLog.toBuilder();
    if (builder.getPlatformId() == 0) {
      builder.setPlatformId(batchValue.getPlatformId());
    }
    // There can be a weird bug if DeliveryLog.platform_id and Request.platform_id are different.
    builder.getRequestBuilder().setPlatformId(builder.getPlatformId());
    pushDownFields(builder.getRequestBuilder(), batchValue, lowerCaseBatchLogUserId);
    return builder.build();
  }

  private static void pushDownFields(
      Request.Builder builder, LogRequest batchValue, String lowerCaseBatchLogUserId) {
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
  }
}
