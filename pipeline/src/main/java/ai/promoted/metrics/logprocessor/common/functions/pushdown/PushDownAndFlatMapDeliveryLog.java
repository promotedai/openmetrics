package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.event.LogRequest;

/**
 * Copies {@link LogRequest}'s {@link DeliveryLog} and fills in default base values. See base class
 * for more details.
 */
public class PushDownAndFlatMapDeliveryLog
    extends BaseDevicePushDownAndFlatMap<DeliveryLog, DeliveryLog.Builder> {

  public PushDownAndFlatMapDeliveryLog() {
    super(
        LogRequest::getDeliveryLogList,
        DeliveryLog::toBuilder,
        DeliveryLog.Builder::build,
        builder -> {
          if (builder.getPlatformId() != 0L) {
            return builder.getPlatformId();
          }
          return builder.getRequestBuilder().getPlatformId();
        },
        DeliveryLog.Builder::setPlatformId,
        builder -> builder.getRequest().hasUserInfo(),
        builder -> builder.getRequest().getUserInfo(),
        (builder, userInfo) -> builder.getRequestBuilder().setUserInfo(userInfo),
        builder -> builder.getRequest().hasTiming(),
        builder -> builder.getRequest().getTiming(),
        (builder, timing) -> builder.getRequestBuilder().setTiming(timing),
        builder -> builder.getRequest().hasClientInfo(),
        builder -> builder.getRequest().getClientInfo(),
        (builder, clientInfo) -> builder.getRequestBuilder().setClientInfo(clientInfo),
        builder -> builder.getRequest().hasDevice(),
        builder -> builder.getRequest().getDevice(),
        (builder, device) -> builder.getRequestBuilder().setDevice(device));
  }

  @Override
  protected void pushDownPlatformId(DeliveryLog.Builder builder, LogRequest batchValue) {
    super.pushDownPlatformId(builder, batchValue);
    // There's two platform_id fields on DeliveryLog.  Set both for now.
    long platformId = builder.getPlatformId();
    if (platformId == 0L) {
      platformId = builder.getRequestBuilder().getPlatformId();
    }
    if (builder.getPlatformId() != platformId) {
      builder.setPlatformId(platformId);
    }
    if (builder.getRequestBuilder().getPlatformId() != platformId) {
      builder.getRequestBuilder().setPlatformId(platformId);
    }
  }
}
