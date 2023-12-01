package ai.promoted.metrics.logprocessor.common.util;

import ai.promoted.proto.common.ClientInfo;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.event.CombinedDeliveryLog;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Utils for DeliveryLog. This method is a bit different than FlatUtil. */
public class DeliveryLogUtil {
  private static final Logger LOGGER = LogManager.getLogger(DeliveryLogUtil.class);

  /**
   * Returns a priority for how important the traffic is for combining and joining. Higher is more
   * important.
   *
   * <ul>
   *   <li>3 = was used by the customer
   *   <li>2 = shadow traffic. We join in this but it's not as important as priority 3
   *   <li>1 = test and dev
   * </ul>
   */
  public static int getTrafficPriority(DeliveryLog deliveryLog) {
    return getTrafficPriority(deliveryLog.getRequest().getClientInfo().getTrafficType());
  }

  @VisibleForTesting
  static int getTrafficPriority(ClientInfo.TrafficType trafficType) {
    switch (trafficType) {
      case PRODUCTION:
      case UNKNOWN_TRAFFIC_TYPE:
      case UNRECOGNIZED:
        return 3;
        // Needed for the CombineDeliveryLog logic.
      case SHADOW:
        return 2;
      case LOAD_TEST:
      case INTERNAL:
      case REPLAY:
        return 1;
    }
    throw new UnsupportedOperationException("Unsupported TrafficType=" + trafficType);
  }

  public static boolean shouldJoin(DeliveryLog deliveryLog) {
    return shouldJoin(deliveryLog.getRequest().getClientInfo().getTrafficType());
  }

  public static boolean shouldJoin(ClientInfo.TrafficType trafficType) {
    return getTrafficPriority(trafficType) > 1;
  }

  public static String getAnonUserId(DeliveryLog deliveryLog) {
    return deliveryLog.getRequest().getUserInfo().getAnonUserId();
  }

  public static String getAnonUserId(CombinedDeliveryLog combinedDeliveryLog) {
    // Temporary logging code to catch potential key issue.
    // TODO - remove this condition after running in production for a bit.
    if (combinedDeliveryLog.hasApi() && combinedDeliveryLog.hasSdk()) {
      String apiAnonUserId = getAnonUserId(combinedDeliveryLog.getApi());
      String sdkAnonUserId = getAnonUserId(combinedDeliveryLog.getSdk());
      if (!Objects.equal(apiAnonUserId, sdkAnonUserId)) {
        LOGGER.warn(
            "DeliveryLogUtil.getAnonUserId(CombinedDeliveryLog) has mismatching platformIds, api.anonUserId={}, sdk.anonUserId={}",
            apiAnonUserId,
            sdkAnonUserId);
      }
      if (!apiAnonUserId.isEmpty()) {
        return apiAnonUserId;
      }
      return sdkAnonUserId;
    }
    return getAnonUserId(getDeliveryLog(combinedDeliveryLog));
  }

  // It's a little confusing for user_info to be retrieved from Request and platformId from the
  // parent.
  public static long getPlatformId(DeliveryLog deliveryLog) {
    return deliveryLog.getPlatformId();
  }

  public static long getPlatformId(CombinedDeliveryLog combinedDeliveryLog) {
    // Temporary logging code to catch potential key issue.
    // TODO - remove this condition after running in production for a bit.
    if (combinedDeliveryLog.hasApi() && combinedDeliveryLog.hasSdk()) {
      long apiPlatformId = combinedDeliveryLog.getApi().getPlatformId();
      long sdkPlatformId = combinedDeliveryLog.getSdk().getPlatformId();
      if (apiPlatformId != sdkPlatformId) {
        LOGGER.error(
            "DeliveryLogUtil.getPlatformId(CombinedDeliveryLog) has mismatching platformIds, api.platformId={}, sdk.platformId={}",
            apiPlatformId,
            sdkPlatformId);
      }
      if (apiPlatformId != 0L) {
        return apiPlatformId;
      }
      return sdkPlatformId;
    }
    return getPlatformId(getDeliveryLog(combinedDeliveryLog));
  }

  public static String getRequestId(CombinedDeliveryLog combinedDeliveryLog) {
    return getRequest(combinedDeliveryLog).getRequestId();
  }

  public static String getViewId(CombinedDeliveryLog combinedDeliveryLog) {
    return getRequest(combinedDeliveryLog).getViewId();
  }

  public static DeliveryLog getDeliveryLog(CombinedDeliveryLog combinedDeliveryLog) {
    DeliveryLog deliveryLog;
    if (combinedDeliveryLog.hasSdk()) {
      deliveryLog = combinedDeliveryLog.getSdk();
    } else {
      deliveryLog = combinedDeliveryLog.getApi();
    }
    return deliveryLog;
  }

  public static Request getRequest(CombinedDeliveryLog combinedDeliveryLog) {
    return getDeliveryLog(combinedDeliveryLog).getRequest();
  }
}
