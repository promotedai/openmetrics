package ai.promoted.metrics.logprocessor.common.functions;

import ai.promoted.proto.common.ClientInfo;
import ai.promoted.proto.common.Device;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.AutoView;
import ai.promoted.proto.event.CohortMembership;
import ai.promoted.proto.event.Diagnostics;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.LogRequest;
import ai.promoted.proto.event.Session;
import ai.promoted.proto.event.SessionProfile;
import ai.promoted.proto.event.User;
import ai.promoted.proto.event.View;
import java.util.Objects;
import java.util.function.Function;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Copies {@link LogRequest} and copies missing values from {@link LogRequest} to each sub-log.
 *
 * <p>This is called before doing other processing so we log out the values on each specific type.
 */
public class PushDownBatchFields
    implements Function<LogRequest, LogRequest>, MapFunction<LogRequest, LogRequest> {

  @Override
  public LogRequest map(LogRequest value) throws Exception {
    return staticApply(value);
  }

  @Override
  public LogRequest apply(LogRequest value) {
    return staticApply(value);
  }

  // TODO(PRO-2420) - Lower-case the keys.  Move from Metrics API.
  // TODO(PRO-2420) - In Metrics API, set a primary key on LogRequest.  Change auto-generated UUIDs
  // to be based on the LogRequest.log_request_id.

  public static LogRequest staticApply(LogRequest value) {
    LogRequest.Builder requestBuilder = value.toBuilder();

    String lowerCaseBatchLogUserId = value.getUserInfo().getLogUserId().toLowerCase();
    if (requestBuilder.hasUserInfo()) {
      pushDownLogUserIdFields(requestBuilder.getUserInfoBuilder(), lowerCaseBatchLogUserId);
    }

    requestBuilder
        .getUserBuilderList()
        .forEach(builder -> pushDownFields(builder, value, lowerCaseBatchLogUserId));
    requestBuilder
        .getCohortMembershipBuilderList()
        .forEach(builder -> pushDownFields(builder, value, lowerCaseBatchLogUserId));
    requestBuilder
        .getSessionProfileBuilderList()
        .forEach(builder -> pushDownFields(builder, value, lowerCaseBatchLogUserId));
    requestBuilder
        .getSessionBuilderList()
        .forEach(builder -> pushDownFields(builder, value, lowerCaseBatchLogUserId));
    requestBuilder
        .getDeliveryLogBuilderList()
        .forEach(builder -> pushDownFields(builder, value, lowerCaseBatchLogUserId));
    requestBuilder
        .getViewBuilderList()
        .forEach(builder -> pushDownFields(builder, value, lowerCaseBatchLogUserId));
    requestBuilder
        .getAutoViewBuilderList()
        .forEach(builder -> pushDownFields(builder, value, lowerCaseBatchLogUserId));
    requestBuilder
        .getRequestBuilderList()
        .forEach(builder -> pushDownFields(builder, value, lowerCaseBatchLogUserId));
    requestBuilder
        .getInsertionBuilderList()
        .forEach(builder -> pushDownFields(builder, value, lowerCaseBatchLogUserId));
    requestBuilder
        .getImpressionBuilderList()
        .forEach(builder -> pushDownFields(builder, value, lowerCaseBatchLogUserId));
    requestBuilder
        .getActionBuilderList()
        .forEach(builder -> pushDownFields(builder, value, lowerCaseBatchLogUserId));
    requestBuilder
        .getDiagnosticsBuilderList()
        .forEach(builder -> pushDownFields(builder, value, lowerCaseBatchLogUserId));

    return requestBuilder.build();
  }

  private static void pushDownUserIdFields(
      UserInfo.Builder userInfoBuilder, LogRequest batchValue) {
    if (Objects.equals(userInfoBuilder.getUserId(), "")) {
      userInfoBuilder.setUserId(batchValue.getUserInfo().getUserId());
    }
  }

  private static void pushDownLogUserIdFields(
      UserInfo.Builder userInfoBuilder, String lowerCaseBatchLogUserId) {
    if (Objects.equals(userInfoBuilder.getLogUserId(), "")) {
      userInfoBuilder.setLogUserId(lowerCaseBatchLogUserId);
    } else {
      lowerCaseLogUserIdFields(userInfoBuilder);
    }
  }

  private static void lowerCaseLogUserIdFields(UserInfo.Builder userInfoBuilder) {
    if (hasUpperCase(userInfoBuilder.getLogUserId())) {
      userInfoBuilder.setLogUserId(userInfoBuilder.getLogUserId().toLowerCase());
    }
  }

  private static void pushDownTiming(Timing.Builder timingBuilder, LogRequest batchValue) {
    if (timingBuilder.getClientLogTimestamp() == 0) {
      timingBuilder.setClientLogTimestamp(batchValue.getTiming().getClientLogTimestamp());
    }
    if (timingBuilder.getEventApiTimestamp() == 0) {
      timingBuilder.setEventApiTimestamp(batchValue.getTiming().getEventApiTimestamp());
    }
  }

  private static void pushDownClientInfo(
      ClientInfo.Builder clientInfoBuilder, LogRequest batchValue) {
    if (clientInfoBuilder.getClientTypeValue() == 0) {
      clientInfoBuilder.setClientType(batchValue.getClientInfo().getClientType());
    }
    if (clientInfoBuilder.getTrafficTypeValue() == 0) {
      clientInfoBuilder.setTrafficType(batchValue.getClientInfo().getTrafficType());
    }
  }

  private static void pushDownDevice(Device.Builder deviceBuilder, LogRequest batchValue) {
    // TODO: implement?
  }

  private static void pushDownFields(
      User.Builder builder, LogRequest batchValue, String lowerCaseBatchLogUserId) {
    if (builder.getPlatformId() == 0) {
      builder.setPlatformId(batchValue.getPlatformId());
    }
    if (batchValue.hasUserInfo()) {
      pushDownUserIdFields(builder.getUserInfoBuilder(), batchValue);
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

  private static void pushDownFields(
      CohortMembership.Builder builder, LogRequest batchValue, String lowerCaseBatchLogUserId) {
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
  }

  private static void pushDownFields(
      SessionProfile.Builder builder, LogRequest batchValue, String lowerCaseBatchLogUserId) {
    if (builder.getPlatformId() == 0) {
      builder.setPlatformId(batchValue.getPlatformId());
    }
    if (batchValue.hasUserInfo()) {
      pushDownUserIdFields(builder.getUserInfoBuilder(), batchValue);
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

  private static void pushDownFields(
      Session.Builder builder, LogRequest batchValue, String lowerCaseBatchLogUserId) {
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
  }

  private static void pushDownFields(
      DeliveryLog.Builder builder, LogRequest batchValue, String lowerCaseBatchLogUserId) {
    if (builder.getPlatformId() == 0) {
      builder.setPlatformId(batchValue.getPlatformId());
    }
    pushDownFields(builder.getRequestBuilder(), batchValue, lowerCaseBatchLogUserId);
  }

  private static void pushDownFields(
      View.Builder builder, LogRequest batchValue, String lowerCaseBatchLogUserId) {
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
    /* Marked as deprecated, but used still... should we do this?
    if (batchValue.hasDevice()) {
        pushDownDevice(builder.getDeviceBuilder(), batchValue);
    }*/
  }

  private static void pushDownFields(
      AutoView.Builder builder, LogRequest batchValue, String lowerCaseBatchLogUserId) {
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
  }

  private static void pushDownFields(
      Request.Builder builder, LogRequest batchValue, String lowerCaseBatchLogUserId) {
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
    if (batchValue.hasDevice()) {
      pushDownDevice(builder.getDeviceBuilder(), batchValue);
    }
  }

  private static void pushDownFields(
      Insertion.Builder builder, LogRequest batchValue, String lowerCaseBatchLogUserId) {
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
  }

  private static void pushDownFields(
      Impression.Builder builder, LogRequest batchValue, String lowerCaseBatchLogUserId) {
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
  }

  private static void pushDownFields(
      Action.Builder builder, LogRequest batchValue, String lowerCaseBatchLogUserId) {
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
    if (batchValue.hasDevice()) {
      pushDownDevice(builder.getDeviceBuilder(), batchValue);
    }
  }

  private static void pushDownFields(
      Diagnostics.Builder builder, LogRequest batchValue, String lowerCaseBatchLogUserId) {
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
  }

  static boolean hasUpperCase(String s) {
    return s.chars().anyMatch(Character::isUpperCase);
  }
}
