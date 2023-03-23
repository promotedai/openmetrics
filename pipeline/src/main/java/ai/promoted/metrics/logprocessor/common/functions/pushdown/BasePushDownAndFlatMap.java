package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import ai.promoted.proto.common.ClientInfo;
import ai.promoted.proto.common.Device;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.LogRequest;
import java.util.Objects;
import org.apache.flink.api.common.functions.FlatMapFunction;

/**
 * Copies a specific sub-log from {@link LogRequest} and fills in missing sub-log values from {@link
 * LogRequest} values.
 *
 * <p>This is called before doing other processing so we log out the values on each specific type.
 *
 * @param <T> the type of the log record on {@link LogRequest}
 */
public abstract class BasePushDownAndFlatMap<T> implements FlatMapFunction<LogRequest, T> {

  // TODO(PRO-2420) - In Metrics API, set a primary key on LogRequest.  Change auto-generated UUIDs
  // to be based on the LogRequest.log_request_id.

  static void pushDownUserIdFields(UserInfo.Builder userInfoBuilder, LogRequest batchValue) {
    if (Objects.equals(userInfoBuilder.getUserId(), "")) {
      userInfoBuilder.setUserId(batchValue.getUserInfo().getUserId());
    }
  }

  static void pushDownLogUserIdFields(
      UserInfo.Builder userInfoBuilder, String lowerCaseBatchLogUserId) {
    if (Objects.equals(userInfoBuilder.getLogUserId(), "")) {
      userInfoBuilder.setLogUserId(lowerCaseBatchLogUserId);
    } else {
      lowerCaseLogUserIdFields(userInfoBuilder);
    }
  }

  static void lowerCaseLogUserIdFields(UserInfo.Builder userInfoBuilder) {
    if (hasUpperCase(userInfoBuilder.getLogUserId())) {
      userInfoBuilder.setLogUserId(userInfoBuilder.getLogUserId().toLowerCase());
    }
  }

  static void pushDownTiming(Timing.Builder timingBuilder, LogRequest batchValue) {
    if (timingBuilder.getClientLogTimestamp() == 0) {
      timingBuilder.setClientLogTimestamp(batchValue.getTiming().getClientLogTimestamp());
    }
    if (timingBuilder.getEventApiTimestamp() == 0) {
      timingBuilder.setEventApiTimestamp(batchValue.getTiming().getEventApiTimestamp());
    }
  }

  static void pushDownClientInfo(ClientInfo.Builder clientInfoBuilder, LogRequest batchValue) {
    if (clientInfoBuilder.getClientTypeValue() == 0) {
      clientInfoBuilder.setClientType(batchValue.getClientInfo().getClientType());
    }
    if (clientInfoBuilder.getTrafficTypeValue() == 0) {
      clientInfoBuilder.setTrafficType(batchValue.getClientInfo().getTrafficType());
    }
  }

  static void pushDownDevice(Device.Builder deviceBuilder, LogRequest batchValue) {
    // TODO: implement?
  }

  static boolean hasUpperCase(String s) {
    return s.chars().anyMatch(Character::isUpperCase);
  }
}
