package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import ai.promoted.proto.event.LogRequest;
import ai.promoted.proto.event.User;

/**
 * Copies {@link LogRequest}'s {@link User} and fills in default base values. See base class for
 * more details.
 */
public class PushDownAndFlatMapUser extends BasePushDownAndFlatMap<User, User.Builder> {
  public PushDownAndFlatMapUser() {
    super(
        LogRequest::getUserList,
        User::toBuilder,
        User.Builder::build,
        User.Builder::getPlatformId,
        User.Builder::setPlatformId,
        User.Builder::hasUserInfo,
        User.Builder::getUserInfo,
        User.Builder::setUserInfo,
        User.Builder::hasTiming,
        User.Builder::getTiming,
        User.Builder::setTiming,
        User.Builder::hasClientInfo,
        User.Builder::getClientInfo,
        User.Builder::setClientInfo);
  }
}
