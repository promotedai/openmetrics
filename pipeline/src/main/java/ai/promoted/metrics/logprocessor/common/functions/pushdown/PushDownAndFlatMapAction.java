package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.LogRequest;

/**
 * Copies {@link LogRequest}'s {@link Action} and fills in default base values. See base class for
 * more details.
 */
public class PushDownAndFlatMapAction extends BaseDevicePushDownAndFlatMap<Action, Action.Builder> {

  public PushDownAndFlatMapAction() {
    super(
        LogRequest::getActionList,
        Action::toBuilder,
        Action.Builder::build,
        Action.Builder::getPlatformId,
        Action.Builder::setPlatformId,
        Action.Builder::hasUserInfo,
        Action.Builder::getUserInfo,
        Action.Builder::setUserInfo,
        Action.Builder::hasTiming,
        Action.Builder::getTiming,
        Action.Builder::setTiming,
        Action.Builder::hasClientInfo,
        Action.Builder::getClientInfo,
        Action.Builder::setClientInfo,
        Action.Builder::hasDevice,
        Action.Builder::getDevice,
        Action.Builder::setDevice);
  }
}
