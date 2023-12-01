package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import ai.promoted.proto.event.LogRequest;
import ai.promoted.proto.event.Session;

/**
 * Copies {@link LogRequest}'s {@link Session} and fills in default base values. See base class for
 * more details.
 */
public class PushDownAndFlatMapSession extends BasePushDownAndFlatMap<Session, Session.Builder> {

  public PushDownAndFlatMapSession() {
    super(
        LogRequest::getSessionList,
        Session::toBuilder,
        Session.Builder::build,
        Session.Builder::getPlatformId,
        Session.Builder::setPlatformId,
        Session.Builder::hasUserInfo,
        Session.Builder::getUserInfo,
        Session.Builder::setUserInfo,
        Session.Builder::hasTiming,
        Session.Builder::getTiming,
        Session.Builder::setTiming,
        Session.Builder::hasClientInfo,
        Session.Builder::getClientInfo,
        Session.Builder::setClientInfo);
  }
}
