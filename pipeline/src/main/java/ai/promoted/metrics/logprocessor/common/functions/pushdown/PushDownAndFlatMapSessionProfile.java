package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import ai.promoted.proto.event.LogRequest;
import ai.promoted.proto.event.SessionProfile;

/**
 * Copies {@link LogRequest}'s {@link SessionProfile} and fills in default base values. See base
 * class for more details.
 */
public class PushDownAndFlatMapSessionProfile
    extends BasePushDownAndFlatMap<SessionProfile, SessionProfile.Builder> {

  public PushDownAndFlatMapSessionProfile() {
    super(
        LogRequest::getSessionProfileList,
        SessionProfile::toBuilder,
        SessionProfile.Builder::build,
        SessionProfile.Builder::getPlatformId,
        SessionProfile.Builder::setPlatformId,
        SessionProfile.Builder::hasUserInfo,
        SessionProfile.Builder::getUserInfo,
        SessionProfile.Builder::setUserInfo,
        SessionProfile.Builder::hasTiming,
        SessionProfile.Builder::getTiming,
        SessionProfile.Builder::setTiming,
        SessionProfile.Builder::hasClientInfo,
        SessionProfile.Builder::getClientInfo,
        SessionProfile.Builder::setClientInfo);
  }
}
