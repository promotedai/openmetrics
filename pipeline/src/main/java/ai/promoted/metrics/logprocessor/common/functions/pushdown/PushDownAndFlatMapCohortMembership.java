package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import ai.promoted.proto.event.CohortMembership;
import ai.promoted.proto.event.LogRequest;

/**
 * Copies {@link LogRequest}'s {@link CohortMembership} and fills in default base values. See base
 * class for more details.
 */
public class PushDownAndFlatMapCohortMembership
    extends BasePushDownAndFlatMap<CohortMembership, CohortMembership.Builder> {
  public PushDownAndFlatMapCohortMembership() {
    super(
        LogRequest::getCohortMembershipList,
        CohortMembership::toBuilder,
        CohortMembership.Builder::build,
        CohortMembership.Builder::getPlatformId,
        CohortMembership.Builder::setPlatformId,
        CohortMembership.Builder::hasUserInfo,
        CohortMembership.Builder::getUserInfo,
        CohortMembership.Builder::setUserInfo,
        CohortMembership.Builder::hasTiming,
        CohortMembership.Builder::getTiming,
        CohortMembership.Builder::setTiming,
        CohortMembership.Builder::hasClientInfo,
        CohortMembership.Builder::getClientInfo,
        CohortMembership.Builder::setClientInfo);
  }
}
