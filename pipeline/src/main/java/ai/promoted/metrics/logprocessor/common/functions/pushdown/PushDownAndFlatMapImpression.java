package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.LogRequest;

/**
 * Copies {@link LogRequest}'s {@link Impression} and fills in default base values. See base class
 * for more details.
 */
public class PushDownAndFlatMapImpression
    extends BasePushDownAndFlatMap<Impression, Impression.Builder> {
  public PushDownAndFlatMapImpression() {
    super(
        LogRequest::getImpressionList,
        Impression::toBuilder,
        Impression.Builder::build,
        Impression.Builder::getPlatformId,
        Impression.Builder::setPlatformId,
        Impression.Builder::hasUserInfo,
        Impression.Builder::getUserInfo,
        Impression.Builder::setUserInfo,
        Impression.Builder::hasTiming,
        Impression.Builder::getTiming,
        Impression.Builder::setTiming,
        Impression.Builder::hasClientInfo,
        Impression.Builder::getClientInfo,
        Impression.Builder::setClientInfo);
  }
}
