package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import ai.promoted.proto.event.Diagnostics;
import ai.promoted.proto.event.LogRequest;

/**
 * Copies {@link LogRequest}'s {@link Diagnostics} and fills in default base values. See base class
 * for more details.
 */
public class PushDownAndFlatMapDiagnostics
    extends BasePushDownAndFlatMap<Diagnostics, Diagnostics.Builder> {
  public PushDownAndFlatMapDiagnostics() {
    super(
        LogRequest::getDiagnosticsList,
        Diagnostics::toBuilder,
        Diagnostics.Builder::build,
        Diagnostics.Builder::getPlatformId,
        Diagnostics.Builder::setPlatformId,
        Diagnostics.Builder::hasUserInfo,
        Diagnostics.Builder::getUserInfo,
        Diagnostics.Builder::setUserInfo,
        Diagnostics.Builder::hasTiming,
        Diagnostics.Builder::getTiming,
        Diagnostics.Builder::setTiming,
        Diagnostics.Builder::hasClientInfo,
        Diagnostics.Builder::getClientInfo,
        Diagnostics.Builder::setClientInfo);
  }
}
