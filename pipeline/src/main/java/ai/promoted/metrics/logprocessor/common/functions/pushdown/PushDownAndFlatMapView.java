package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import ai.promoted.proto.event.LogRequest;
import ai.promoted.proto.event.View;

/**
 * Copies {@link LogRequest}'s {@link View} and fills in default base values. See base class for
 * more details.
 */
public class PushDownAndFlatMapView extends BaseDevicePushDownAndFlatMap<View, View.Builder> {
  public PushDownAndFlatMapView() {
    super(
        LogRequest::getViewList,
        View::toBuilder,
        View.Builder::build,
        View.Builder::getPlatformId,
        View.Builder::setPlatformId,
        View.Builder::hasUserInfo,
        View.Builder::getUserInfo,
        View.Builder::setUserInfo,
        View.Builder::hasTiming,
        View.Builder::getTiming,
        View.Builder::setTiming,
        View.Builder::hasClientInfo,
        View.Builder::getClientInfo,
        View.Builder::setClientInfo,
        View.Builder::hasDevice,
        View.Builder::getDevice,
        View.Builder::setDevice);
  }
}
