package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import ai.promoted.proto.event.AutoView;
import ai.promoted.proto.event.LogRequest;

/**
 * Copies {@link LogRequest}'s {@link AutoView} and fills in default base values. See base class for
 * more details.
 */
public class PushDownAndFlatMapAutoView extends BasePushDownAndFlatMap<AutoView, AutoView.Builder> {

  public PushDownAndFlatMapAutoView() {
    super(
        LogRequest::getAutoViewList,
        AutoView::toBuilder,
        AutoView.Builder::build,
        AutoView.Builder::getPlatformId,
        AutoView.Builder::setPlatformId,
        AutoView.Builder::hasUserInfo,
        AutoView.Builder::getUserInfo,
        AutoView.Builder::setUserInfo,
        AutoView.Builder::hasTiming,
        AutoView.Builder::getTiming,
        AutoView.Builder::setTiming,
        AutoView.Builder::hasClientInfo,
        AutoView.Builder::getClientInfo,
        AutoView.Builder::setClientInfo);
  }
}
