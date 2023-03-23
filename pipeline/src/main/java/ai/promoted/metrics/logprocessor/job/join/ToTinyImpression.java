package ai.promoted.metrics.logprocessor.job.join;

import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.TinyEvent;
import java.util.List;
import org.apache.flink.api.common.functions.MapFunction;

final class ToTinyImpression implements MapFunction<Impression, TinyEvent> {
  private final OtherContentIdsConverter otherContentIdsConverter;

  ToTinyImpression(List<String> requestInsertionOtherContentIdKeys) {
    this.otherContentIdsConverter =
        new OtherContentIdsConverter(requestInsertionOtherContentIdKeys);
  }

  @Override
  public TinyEvent map(Impression impression) {
    TinyEvent.Builder tinyImpression =
        TinyEvent.newBuilder()
            .setPlatformId(impression.getPlatformId())
            .setLogUserId(impression.getUserInfo().getLogUserId())
            .setLogTimestamp(impression.getTiming().getLogTimestamp())
            .setViewId(impression.getViewId())
            .setRequestId(impression.getRequestId())
            .setInsertionId(impression.getInsertionId())
            .setContentId(impression.getContentId())
            .setImpressionId(impression.getImpressionId());

    // We don't expect customers to see this.  Customers should stick these properties onto
    // RequestInsertions.
    if (otherContentIdsConverter.hasKeys() && impression.hasProperties()) {
      otherContentIdsConverter.putFromProperties(
          tinyImpression::putOtherContentIds, impression.getProperties());
    }
    return tinyImpression.build();
  }
}
