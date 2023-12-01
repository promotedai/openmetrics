package ai.promoted.metrics.logprocessor.job.join;

import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.TinyCommonInfo;
import ai.promoted.proto.event.TinyImpression;
import org.apache.flink.api.common.functions.MapFunction;

final class ToTinyImpression implements MapFunction<Impression, TinyImpression> {
  @Override
  public TinyImpression map(Impression impression) {
    return TinyImpression.newBuilder()
        .setCommon(
            TinyCommonInfo.newBuilder()
                .setPlatformId(impression.getPlatformId())
                .setAnonUserId(impression.getUserInfo().getAnonUserId())
                .setEventApiTimestamp(impression.getTiming().getEventApiTimestamp()))
        .setViewId(impression.getViewId())
        .setRequestId(impression.getRequestId())
        .setInsertionId(impression.getInsertionId())
        .setImpressionId(impression.getImpressionId())
        .setContentId(impression.getContentId())
        .build();
  }
}
