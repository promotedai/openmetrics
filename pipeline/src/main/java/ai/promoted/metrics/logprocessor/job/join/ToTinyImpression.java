package ai.promoted.metrics.logprocessor.job.join;

import ai.promoted.metrics.logprocessor.common.functions.inferred.MergeImpressionDetails;
import ai.promoted.metrics.logprocessor.common.util.DeliveryLogUtil;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.event.CombinedDeliveryLog;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.TinyDeliveryLog;
import ai.promoted.proto.event.TinyEvent;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

final class ToTinyImpression implements MapFunction<Impression, TinyEvent> {
    private final OtherContentIdsConverter otherContentIdsConverter;

    ToTinyImpression(List<String> requestInsertionOtherContentIdKeys) {
        this.otherContentIdsConverter = new OtherContentIdsConverter(requestInsertionOtherContentIdKeys);
    }

    @Override
    public TinyEvent map(Impression impression) {
        TinyEvent.Builder tinyImpression = TinyEvent.newBuilder()
                .setPlatformId(impression.getPlatformId())
                .setLogUserId(impression.getUserInfo().getLogUserId())
                .setLogTimestamp(impression.getTiming().getLogTimestamp())
                .setViewId(impression.getViewId())
                .setRequestId(impression.getRequestId())
                .setInsertionId(impression.getInsertionId())
                .setContentId(impression.getContentId())
                .setImpressionId(impression.getImpressionId());

        // We don't expect customers to see this.  Customers should stick these properties onto RequestInsertions.
        if (otherContentIdsConverter.hasKeys() && impression.hasProperties()) {
            otherContentIdsConverter.putFromProperties(tinyImpression::putOtherContentIds, impression.getProperties());
        }
        return tinyImpression.build();
    }
}
