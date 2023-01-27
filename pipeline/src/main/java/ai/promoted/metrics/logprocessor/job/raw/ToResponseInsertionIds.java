package ai.promoted.metrics.logprocessor.job.raw;

import ai.promoted.metrics.common.ResponseInsertionIds;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.delivery.Response;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Converts DeliveryLog to RequestInsertionIds.
 */
class ToResponseInsertionIds implements FlatMapFunction<DeliveryLog, ResponseInsertionIds> {

    @Override
    public void flatMap(DeliveryLog deliveryLog, Collector<ResponseInsertionIds> collector) {
        Request request = deliveryLog.getRequest();
        long platformId = deliveryLog.getPlatformId();
        long eventApiTimestamp = request.getTiming().getEventApiTimestamp();
        String requestId = request.getRequestId();

        Response response = deliveryLog.getResponse();
        for (Insertion insertion : response.getInsertionList()) {
            collector.collect(ResponseInsertionIds.newBuilder()
                    .setPlatformId(platformId)
                    .setEventApiTimestamp(eventApiTimestamp)
                    .setRequestId(requestId)
                    .setInsertionId(insertion.getInsertionId())
                    .setContentId(insertion.getContentId())
                    .setPosition(insertion.getPosition())
                    .build());
        }
    }
}
