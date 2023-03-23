package ai.promoted.metrics.logprocessor.job.raw;

import ai.promoted.metrics.common.RequestInsertionIds;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.delivery.Request;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/** Converts DeliveryLog to RequestInsertionIds. */
class ToRequestInsertionIds implements FlatMapFunction<DeliveryLog, RequestInsertionIds> {

  @Override
  public void flatMap(DeliveryLog deliveryLog, Collector<RequestInsertionIds> collector) {
    Request request = deliveryLog.getRequest();
    long platformId = deliveryLog.getPlatformId();
    long eventApiTimestamp = request.getTiming().getEventApiTimestamp();
    String requestId = request.getRequestId();

    for (Insertion insertion : request.getInsertionList()) {
      collector.collect(
          RequestInsertionIds.newBuilder()
              .setPlatformId(platformId)
              .setEventApiTimestamp(eventApiTimestamp)
              .setRequestId(requestId)
              .setContentId(insertion.getContentId())
              .setRetrievalRank(insertion.hasRetrievalRank() ? insertion.getRetrievalRank() : null)
              .build());
    }
  }
}
