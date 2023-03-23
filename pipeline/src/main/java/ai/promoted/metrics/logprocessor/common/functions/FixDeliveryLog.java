package ai.promoted.metrics.logprocessor.common.functions;

import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.ExecutionServer;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.delivery.Response;
import java.util.List;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Fixes DeliveryLog.
 *
 * <p>Older DeliveryLogs do not have insertionId set on Response Insertions. This class creates
 * insertion IDs, if they do not exist, from "{requestId}-{position}-{contentId}". We can remove
 * this function after we launch and fix and wait enough time such that we won't want to backfill
 * over this range.
 *
 * <p>TODO(PRO-857): remove this hack.
 */
public class FixDeliveryLog implements MapFunction<DeliveryLog, DeliveryLog> {

  @Override
  public DeliveryLog map(DeliveryLog deliveryLog) throws Exception {
    // Optimization - avoid unnecessarily rebuilding deliveryLog.
    DeliveryLog.Builder deliveryLogBuilder = null;
    if (hasMissingResponseInsertionIds(deliveryLog)) {
      deliveryLogBuilder = fillInMissingResponseInsertionIds(deliveryLog.toBuilder());
    }
    if (isV1OptimizedSdkDeliveryLog(deliveryLog)) {
      if (deliveryLogBuilder == null) {
        deliveryLogBuilder = deliveryLog.toBuilder();
      }
      deliveryLogBuilder = convertFromV1OptimizedSdkDeliveryLog(deliveryLogBuilder);
    }

    if (deliveryLogBuilder == null) {
      return deliveryLog;
    } else {
      return deliveryLogBuilder.build();
    }
  }

  private static boolean hasMissingResponseInsertionIds(DeliveryLog deliveryLog) {
    return deliveryLog.getResponse().getInsertionList().stream()
        .anyMatch(insertion -> insertion.getInsertionId().isEmpty());
  }

  private static DeliveryLog.Builder fillInMissingResponseInsertionIds(
      DeliveryLog.Builder builder) {
    String requestId = builder.getRequest().getRequestId();
    Response.Builder responseBuilder = builder.getResponseBuilder();
    for (Insertion.Builder responseInsertionBuilder : responseBuilder.getInsertionBuilderList()) {
      if (responseInsertionBuilder.getInsertionId().isEmpty()) {
        String contentId = responseInsertionBuilder.getContentId();
        long position = responseInsertionBuilder.getPosition();
        // TODO - we should generate a valid UUID so it's easier to shift to a byte array.
        responseInsertionBuilder.setInsertionId(requestId + "-" + position + "-" + contentId);
      }
    }
    return builder;
  }

  /**
   * Returns if {@param deliveryLog} is the v1 optimized SDK DeliveryLog format. The v1 optimized
   * format saved request and response insertions as response insertions with extra properties. This
   * was originally done to save space since we were going to pass CMS data through Insertions.
   * We'll change this because (1) we eventually need to support no request insertions (Promoted
   * retrieval) and (2) this format leads to more bugs.
   *
   * <p>Reminder that the SDK DeliveryLog doesn't reorder the request insertions.
   */
  private static boolean isV1OptimizedSdkDeliveryLog(DeliveryLog deliveryLog) {
    return deliveryLog.getExecution().getExecutionServer() == ExecutionServer.SDK
        && deliveryLog.getRequest().getInsertionCount() == 0
        && deliveryLog.getResponse().getInsertionCount() != 0;
  }

  private static DeliveryLog.Builder convertFromV1OptimizedSdkDeliveryLog(
      DeliveryLog.Builder builder) {
    // Copy the merged response insertions to request insertions.  Strip out position since that's
    // the resposne
    // insertion field.
    Request.Builder requestBuilder = builder.getRequestBuilder();
    Response response = builder.getResponse();
    List<Insertion> responseInsertions = response.getInsertionList();
    for (Insertion responseInsertion : responseInsertions) {
      requestBuilder.addInsertion(responseInsertion.toBuilder().clearPosition());
    }

    // Recreate response insertions with only a small set of fields.
    Response.Builder responseBuilder = builder.getResponseBuilder();
    responseBuilder.clearInsertion();
    for (Insertion responseInsertion : responseInsertions) {
      responseBuilder.addInsertion(
          Insertion.newBuilder()
              .setInsertionId(responseInsertion.getInsertionId())
              .setContentId(responseInsertion.getContentId())
              .setPosition(responseInsertion.getPosition()));
    }
    return builder;
  }
}
