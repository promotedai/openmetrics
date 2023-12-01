package ai.promoted.metrics.logprocessor.job.join;

import ai.promoted.metrics.logprocessor.common.functions.inferred.MergeImpressionDetails;
import ai.promoted.metrics.logprocessor.common.util.DeliveryLogUtil;
import ai.promoted.proto.common.Properties;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.event.CombinedDeliveryLog;
import ai.promoted.proto.event.TinyCommonInfo;
import ai.promoted.proto.event.TinyDeliveryLog;
import ai.promoted.proto.event.TinyInsertionCore;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

final class ToTinyDeliveryLog implements MapFunction<CombinedDeliveryLog, TinyDeliveryLog> {
  private static final Logger LOGGER = LogManager.getLogger(MergeImpressionDetails.class);

  private final OtherContentIdsConverter otherContentIdsConverter;

  // TODO - use ImmutableList.
  ToTinyDeliveryLog(List<String> requestInsertionOtherContentIdKeys) {
    this.otherContentIdsConverter =
        new OtherContentIdsConverter(requestInsertionOtherContentIdKeys);
  }

  @Override
  public TinyDeliveryLog map(CombinedDeliveryLog combinedDeliveryLog) {
    DeliveryLog deliveryLog = DeliveryLogUtil.getDeliveryLog(combinedDeliveryLog);
    Request request = deliveryLog.getRequest();
    TinyDeliveryLog.Builder builder =
        TinyDeliveryLog.newBuilder()
            .setCommon(
                TinyCommonInfo.newBuilder()
                    .setPlatformId(DeliveryLogUtil.getPlatformId(combinedDeliveryLog))
                    .setAnonUserId(request.getUserInfo().getAnonUserId())
                    .setEventApiTimestamp(request.getTiming().getEventApiTimestamp()))
            .setViewId(request.getViewId())
            .setRequestId(request.getRequestId())
            .setPagingId(deliveryLog.getResponse().getPagingInfo().getPagingId());

    // OPTIMIZATION: keep empty when we don't need to look up Request Insertions.
    Map<String, Insertion> contentIdToRequestInsertion;
    Map<Integer, String> requestOtherContentIds;
    if (otherContentIdsConverter.hasKeys()) {
      contentIdToRequestInsertion = getContentIdToRequestInsertion(request);
      requestOtherContentIds = new HashMap<>();
      if (request.hasProperties()) {
        otherContentIdsConverter.putFromProperties(
            requestOtherContentIds::put, request.getProperties());
      }
    } else {
      contentIdToRequestInsertion = ImmutableMap.of();
      requestOtherContentIds = ImmutableMap.of();
    }

    deliveryLog.getResponse().getInsertionList().stream()
        .forEach(
            responseInsertion -> {
              TinyInsertionCore.Builder tinyInsertion =
                  builder
                      .addResponseInsertionBuilder()
                      .setInsertionId(responseInsertion.getInsertionId())
                      .setContentId(responseInsertion.getContentId())
                      .setPosition(responseInsertion.getPosition());
              if (otherContentIdsConverter.hasKeys()) {
                // RequestInsertions properties are higher priority than Request properties.
                tinyInsertion.putAllOtherContentIds(requestOtherContentIds);
                Insertion requestInsertion =
                    contentIdToRequestInsertion.get(responseInsertion.getContentId());
                if (request != null && request.hasProperties()) {
                  addOtherContentIdsFromProperties(tinyInsertion, request.getProperties());
                }
                // Put after so Request.insertion.properties override Request.properties.
                if (requestInsertion != null && requestInsertion.hasProperties()) {
                  addOtherContentIdsFromProperties(tinyInsertion, requestInsertion.getProperties());
                }
              }
            });

    return builder.build();
  }

  // For now, we only support 1 level.
  private void addOtherContentIdsFromProperties(
      TinyInsertionCore.Builder tinyInsertion, Properties properties) {
    otherContentIdsConverter.putFromProperties(tinyInsertion::putOtherContentIds, properties);
  }

  private Map<String, Insertion> getContentIdToRequestInsertion(Request request) {
    return request.getInsertionList().stream()
        .collect(
            Collectors.toMap(
                Insertion::getContentId,
                Function.identity(),
                (insertion1, insertion2) -> {
                  // If we see duplicate contentIds, we need to verify how duplicate contentIds
                  // work.
                  LOGGER.warn(
                      "Multiple request insertions with the same contentId, {}, found on Request={}",
                      insertion1.getContentId(),
                      request);
                  // Just take the first insertion for now.
                  return insertion1;
                }));
  }
}
