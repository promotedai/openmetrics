package ai.promoted.metrics.logprocessor.common.util;

import ai.promoted.metrics.common.Field;
import ai.promoted.metrics.common.JoinedIdentifiers;
import ai.promoted.metrics.common.RecordType;
import ai.promoted.metrics.error.LogFunctionName;
import ai.promoted.metrics.error.MismatchError;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.TinyAction;
import ai.promoted.proto.event.TinyCommonInfo;
import ai.promoted.proto.event.TinyDeliveryLog;
import ai.promoted.proto.event.TinyInsertion;
import ai.promoted.proto.event.TinyJoinedImpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.function.Consumer;

/** Utilities for TinyEvent. */
public final class TinyFlatUtil {
  private TinyFlatUtil() {}

  // TODO - move a bunch of these to a different class.
  public static String getViewId(TinyJoinedImpression joinedImpression) {
    String viewId = joinedImpression.getInsertion().getViewId();
    if (!viewId.isEmpty()) {
      return viewId;
    }
    return joinedImpression.getImpression().getViewId();
  }

  public static String getRequestId(TinyJoinedImpression joinedImpression) {
    String requestId = joinedImpression.getInsertion().getRequestId();
    if (!requestId.isEmpty()) {
      return requestId;
    }
    return joinedImpression.getImpression().getRequestId();
  }

  public static String getResponseInsertionId(TinyJoinedImpression joinedImpression) {
    String insertionId = joinedImpression.getInsertion().getCore().getInsertionId();
    if (!insertionId.isEmpty()) {
      return insertionId;
    }
    return joinedImpression.getImpression().getInsertionId();
  }

  public static List<TinyInsertion.Builder> createTinyFlatResponseInsertions(
      TinyInsertion.Builder baseBuilder, TinyDeliveryLog deliveryLog) {
    // TODO - remove MismatchError logic.
    Consumer<MismatchError> errorLogger = (ignored) -> {};
    // In case the code is doing a right outer join, we need to try setting the parent fields that
    // would
    // normally be set on viewBuilder.
    JoinValueSetter setter =
        new JoinValueSetter(
            JoinValueSetterOptions.builder()
                .setRecordType(RecordType.DELIVERY_LOG)
                .setRecordId(deliveryLog.getRequestId())
                .setEventApiTimestamp(deliveryLog.getCommon().getEventApiTimestamp())
                .setLhsIds(
                    JoinedIdentifiers.newBuilder()
                        .setPlatformId(baseBuilder.getCommon().getPlatformId())
                        .setAnonUserId(baseBuilder.getCommon().getAnonUserId())
                        .setViewId(baseBuilder.getViewId())
                        .build())
                .setLogFunctionName(
                    LogFunctionName.TINY_FLAT_UTIL_CREATE_TINY_FLAT_RESPONSE_INSERTIONS)
                .setErrorLogger(errorLogger)
                .build());

    // TODO - refactor this.
    setter.setValue(
        Field.PLATFORM_ID,
        baseBuilder.getCommonBuilder()::setPlatformId,
        baseBuilder.getCommon().getPlatformId(),
        deliveryLog.getCommon().getPlatformId());
    setter.setValue(
        Field.LOG_USER_ID,
        baseBuilder.getCommonBuilder()::setAnonUserId,
        baseBuilder.getCommon().getAnonUserId(),
        deliveryLog.getCommon().getAnonUserId());
    setter.setValue(
        Field.VIEW_ID, baseBuilder::setViewId, baseBuilder.getViewId(), deliveryLog.getViewId());
    baseBuilder.setRequestId(deliveryLog.getRequestId()).setPagingId(deliveryLog.getPagingId());
    if (deliveryLog.getCommon().getEventApiTimestamp() != 0L) {
      baseBuilder
          .getCommonBuilder()
          .setEventApiTimestamp(deliveryLog.getCommon().getEventApiTimestamp());
    }
    final TinyInsertion.Builder finalBaseBuilder = baseBuilder;
    List<TinyInsertion.Builder> result =
        deliveryLog.getResponseInsertionList().stream()
            .map(responseInsertion -> finalBaseBuilder.clone().setCore(responseInsertion))
            .collect(ImmutableList.toImmutableList());
    return result;
  }

  /** Returns a combined set of contentId and otherContentIds. */
  public static ImmutableSet<String> getAllContentIds(TinyInsertion insertion) {
    ImmutableSet.Builder<String> builder =
        ImmutableSet.builderWithExpectedSize(1 + insertion.getCore().getOtherContentIdsCount());
    if (!insertion.getCore().getContentId().isEmpty()) {
      builder.add(insertion.getCore().getContentId());
    }
    for (String otherContentId : insertion.getCore().getOtherContentIdsMap().values()) {
      if (!otherContentId.isEmpty()) {
        builder.add(otherContentId);
      }
    }
    return builder.build();
  }

  /** Returns a combined set of contentId and otherContentIds. */
  public static ImmutableSet<String> getAllContentIds(TinyJoinedImpression impression) {
    // TODO - should we also add the Impression.content_id too?
    return getAllContentIds(impression.getInsertion());
  }

  /** Returns a combined set of contentId and otherContentIds. */
  public static ImmutableSet<String> getAllContentIds(TinyAction action) {
    ImmutableSet.Builder<String> builder =
        ImmutableSet.builderWithExpectedSize(1 + action.getOtherContentIdsCount());
    if (!action.getContentId().isEmpty()) {
      builder.add(action.getContentId());
    }
    for (String otherContentId : action.getOtherContentIdsMap().values()) {
      if (!otherContentId.isEmpty()) {
        builder.add(otherContentId);
      }
    }
    return builder.build();
  }

  public static TinyAction.Builder toTinyActionBuilder(Action action, String contentId) {
    return TinyAction.newBuilder()
        .setCommon(
            TinyCommonInfo.newBuilder()
                .setPlatformId(action.getPlatformId())
                .setAnonUserId(action.getUserInfo().getAnonUserId())
                .setEventApiTimestamp(action.getTiming().getEventApiTimestamp()))
        .setViewId(action.getViewId())
        .setRequestId(action.getRequestId())
        .setInsertionId(action.getInsertionId())
        .setContentId(contentId)
        .setImpressionId(action.getImpressionId())
        .setActionId(action.getActionId())
        .setActionType(action.getActionType())
        .setCustomActionType(action.getCustomActionType());
  }
}
