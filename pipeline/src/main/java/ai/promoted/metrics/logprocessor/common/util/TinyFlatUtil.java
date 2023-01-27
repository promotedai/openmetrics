package ai.promoted.metrics.logprocessor.common.util;

import ai.promoted.metrics.common.Field;
import ai.promoted.metrics.common.JoinedIdentifiers;
import ai.promoted.metrics.common.RecordType;
import ai.promoted.metrics.error.LogFunctionName;
import ai.promoted.metrics.error.MismatchError;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.TinyDeliveryLog;
import ai.promoted.proto.event.TinyEvent;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/** Utilities for TinyEvent. */
public final class TinyFlatUtil {
    public static TinyEvent.Builder mergeImpression(TinyEvent.Builder builder, TinyEvent rhs, BiConsumer<OutputTag<MismatchError>, MismatchError> errorLogger) {
        TinyEvent.Builder tinyImpressionBuilder = mergeTinyEvent(
                RecordType.IMPRESSION,
                builder,
                rhs,
                rhs.getImpressionId(),
                LogFunctionName.TINY_FLAT_UTIL_MERGE_IMPRESSION,
                errorLogger);

        // It doesn't really matter if we prefer otherContentIds on Insertion vs Impressions.
        // Insertions are more safe since they are set server-side.
        for (Map.Entry<Integer, String> entry : rhs.getOtherContentIdsMap().entrySet()) {
            if (!builder.getOtherContentIdsMap().containsKey(entry.getKey())) {
                builder.putOtherContentIds(entry.getKey(), entry.getValue());
            }
        }
        return tinyImpressionBuilder;
    }

    public static TinyEvent.Builder mergeAction(
            TinyEvent.Builder builder,
            TinyEvent rhs,
            BiConsumer<OutputTag<MismatchError>, MismatchError> errorLogger) {
        TinyEvent.Builder tinyActionBuilder = mergeTinyEvent(
                RecordType.ACTION,
                builder,
                rhs,
                rhs.getActionId(),
                LogFunctionName.TINY_FLAT_UTIL_MERGE_ACTION,
                errorLogger);

        // When merging tiny events, the LHS values are usually more accurate.  E.g. we usually trust IDs on
        // Request/Insertion over IDs set on Impression.
        //
        // This code overrides the TinyImpression's contentId with the TinyAction's contentId.
        // The Action's contentId can be different for hierarchical content and shopping cart.
        // We need to force override the values.
        //
        // PR - we could split the TinyEvent.content_id field into an impression_content_id and an action_content_id.
        // PR - should I do similar things for merging TinyImpressions onto TinyInsertions?
        if (!rhs.getContentId().isEmpty()) {
            builder.setContentId(rhs.getContentId());
        }
        // Same for otherContentIds.  Prefer the ones on Actions if both are set.
        builder.putAllOtherContentIds(rhs.getOtherContentIdsMap());

        return tinyActionBuilder;
    }

    private static TinyEvent.Builder mergeTinyEvent(
            RecordType recordType,
            TinyEvent.Builder builder,
            TinyEvent rhs,
            String recordId,
            LogFunctionName logFunctionName,
            BiConsumer<OutputTag<MismatchError>, MismatchError> errorLogger) {
        // TODO - rename the methods different for flat joins.

        JoinValueSetter setter = new JoinValueSetter(JoinValueSetterOptions.builder()
                .setRecordType(recordType)
                .setRecordId(recordId)
                .setLogTimestamp(rhs.getLogTimestamp())
                .setLhsIds(JoinedIdentifiers.newBuilder()
                        .setPlatformId(builder.getPlatformId())
                        .setLogUserId(builder.getLogUserId())
                        .setViewId(builder.getViewId())
                        .setAutoViewId(builder.getAutoViewId())
                        .setRequestId(builder.getRequestId())
                        .setInsertionId(builder.getInsertionId())
                        .setImpressionId(builder.getImpressionId())
                        // It's okay that this does not have ActionId.
                        .build())
                .setLogFunctionName(logFunctionName)
                .setErrorLogger(errorLogger)
                .build());

        setter.setValue(Field.PLATFORM_ID, builder::setPlatformId, builder.getPlatformId(), rhs.getPlatformId());
        setter.setValue(Field.LOG_USER_ID, builder::setLogUserId, builder.getLogUserId(), rhs.getLogUserId());
        setter.setValue(Field.VIEW_ID, builder::setViewId, builder.getViewId(), rhs.getViewId());
        setter.setValue(Field.REQUEST_ID, builder::setRequestId, builder.getRequestId(), rhs.getRequestId());
        setter.setValue(Field.INSERTION_ID, builder::setInsertionId, builder.getInsertionId(), rhs.getInsertionId());
        setter.setValue(Field.IMPRESSION_ID, builder::setImpressionId, builder.getImpressionId(), rhs.getImpressionId());
        setter.setValue(Field.ACTION_ID, builder::setActionId, builder.getActionId(), rhs.getActionId());
        if (rhs.getLogTimestamp() != 0L) {
            builder.setLogTimestamp(rhs.getLogTimestamp());
        }
        return builder;
    }

    public static List<TinyEvent.Builder> createTinyFlatResponseInsertions(
            TinyEvent.Builder viewBuilder,
            TinyDeliveryLog deliveryLog,
            BiConsumer<OutputTag<MismatchError>, MismatchError> errorLogger) {
        TinyEvent.Builder baseBuilder = viewBuilder.clone();
        // In case the code is doing a right outer join, we need to try setting the parent fields that would
        // normally be set on viewBuilder.
        JoinValueSetter setter = new JoinValueSetter(JoinValueSetterOptions.builder()
                .setRecordType(RecordType.DELIVERY_LOG)
                .setRecordId(deliveryLog.getRequestId())
                .setLogTimestamp(deliveryLog.getLogTimestamp())
                .setLhsIds(JoinedIdentifiers.newBuilder()
                        .setPlatformId(viewBuilder.getPlatformId())
                        .setLogUserId(viewBuilder.getLogUserId())
                        .setViewId(viewBuilder.getViewId())
                        .setAutoViewId(viewBuilder.getAutoViewId())
                        .build())
                .setLogFunctionName(LogFunctionName.TINY_FLAT_UTIL_CREATE_TINY_FLAT_RESPONSE_INSERTIONS)
                .setErrorLogger(errorLogger)
                .build());

        setter.setValue(Field.PLATFORM_ID, baseBuilder::setPlatformId, baseBuilder.getPlatformId(), deliveryLog.getPlatformId());
        setter.setValue(Field.LOG_USER_ID, baseBuilder::setLogUserId, baseBuilder.getLogUserId(), deliveryLog.getLogUserId());
        setter.setValue(Field.VIEW_ID, baseBuilder::setViewId, baseBuilder.getViewId(), deliveryLog.getViewId());
        baseBuilder.setRequestId(deliveryLog.getRequestId());
        if (deliveryLog.getLogTimestamp() != 0L) {
            baseBuilder.setLogTimestamp(deliveryLog.getLogTimestamp());
        }
        final TinyEvent.Builder finalBaseBuilder = baseBuilder;
        List<TinyEvent.Builder> result = deliveryLog.getResponseInsertionList().stream()
                .map(responseInsertion -> finalBaseBuilder.clone()
                        .setInsertionId(responseInsertion.getInsertionId())
                        .setContentId(responseInsertion.getContentId()))
                .collect(ImmutableList.toImmutableList());
        return result;
    }

    /**
     * Returns a combined list of contentId and otherContentIds.
     */
    public static ImmutableSet<String> getAllContentIds(TinyEvent event) {
        ImmutableSet.Builder<String> builder = ImmutableSet.builderWithExpectedSize(1 + event.getOtherContentIdsCount());
        if (!event.getContentId().isEmpty()) {
            builder.add(event.getContentId());
        }
        return builder.addAll(event.getOtherContentIdsMap().values()).build();
    }

    public static TinyEvent.Builder toTinyActionBuilder(Action action, String contentId) {
        return TinyEvent.newBuilder()
                .setPlatformId(action.getPlatformId())
                .setLogUserId(action.getUserInfo().getLogUserId())
                .setLogTimestamp(action.getTiming().getLogTimestamp())
                .setViewId(action.getViewId())
                .setRequestId(action.getRequestId())
                .setInsertionId(action.getInsertionId())
                .setContentId(contentId)
                .setImpressionId(action.getImpressionId())
                .setActionId(action.getActionId());
    }

    private TinyFlatUtil() {};
}
