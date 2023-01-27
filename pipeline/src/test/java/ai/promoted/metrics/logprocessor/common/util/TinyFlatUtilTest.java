package ai.promoted.metrics.logprocessor.common.util;

import ai.promoted.metrics.error.MismatchError;
import ai.promoted.proto.event.TinyDeliveryLog;
import ai.promoted.proto.event.TinyEvent;
import com.google.common.collect.ImmutableSet;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TinyFlatUtilTest {

    private BiConsumer<OutputTag<MismatchError>, MismatchError> noopErrorLogger = (tag, error) -> {};

    @Test
    public void mergeTinyEvent_mergeImpression_noTimestamp() {
        TinyEvent.Builder builder = TinyFlatUtil.mergeImpression(
                TinyEvent.newBuilder()
                        .setViewId("view1")
                        .setRequestId("request1")
                        .setInsertionId("insertion1")
                        .setContentId("content1"),
                TinyEvent.newBuilder()
                        .setImpressionId("impression1")
                        .build(),
                noopErrorLogger);

        assertEquals(
                TinyEvent.newBuilder()
                        .setViewId("view1")
                        .setRequestId("request1")
                        .setInsertionId("insertion1")
                        .setContentId("content1")
                        .setImpressionId("impression1")
                        .build(),
                builder.build());
    }

    @Test
    public void mergeTinyEvent_mergeImpression_withTimestamp() {
        TinyEvent.Builder builder = TinyFlatUtil.mergeImpression(
                TinyEvent.newBuilder()
                        .setViewId("view1")
                        .setRequestId("request1")
                        .setInsertionId("insertion1")
                        .setContentId("content1")
                        .setLogTimestamp(1L),
                TinyEvent.newBuilder()
                        .setImpressionId("impression1")
                        .setLogTimestamp(2L)
                        .build(),
                noopErrorLogger);

        assertEquals(
                TinyEvent.newBuilder()
                        .setViewId("view1")
                        .setRequestId("request1")
                        .setInsertionId("insertion1")
                        .setContentId("content1")
                        .setImpressionId("impression1")
                        .setLogTimestamp(2L)
                        .build(),
                builder.build());
    }

    @Test
    public void mergeTinyEvent_mergeAction_withoutTimestamps() {
        TinyEvent.Builder builder = TinyFlatUtil.mergeAction(
                TinyEvent.newBuilder()
                        .setViewId("view1")
                        .setRequestId("request1")
                        .setInsertionId("insertion1")
                        .setContentId("content1")
                        .setImpressionId("impression1"),
                TinyEvent.newBuilder()
                        .setActionId("action1")
                        .build(),
                noopErrorLogger);

        assertEquals(
                TinyEvent.newBuilder()
                        .setViewId("view1")
                        .setRequestId("request1")
                        .setInsertionId("insertion1")
                        .setContentId("content1")
                        .setImpressionId("impression1")
                        .setActionId("action1")
                        .build(),
                builder.build());
    }

    @Test
    public void mergeTinyEvent_mergeAction_withTimestamps() {
        TinyEvent.Builder builder = TinyFlatUtil.mergeAction(
                TinyEvent.newBuilder()
                        .setViewId("view1")
                        .setRequestId("request1")
                        .setInsertionId("insertion1")
                        .setContentId("content1")
                        .setImpressionId("impression1")
                        .setLogTimestamp(1L),
                TinyEvent.newBuilder()
                        .setActionId("action1")
                        .setLogTimestamp(2L)
                        .build(),
                noopErrorLogger);

        assertEquals(
                TinyEvent.newBuilder()
                        .setViewId("view1")
                        .setRequestId("request1")
                        .setInsertionId("insertion1")
                        .setContentId("content1")
                        .setImpressionId("impression1")
                        .setActionId("action1")
                        .setLogTimestamp(2L)
                        .build(),
                builder.build());
    }

    @Test
    public void mergeTinyEvent_mergeImpression_otherContentIds() {
        TinyEvent.Builder builder = TinyFlatUtil.mergeImpression(
                TinyEvent.newBuilder()
                        .setViewId("view1")
                        .setRequestId("request1")
                        .setInsertionId("insertion1")
                        .setContentId("content1")
                        .putOtherContentIds(StringUtil.hash("ownerId"), "value1"),
                TinyEvent.newBuilder()
                        .setImpressionId("impression1")
                        .putOtherContentIds(StringUtil.hash("storeId"), "value2")
                        .build(),
                noopErrorLogger);

        assertEquals(
                TinyEvent.newBuilder()
                        .setViewId("view1")
                        .setRequestId("request1")
                        .setInsertionId("insertion1")
                        .setContentId("content1")
                        .setImpressionId("impression1")
                        .putOtherContentIds(StringUtil.hash("ownerId"), "value1")
                        .putOtherContentIds(StringUtil.hash("storeId"), "value2")
                        .build(),
                builder.build());
    }

    @Test
    public void createTinyFlatResponseInsertions_withoutTimestamps() {
        List<TinyEvent.Builder> builders = TinyFlatUtil.createTinyFlatResponseInsertions(
                TinyEvent.newBuilder()
                        .setPlatformId(1L)
                        .setLogUserId("logUser1")
                        .setViewId("view1"),
                TinyDeliveryLog.newBuilder()
                        .setViewId("view1")
                        .setRequestId("request1")
                        .addResponseInsertion(TinyDeliveryLog.TinyInsertion.newBuilder()
                                .setInsertionId("insertion1")
                                .setContentId("content1"))
                        .addResponseInsertion(TinyDeliveryLog.TinyInsertion.newBuilder()
                                .setInsertionId("insertion2")
                                .setContentId("content2"))
                        .build(),
                noopErrorLogger);

        assertEquals(2, builders.size());
        assertEquals(
                TinyEvent.newBuilder()
                        .setPlatformId(1L)
                        .setLogUserId("logUser1")
                        .setViewId("view1")
                        .setRequestId("request1")
                        .setInsertionId("insertion1")
                        .setContentId("content1")
                        .build(),
                builders.get(0).build());
        assertEquals(
                TinyEvent.newBuilder()
                        .setPlatformId(1L)
                        .setLogUserId("logUser1")
                        .setViewId("view1")
                        .setRequestId("request1")
                        .setInsertionId("insertion2")
                        .setContentId("content2")
                        .build(),
                builders.get(1).build());
    }

    @Test
    public void createTinyFlatResponseInsertions_withoutTimestamps_noLhs() {
        List<TinyEvent.Builder> builders = TinyFlatUtil.createTinyFlatResponseInsertions(
                TinyEvent.newBuilder(),
                TinyDeliveryLog.newBuilder()
                        .setPlatformId(1L)
                        .setLogUserId("logUser1")
                        .setViewId("view1")
                        .setRequestId("request1")
                        .addResponseInsertion(TinyDeliveryLog.TinyInsertion.newBuilder()
                                .setInsertionId("insertion1")
                                .setContentId("content1"))
                        .addResponseInsertion(TinyDeliveryLog.TinyInsertion.newBuilder()
                                .setInsertionId("insertion2")
                                .setContentId("content2"))
                        .build(),
                noopErrorLogger);

        assertEquals(2, builders.size());
        assertEquals(
                TinyEvent.newBuilder()
                        .setPlatformId(1L)
                        .setLogUserId("logUser1")
                        .setViewId("view1")
                        .setRequestId("request1")
                        .setInsertionId("insertion1")
                        .setContentId("content1")
                        .build(),
                builders.get(0).build());
        assertEquals(
                TinyEvent.newBuilder()
                        .setPlatformId(1L)
                        .setLogUserId("logUser1")
                        .setViewId("view1")
                        .setRequestId("request1")
                        .setInsertionId("insertion2")
                        .setContentId("content2")
                        .build(),
                builders.get(1).build());
    }

    @Test
    public void createTinyFlatResponseInsertions_withTimestamps() {
        List<TinyEvent.Builder> builders = TinyFlatUtil.createTinyFlatResponseInsertions(
                TinyEvent.newBuilder()
                        .setPlatformId(1L)
                        .setLogUserId("logUser1")
                        .setViewId("view1")
                        .setLogTimestamp(1L),
                TinyDeliveryLog.newBuilder()
                        .setViewId("view1")
                        .setRequestId("request1")
                        .addResponseInsertion(TinyDeliveryLog.TinyInsertion.newBuilder()
                                .setInsertionId("insertion1")
                                .setContentId("content1"))
                        .addResponseInsertion(TinyDeliveryLog.TinyInsertion.newBuilder()
                                .setInsertionId("insertion2")
                                .setContentId("content2"))
                        .setLogTimestamp(2L)
                        .build(),
                noopErrorLogger);

        assertEquals(2, builders.size());
        assertEquals(
                TinyEvent.newBuilder()
                        .setPlatformId(1L)
                        .setLogUserId("logUser1")
                        .setViewId("view1")
                        .setRequestId("request1")
                        .setInsertionId("insertion1")
                        .setContentId("content1")
                        .setLogTimestamp(2L)
                        .build(),
                builders.get(0).build());
        assertEquals(
                TinyEvent.newBuilder()
                        .setPlatformId(1L)
                        .setLogUserId("logUser1")
                        .setViewId("view1")
                        .setRequestId("request1")
                        .setInsertionId("insertion2")
                        .setContentId("content2")
                        .setLogTimestamp(2L)
                        .build(),
                builders.get(1).build());
    }


    @Test
    public void createTinyFlatResponseInsertions_withTimestamps_noLhs() {
        List<TinyEvent.Builder> builders = TinyFlatUtil.createTinyFlatResponseInsertions(
                TinyEvent.newBuilder(),
                TinyDeliveryLog.newBuilder()
                        .setPlatformId(1L)
                        .setLogUserId("logUser1")
                        .setViewId("view1")
                        .setRequestId("request1")
                        .addResponseInsertion(TinyDeliveryLog.TinyInsertion.newBuilder()
                                .setInsertionId("insertion1")
                                .setContentId("content1"))
                        .addResponseInsertion(TinyDeliveryLog.TinyInsertion.newBuilder()
                                .setInsertionId("insertion2")
                                .setContentId("content2"))
                        .setLogTimestamp(2L)
                        .build(),
                noopErrorLogger);

        assertEquals(2, builders.size());
        assertEquals(
                TinyEvent.newBuilder()
                        .setPlatformId(1L)
                        .setLogUserId("logUser1")
                        .setViewId("view1")
                        .setRequestId("request1")
                        .setInsertionId("insertion1")
                        .setContentId("content1")
                        .setLogTimestamp(2L)
                        .build(),
                builders.get(0).build());
        assertEquals(
                TinyEvent.newBuilder()
                        .setPlatformId(1L)
                        .setLogUserId("logUser1")
                        .setViewId("view1")
                        .setRequestId("request1")
                        .setInsertionId("insertion2")
                        .setContentId("content2")
                        .setLogTimestamp(2L)
                        .build(),
                builders.get(1).build());
    }

    @Test
    public void getAllContentIds() {
        assertEquals(ImmutableSet.of(), TinyFlatUtil.getAllContentIds(TinyEvent.getDefaultInstance()));
        assertEquals(
                ImmutableSet.of("content1"),
                TinyFlatUtil.getAllContentIds(TinyEvent.newBuilder().setContentId("content1").build()));
        assertEquals(
                ImmutableSet.of("value1", "value2"),
                TinyFlatUtil.getAllContentIds(
                        TinyEvent.newBuilder()
                                .putOtherContentIds(StringUtil.hash("ownerId"), "value1")
                                .putOtherContentIds(StringUtil.hash("storeId"), "value2")
                                .build()));
        assertEquals(
                ImmutableSet.of("content1", "value1", "value2"),
                TinyFlatUtil.getAllContentIds(
                        TinyEvent.newBuilder()
                                .setContentId("content1")
                                .putOtherContentIds(StringUtil.hash("ownerId"), "value1")
                                .putOtherContentIds(StringUtil.hash("storeId"), "value2")
                                .build()));
    }
}
