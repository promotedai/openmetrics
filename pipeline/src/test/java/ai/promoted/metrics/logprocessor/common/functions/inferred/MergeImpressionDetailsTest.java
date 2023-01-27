package ai.promoted.metrics.logprocessor.common.functions.inferred;

import ai.promoted.metrics.logprocessor.common.functions.KeyUtil;
import ai.promoted.metrics.logprocessor.common.functions.inferred.AbstractMergeDetails.MissingEvent;
import ai.promoted.metrics.logprocessor.common.util.DebugIds;
import ai.promoted.proto.common.ClientInfo;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.delivery.Response;
import ai.promoted.proto.event.CombinedDeliveryLog;
import ai.promoted.proto.event.DroppedMergeDetailsEvent;
import ai.promoted.proto.event.JoinedEvent;
import ai.promoted.proto.event.HiddenApiRequest;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.JoinedIdentifiers;
import ai.promoted.proto.event.TinyDeliveryLog;
import ai.promoted.proto.event.TinyEvent;
import ai.promoted.proto.event.UnionEvent;
import ai.promoted.proto.event.View;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.twitter.chill.protobuf.ProtobufSerializer;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.EnumSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests mainly for the underlying BaseInferred join implementation. */
public class MergeImpressionDetailsTest {
    private static final View VIEW1 = createView("view1");
    private static final View VIEW2 = createView("view2");

    private static final CombinedDeliveryLog DELIVERYLOG1 = CombinedDeliveryLog.newBuilder()
            .setSdk(createDeliveryLog(
                    "request1", "view1",
                    ImmutableList.of(
                            createInsertion("insertion1", "content1"),
                            createInsertion("insertion2", "content2"))))
            .setApi(createDeliveryLog(
                    "request3", "view1",
                    ImmutableList.of(
                            createInsertion("insertion5", "content1"),
                            createInsertion("insertion6", "content2"))))
            .build();
    private static final CombinedDeliveryLog DELIVERYLOG2 = CombinedDeliveryLog.newBuilder()
            .setSdk(createDeliveryLog(
                    "request2", "view2",
                    ImmutableList.of(
                            createInsertion("insertion3", "content3"),
                            createInsertion("insertion4", "content4"))))
            .build();

    private static final Impression IMPRESSION1 = createImpression("impression1").toBuilder().setInsertionId("insertion1").build();

    private KeyedTwoInputStreamOperatorTestHarness<Tuple2<Long, String>, UnionEvent, TinyEvent, JoinedEvent> harness;
    private MergeImpressionDetails function;

    @BeforeEach
    public void setUp() throws Exception {
        setUpHarness(false);
    }

    /** Separate setup method so we can override. */
    private void setUpHarness(boolean skipViewJoin) throws Exception {
        if (harness != null) {
            harness.close();
        }
        function = new MergeImpressionDetails(
                Duration.ofMillis(400),
                Duration.ofMillis(300),
                Duration.ofMillis(200),
                Duration.ofMillis(500),
                Duration.ofMillis(50),
                skipViewJoin,
                0L,
                DebugIds.empty());
        harness = new KeyedTwoInputStreamOperatorTestHarness<>(
            new KeyedCoProcessOperator<>(function),
            KeyUtil.unionEntityKeySelector,
            KeyUtil.TinyEventLogUserIdKey,
            Types.TUPLE(Types.LONG, Types.STRING));
        ExecutionConfig config = harness.getExecutionConfig();
        config.registerTypeWithKryoSerializer(TinyEvent.class, ProtobufSerializer.class);
        config.registerTypeWithKryoSerializer(TinyDeliveryLog.class, ProtobufSerializer.class);
        harness.setup();
        harness.open();
    }

    @AfterEach
    public void tearDown() throws Exception {
        harness.close();
        harness = null;
    }

    // TODO - add other unused inputs.

    @Test
    public void fullImpression() throws Exception {
        harness.processElement1(toUnionEvent(VIEW1, 100), 100);
        harness.processElement1(toUnionEvent(DELIVERYLOG1, 150), 150);
        harness.processElement1(toUnionEvent(IMPRESSION1, 200), 200);

        harness.processBothWatermarks(new Watermark(300));

        harness.processElement2(createInputTinyFlatImpression1(), 300);

        harness.processBothWatermarks(new Watermark(350));

        assertEmptyIncompleteEventStates();
        assertEquals(1, Iterables.size(function.viewMerger.idToView.entries()));
        assertEquals(1, Iterables.size(function.deliveryLogMerger.idToCombinedDeliveryLog.entries()));
        assertEquals(2, Iterables.size(function.deliveryLogMerger.idToInsertion.entries()));
        assertEquals(1, Iterables.size(function.impressionMerger.idToImpression.entries()));

        assertIterableEquals(
                ImmutableList.of(new StreamRecord(createExpectedJoinedImpression(), 300)),
                harness.extractOutputStreamRecords());

        harness.processBothWatermarks(new Watermark(1500));
        assertEmptyStates();
    }

    @Test
    public void fullImpression_outOfOrder() throws Exception {
        harness.processElement2(createInputTinyFlatImpression1(), 100);

        assertEquals(1, Iterables.size(function.timeToIncompleteEvents.entries()));
        assertEquals(1, Iterables.size(function.viewMerger.viewIdToIncompleteEventTimers.entries()));
        assertEquals(1, Iterables.size(function.deliveryLogMerger.requestIdToIncompleteEventTimers.entries()));
        assertEquals(1, Iterables.size(function.impressionMerger.impressionIdToIncompleteEventTimers.entries()));
        assertEquals(0, Iterables.size(function.viewMerger.idToView.entries()));
        assertEquals(0, Iterables.size(function.deliveryLogMerger.idToCombinedDeliveryLog.entries()));
        assertEquals(0, Iterables.size(function.deliveryLogMerger.idToInsertion.entries()));
        assertEquals(0, Iterables.size(function.impressionMerger.idToImpression.entries()));

        harness.processElement1(toUnionEvent(VIEW1, 100), 100);
        harness.processElement1(toUnionEvent(DELIVERYLOG1, 150), 150);
        harness.processElement1(toUnionEvent(IMPRESSION1, 200), 200);

        harness.processBothWatermarks(new Watermark(250));

        assertEquals(0, Iterables.size(function.timeToIncompleteEvents.entries()));
        assertEquals(1, Iterables.size(function.viewMerger.viewIdToIncompleteEventTimers.entries()));
        assertEquals(1, Iterables.size(function.deliveryLogMerger.requestIdToIncompleteEventTimers.entries()));
        assertEquals(0, Iterables.size(function.impressionMerger.impressionIdToIncompleteEventTimers.entries()));
        assertEquals(1, Iterables.size(function.viewMerger.idToView.entries()));
        assertEquals(1, Iterables.size(function.deliveryLogMerger.idToCombinedDeliveryLog.entries()));
        assertEquals(2, Iterables.size(function.deliveryLogMerger.idToInsertion.entries()));
        assertEquals(1, Iterables.size(function.impressionMerger.idToImpression.entries()));

        assertIterableEquals(
                ImmutableList.of(new StreamRecord(createExpectedJoinedImpression(), 200)),
                harness.extractOutputStreamRecords());

        harness.processBothWatermarks(new Watermark(1500));
        assertEmptyStates();
    }

    @Test
    public void fullImpression_lateDeliveryLog() throws Exception {
        harness.processElement1(toUnionEvent(VIEW1, 100), 100);
        harness.processElement1(toUnionEvent(IMPRESSION1, 200), 200);

        harness.processElement2(createInputTinyFlatImpression1(), 100);

        assertEquals(1, Iterables.size(function.timeToIncompleteEvents.entries()));
        assertEquals(0, Iterables.size(function.viewMerger.viewIdToIncompleteEventTimers.entries()));
        assertEquals(1, Iterables.size(function.deliveryLogMerger.requestIdToIncompleteEventTimers.entries()));
        assertEquals(0, Iterables.size(function.impressionMerger.impressionIdToIncompleteEventTimers.entries()));
        assertEquals(1, Iterables.size(function.viewMerger.idToView.entries()));
        assertEquals(0, Iterables.size(function.deliveryLogMerger.idToCombinedDeliveryLog.entries()));
        assertEquals(0, Iterables.size(function.deliveryLogMerger.idToInsertion.entries()));
        assertEquals(1, Iterables.size(function.impressionMerger.idToImpression.entries()));

        harness.processElement1(toUnionEvent(DELIVERYLOG1, 300), 300);

        harness.processBothWatermarks(new Watermark(300));

        assertEmptyIncompleteEventStates();
        assertEquals(1, Iterables.size(function.viewMerger.idToView.entries()));
        assertEquals(1, Iterables.size(function.deliveryLogMerger.idToCombinedDeliveryLog.entries()));
        assertEquals(2, Iterables.size(function.deliveryLogMerger.idToInsertion.entries()));
        assertEquals(1, Iterables.size(function.impressionMerger.idToImpression.entries()));

        JoinedEvent.Builder expectedFlatImpression0 = createExpectedJoinedImpression().toBuilder();
        expectedFlatImpression0.getRequestBuilder().getTimingBuilder().setLogTimestamp(300);
        expectedFlatImpression0.getHiddenApiRequestBuilder().getTimingBuilder().setLogTimestamp(300);
        assertIterableEquals(
                ImmutableList.of(new StreamRecord(expectedFlatImpression0.build(), 300)),
                harness.extractOutputStreamRecords());

        harness.processBothWatermarks(new Watermark(1500));
        assertEmptyStates();
    }

    @Test
    public void incompleteImpression_missingView() throws Exception {
        // No view.
        harness.processElement1(toUnionEvent(DELIVERYLOG1, 150), 150);
        harness.processElement1(toUnionEvent(IMPRESSION1, 200), 200);

        harness.processBothWatermarks(new Watermark(300));

        harness.processElement2(createInputTinyFlatImpression1(), 300);

        harness.processBothWatermarks(new Watermark(350));

        assertEquals(0, Iterables.size(function.timeToIncompleteEvents.entries()));
        assertEquals(0, Iterables.size(function.viewMerger.viewIdToIncompleteEventTimers.entries()));
        assertEquals(0, Iterables.size(function.deliveryLogMerger.requestIdToIncompleteEventTimers.entries()));
        assertEquals(0, Iterables.size(function.impressionMerger.impressionIdToIncompleteEventTimers.entries()));
        assertEquals(0, Iterables.size(function.viewMerger.idToView.entries()));
        assertEquals(1, Iterables.size(function.deliveryLogMerger.idToCombinedDeliveryLog.entries()));
        assertEquals(2, Iterables.size(function.deliveryLogMerger.idToInsertion.entries()));
        assertEquals(1, Iterables.size(function.impressionMerger.idToImpression.entries()));

        JoinedEvent.Builder expectedFlatImpression0 = createExpectedJoinedImpression().toBuilder();
        expectedFlatImpression0.clearView();
        assertIterableEquals(
                ImmutableList.of(new StreamRecord(expectedFlatImpression0.build(), 300)),
                harness.extractOutputStreamRecords());

        harness.processBothWatermarks(new Watermark(1500));
        assertEmptyStates();
    }

    @Test
    public void fullImpression_skipViewJoin() throws Exception {
        setUpHarness(true);
        harness.processElement1(toUnionEvent(clearViewId(DELIVERYLOG1), 150), 150);
        harness.processElement1(toUnionEvent(IMPRESSION1, 200), 200);

        harness.processBothWatermarks(new Watermark(300));

        harness.processElement2(createInputTinyFlatImpression1(), 300);

        harness.processBothWatermarks(new Watermark(350));

        assertEmptyIncompleteEventStates();
        assertEquals(0, Iterables.size(function.viewMerger.idToView.entries()));
        assertEquals(1, Iterables.size(function.deliveryLogMerger.idToCombinedDeliveryLog.entries()));
        assertEquals(2, Iterables.size(function.deliveryLogMerger.idToInsertion.entries()));
        assertEquals(1, Iterables.size(function.impressionMerger.idToImpression.entries()));

        JoinedEvent.Builder expectedFlatImpression = createExpectedJoinedImpression().toBuilder();
        expectedFlatImpression.getIdsBuilder().clearViewId();
        expectedFlatImpression.clearView();
        assertIterableEquals(
                ImmutableList.of(new StreamRecord(expectedFlatImpression.build(), 300)),
                harness.extractOutputStreamRecords());

        harness.processBothWatermarks(new Watermark(1500));
        assertEmptyStates();
    }

    @Test
    public void incompleteImpression_missingDeliveryLog() throws Exception {
        // No view and no DeliveryLog.
        harness.processElement1(toUnionEvent(IMPRESSION1, 200), 200);

        harness.processBothWatermarks(new Watermark(300));

        TinyEvent impression1 = createInputTinyFlatImpression1();
        // Code onTimers at +50ms to complete the partial event.
        harness.processElement2(impression1, 300);

        harness.processBothWatermarks(new Watermark(330));

        assertEquals(1, Iterables.size(function.timeToIncompleteEvents.entries()));
        assertEquals(1, Iterables.size(function.viewMerger.viewIdToIncompleteEventTimers.entries()));
        assertEquals(1, Iterables.size(function.deliveryLogMerger.requestIdToIncompleteEventTimers.entries()));
        assertEquals(0, Iterables.size(function.impressionMerger.impressionIdToIncompleteEventTimers.entries()));
        assertEquals(0, Iterables.size(function.viewMerger.idToView.entries()));
        assertEquals(0, Iterables.size(function.deliveryLogMerger.idToCombinedDeliveryLog.entries()));
        assertEquals(0, Iterables.size(function.deliveryLogMerger.idToInsertion.entries()));
        assertEquals(1, Iterables.size(function.impressionMerger.idToImpression.entries()));

        assertEmpty(harness.extractOutputStreamRecords());

        harness.processBothWatermarks(new Watermark(350));

        JoinedEvent.Builder expectedFlatImpression0 = createExpectedJoinedImpression().toBuilder();
        expectedFlatImpression0.clearView();
        assertTrue(harness.extractOutputStreamRecords().isEmpty());

        assertIterableEquals(
                ImmutableList.of(new StreamRecord(
                        DroppedMergeDetailsEvent.newBuilder()
                                .setTinyEvent(impression1)
                                .setJoinedEvent(JoinedEvent.newBuilder()
                                    .setTiming(Timing.newBuilder().setLogTimestamp(200))
                                    .setIds(JoinedIdentifiers.newBuilder()
                                        .setInsertionId("insertion1")
                                        .setImpressionId("impression1"))
                                    .setImpression(Impression.newBuilder()
                                        .setTiming(Timing.newBuilder().setLogTimestamp(200))))
                                .build(),
                        350)),
                harness.getSideOutput(MergeImpressionDetails.DROPPED_TAG));

        harness.processBothWatermarks(new Watermark(1500));
        assertEmptyStates();
    }

    @Test
    public void incompleteImpression_missingDeliveryLog_flipped() throws Exception {

        TinyEvent impression1 = createInputTinyFlatImpression1();
        harness.processElement2(impression1, 300);
        // No view and no DeliveryLog.
        harness.processElement1(toUnionEvent(IMPRESSION1, 200), 300);

        harness.processBothWatermarks(new Watermark(330));

        assertEquals(1, Iterables.size(function.timeToIncompleteEvents.entries()));
        assertEquals(1, Iterables.size(function.viewMerger.viewIdToIncompleteEventTimers.entries()));
        assertEquals(1, Iterables.size(function.deliveryLogMerger.requestIdToIncompleteEventTimers.entries()));
        assertEquals(1, Iterables.size(function.impressionMerger.impressionIdToIncompleteEventTimers.entries()));
        assertEquals(0, Iterables.size(function.viewMerger.idToView.entries()));
        assertEquals(0, Iterables.size(function.deliveryLogMerger.idToCombinedDeliveryLog.entries()));
        assertEquals(0, Iterables.size(function.deliveryLogMerger.idToInsertion.entries()));
        assertEquals(1, Iterables.size(function.impressionMerger.idToImpression.entries()));

        assertEmpty(harness.extractOutputStreamRecords());

        harness.processBothWatermarks(new Watermark(350));

        JoinedEvent.Builder expectedFlatImpression0 = createExpectedJoinedImpression().toBuilder();
        expectedFlatImpression0.clearView();
        assertTrue(harness.extractOutputStreamRecords().isEmpty());

        assertIterableEquals(
                ImmutableList.of(new StreamRecord(
                        DroppedMergeDetailsEvent.newBuilder()
                                .setTinyEvent(impression1)
                                .setJoinedEvent(JoinedEvent.newBuilder()
                                        .setTiming(Timing.newBuilder().setLogTimestamp(200))
                                        .setIds(JoinedIdentifiers.newBuilder()
                                                .setInsertionId("insertion1")
                                                .setImpressionId("impression1"))
                                        .setImpression(Impression.newBuilder()
                                                .setTiming(Timing.newBuilder().setLogTimestamp(200))))
                                .build(),
                        350)),
                harness.getSideOutput(MergeImpressionDetails.DROPPED_TAG));

        harness.processBothWatermarks(new Watermark(1500));
        assertEmptyStates();
    }

    @Test
    public void noTinyImpression() throws Exception {
        harness.processElement1(toUnionEvent(VIEW1, 100), 100);
        harness.processElement1(toUnionEvent(DELIVERYLOG1, 150), 150);
        harness.processElement1(toUnionEvent(IMPRESSION1, 200), 200);

        harness.processBothWatermarks(new Watermark(300));
        harness.processBothWatermarks(new Watermark(350));

        assertEmptyIncompleteEventStates();
        assertEquals(1, Iterables.size(function.viewMerger.idToView.entries()));
        assertEquals(1, Iterables.size(function.deliveryLogMerger.idToCombinedDeliveryLog.entries()));
        assertEquals(2, Iterables.size(function.deliveryLogMerger.idToInsertion.entries()));
        assertEquals(1, Iterables.size(function.impressionMerger.idToImpression.entries()));

        harness.processBothWatermarks(new Watermark(1500));
        assertEmpty(harness.extractOutputStreamRecords());
        assertEmptyDetailsStates();
        assertEmptyStates();
    }

    @Test
    public void hasRequiredEvents() {
        assertTrue(function.hasRequiredEvents(EnumSet.noneOf(MissingEvent.class)));
        assertTrue(function.hasRequiredEvents(EnumSet.of(MissingEvent.VIEW)));
        assertFalse(function.hasRequiredEvents(EnumSet.of(MissingEvent.DELIERY_LOG)));
        assertFalse(function.hasRequiredEvents(EnumSet.of(MissingEvent.VIEW, MissingEvent.DELIERY_LOG)));
        assertFalse(function.hasRequiredEvents(EnumSet.of(MissingEvent.IMPRESSION)));
        assertTrue(function.hasRequiredEvents(EnumSet.of(MissingEvent.ACTION)));
    }

    private static <T> void assertEmpty(List<T> list) {
        assertTrue(list.isEmpty(), () -> "List should be empty but was not, list=" + list);
    }

    private static JoinedEvent createExpectedJoinedImpression() {
        return JoinedEvent.newBuilder()
                .setIds(JoinedIdentifiers.newBuilder()
                        .setViewId("view1")
                        .setRequestId("request1")
                        .setInsertionId("insertion1")
                        .setImpressionId("impression1"))
                .setTiming(Timing.newBuilder().setLogTimestamp(200))
                .setView(View.newBuilder()
                        .setTiming(Timing.newBuilder().setLogTimestamp(100)))
                .setRequest(Request.newBuilder()
                        .setTiming(Timing.newBuilder().setLogTimestamp(150)))
                .setHiddenApiRequest(HiddenApiRequest.newBuilder()
                        .setRequestId("request3")
                        .setTiming(Timing.newBuilder().setLogTimestamp(150))
                        .setClientInfo(ClientInfo.getDefaultInstance())
                        .build())
                // TODO - add HiddenApiRequest.
                .setResponse(Response.getDefaultInstance())
                .setResponseInsertion(Insertion.newBuilder()
                        .setContentId("content1"))
                .setImpression(Impression.newBuilder()
                        .setTiming(Timing.newBuilder().setLogTimestamp(200)))
                .build();
    }

    private static TinyEvent createInputTinyFlatImpression1() {
        return TinyEvent.newBuilder()
                .setViewId("view1")
                .setRequestId("request1")
                .setInsertionId("insertion1")
                .setContentId("content1")
                .setImpressionId("impression1")
                .build();
    }

    private void assertEmptyStates() throws Exception {
        assertEmptyIncompleteEventStates();
        assertEmptyDetailsStates();
    }

    private void assertEmptyIncompleteEventStates() throws Exception {
        assertEquals(0, Iterables.size(function.timeToIncompleteEvents.entries()));
        assertEquals(0, Iterables.size(function.viewMerger.viewIdToIncompleteEventTimers.entries()));
        assertEquals(0, Iterables.size(function.deliveryLogMerger.requestIdToIncompleteEventTimers.entries()));
        assertEquals(0, Iterables.size(function.impressionMerger.impressionIdToIncompleteEventTimers.entries()));
    }

    private void assertEmptyDetailsStates() throws Exception {
        assertEquals(0, Iterables.size(function.viewMerger.idToView.entries()));
        assertEquals(0, Iterables.size(function.deliveryLogMerger.idToCombinedDeliveryLog.entries()));
        assertEquals(0, Iterables.size(function.deliveryLogMerger.idToInsertion.entries()));
        assertEquals(0, Iterables.size(function.impressionMerger.idToImpression.entries()));
    }

    private static UnionEvent toUnionEvent(View view, long logTimestamp) {
        return UnionEvent.newBuilder().setView(withLogTimestamp(view, logTimestamp)).build();
    }

    private static UnionEvent toUnionEvent(CombinedDeliveryLog combinedDeliveryLog, long logTimestamp) {
        return UnionEvent.newBuilder().setCombinedDeliveryLog(withLogTimestamp(combinedDeliveryLog, logTimestamp)).build();
    }

    private static UnionEvent toUnionEvent(Impression impression, long logTimestamp) {
        return UnionEvent.newBuilder().setImpression(withLogTimestamp(impression, logTimestamp)).build();
    }

    private static View createView(String viewId) {
        return View.newBuilder()
                .setViewId(viewId)
                .build();
    }

    private static View withLogTimestamp(View view, long logTimestamp) {
        View.Builder builder = view.toBuilder();
        builder.getTimingBuilder().setLogTimestamp(logTimestamp);
        return builder.build();
    }

    private static DeliveryLog createDeliveryLog(String requestId, String viewId, List<Insertion> insertions) {
        return DeliveryLog.newBuilder()
                .setRequest(Request.newBuilder()
                    .setRequestId(requestId)
                    .setViewId(viewId))
                .setResponse(Response.newBuilder()
                    .addAllInsertion(insertions))
                .build();
    }

    private static CombinedDeliveryLog withLogTimestamp(CombinedDeliveryLog deliveryLog, long logTimestamp) {
        CombinedDeliveryLog.Builder builder = deliveryLog.toBuilder();
        if (builder.hasSdk()) {
            builder.setSdk(withLogTimestamp(builder.getSdk(), logTimestamp));
        }
        if (builder.hasApi()) {
            builder.setApi(withLogTimestamp(builder.getApi(), logTimestamp));
        }
        return builder.build();
    }

    private static DeliveryLog withLogTimestamp(DeliveryLog deliveryLog, long logTimestamp) {
        DeliveryLog.Builder builder = deliveryLog.toBuilder();
        builder.getRequestBuilder().getTimingBuilder().setLogTimestamp(logTimestamp);
        return builder.build();
    }

    private static Insertion createInsertion(String insertionId, String contentId) {
        return Insertion.newBuilder()
                .setInsertionId(insertionId)
                .setContentId(contentId)
                .build();
    }

    private static Impression createImpression(String impressionId) {
        return Impression.newBuilder()
                .setImpressionId(impressionId)
                .build();
    }

    private static Impression withLogTimestamp(Impression impression, long logTimestamp) {
        Impression.Builder builder = impression.toBuilder();
        builder.getTimingBuilder().setLogTimestamp(logTimestamp);
        return builder.build();
    }

    private static CombinedDeliveryLog clearViewId(CombinedDeliveryLog combinedDeliveryLog) {
        return clearViewId(combinedDeliveryLog.toBuilder()).build();
    }

    private static CombinedDeliveryLog.Builder clearViewId(CombinedDeliveryLog.Builder builder) {
        if (builder.hasApi()) {
            clearViewId(builder.getApiBuilder());
        }
        if (builder.hasSdk()) {
            clearViewId(builder.getSdkBuilder());
        }
        return builder;
    }

    private static DeliveryLog.Builder clearViewId(DeliveryLog.Builder builder) {
        builder.getRequestBuilder().clearViewId();
        return builder;
    }
}
