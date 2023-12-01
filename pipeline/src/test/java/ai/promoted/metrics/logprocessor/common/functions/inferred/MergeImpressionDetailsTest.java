package ai.promoted.metrics.logprocessor.common.functions.inferred;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.promoted.metrics.logprocessor.common.functions.KeyUtil;
import ai.promoted.metrics.logprocessor.common.functions.inferred.AbstractMergeDetails.MissingEvent;
import ai.promoted.metrics.logprocessor.common.util.DebugIds;
import ai.promoted.metrics.logprocessor.common.util.TrackingUtil;
import ai.promoted.proto.common.ClientInfo;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.delivery.Response;
import ai.promoted.proto.event.CombinedDeliveryLog;
import ai.promoted.proto.event.DroppedMergeImpressionDetails;
import ai.promoted.proto.event.HiddenApiRequest;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.JoinedIdentifiers;
import ai.promoted.proto.event.JoinedImpression;
import ai.promoted.proto.event.TinyDeliveryLog;
import ai.promoted.proto.event.TinyImpression;
import ai.promoted.proto.event.TinyInsertion;
import ai.promoted.proto.event.TinyInsertionCore;
import ai.promoted.proto.event.TinyJoinedImpression;
import ai.promoted.proto.event.UnionEvent;
import com.google.common.collect.ImmutableList;
import com.twitter.chill.protobuf.ProtobufSerializer;
import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests mainly for the underlying BaseInferred join implementation. */
public class MergeImpressionDetailsTest {
  private static final CombinedDeliveryLog DELIVERYLOG1 =
      CombinedDeliveryLog.newBuilder()
          .setSdk(
              createDeliveryLog(
                  "request1",
                  ImmutableList.of(
                      createInsertion("insertion1", "content1"),
                      createInsertion("insertion2", "content2"))))
          .setApi(
              createDeliveryLog(
                  "request3",
                  ImmutableList.of(
                      createInsertion("insertion5", "content1"),
                      createInsertion("insertion6", "content2"))))
          .build();
  private static final CombinedDeliveryLog DELIVERYLOG2 =
      CombinedDeliveryLog.newBuilder()
          .setSdk(
              createDeliveryLog(
                  "request2",
                  ImmutableList.of(
                      createInsertion("insertion3", "content3"),
                      createInsertion("insertion4", "content4"))))
          .build();

  private static final Impression IMPRESSION1 =
      createImpression("impression1").toBuilder().setInsertionId("insertion1").build();

  private KeyedTwoInputStreamOperatorTestHarness<
          Tuple2<Long, String>, UnionEvent, TinyJoinedImpression, JoinedImpression>
      harness;
  private MergeImpressionDetails function;

  private static JoinedImpression createExpectedJoinedImpression() {
    return JoinedImpression.newBuilder()
        .setIds(
            JoinedIdentifiers.newBuilder()
                .setRequestId("request1")
                .setInsertionId("insertion1")
                .setImpressionId("impression1"))
        .setTiming(Timing.newBuilder().setEventApiTimestamp(200).setProcessingTimestamp(1L))
        .setRequest(Request.newBuilder().setTiming(Timing.newBuilder().setEventApiTimestamp(150)))
        .setHiddenApiRequest(
            HiddenApiRequest.newBuilder()
                .setRequestId("request3")
                .setTiming(Timing.newBuilder().setEventApiTimestamp(150))
                .setClientInfo(ClientInfo.getDefaultInstance())
                .build())
        // TODO - add HiddenApiRequest.
        .setResponse(Response.getDefaultInstance())
        .setResponseInsertion(Insertion.newBuilder().setContentId("content1"))
        .setImpression(
            Impression.newBuilder().setTiming(Timing.newBuilder().setEventApiTimestamp(200)))
        .build();
  }

  private static TinyJoinedImpression createInputTinyJoinedImpression1() {
    return TinyJoinedImpression.newBuilder()
        .setInsertion(
            TinyInsertion.newBuilder()
                .setRequestId("request1")
                .setCore(
                    TinyInsertionCore.newBuilder()
                        .setInsertionId("insertion1")
                        .setContentId("content1")))
        .setImpression(TinyImpression.newBuilder().setImpressionId("impression1"))
        .build();
  }

  // TODO - add other unused inputs.

  private static UnionEvent toUnionEvent(
      CombinedDeliveryLog combinedDeliveryLog, long logTimestamp) {
    return UnionEvent.newBuilder()
        .setCombinedDeliveryLog(withLogTimestamp(combinedDeliveryLog, logTimestamp))
        .build();
  }

  private static UnionEvent toUnionEvent(Impression impression, long logTimestamp) {
    return UnionEvent.newBuilder()
        .setImpression(withLogTimestamp(impression, logTimestamp))
        .build();
  }

  private static DeliveryLog createDeliveryLog(String requestId, List<Insertion> insertions) {
    return DeliveryLog.newBuilder()
        .setRequest(Request.newBuilder().setRequestId(requestId))
        .setResponse(Response.newBuilder().addAllInsertion(insertions))
        .build();
  }

  private static CombinedDeliveryLog withLogTimestamp(
      CombinedDeliveryLog deliveryLog, long logTimestamp) {
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
    builder.getRequestBuilder().getTimingBuilder().setEventApiTimestamp(logTimestamp);
    return builder.build();
  }

  private static Insertion createInsertion(String insertionId, String contentId) {
    return Insertion.newBuilder().setInsertionId(insertionId).setContentId(contentId).build();
  }

  private static Impression createImpression(String impressionId) {
    return Impression.newBuilder().setImpressionId(impressionId).build();
  }

  private static Impression withLogTimestamp(Impression impression, long logTimestamp) {
    Impression.Builder builder = impression.toBuilder();
    builder.getTimingBuilder().setEventApiTimestamp(logTimestamp);
    return builder.build();
  }

  @BeforeAll
  public static void globalSetUp() {
    TrackingUtil.processingTimeSupplier = () -> 1L;
  }

  @BeforeEach
  public void setUp() throws Exception {
    setUpHarness();
  }

  /** Separate setup method so we can override. */
  private void setUpHarness() throws Exception {
    if (harness != null) {
      harness.close();
    }
    function =
        new MergeImpressionDetails(
            Duration.ofMillis(300),
            Duration.ofMillis(200),
            Duration.ofMillis(500),
            Duration.ofMillis(50),
            0L,
            DebugIds.empty());
    harness =
        new KeyedTwoInputStreamOperatorTestHarness<>(
            new KeyedCoProcessOperator<>(function),
            KeyUtil.unionEntityKeySelector,
            KeyUtil.tinyJoinedImpressionAnonUserIdKey,
            Types.TUPLE(Types.LONG, Types.STRING));
    ExecutionConfig config = harness.getExecutionConfig();
    config.registerTypeWithKryoSerializer(UnionEvent.class, ProtobufSerializer.class);
    config.registerTypeWithKryoSerializer(TinyJoinedImpression.class, ProtobufSerializer.class);
    config.registerTypeWithKryoSerializer(TinyDeliveryLog.class, ProtobufSerializer.class);
    harness.setup();
    harness.open();
  }

  @AfterEach
  public void tearDown() throws Exception {
    harness.close();
    harness = null;
  }

  @Test
  public void fullImpression() throws Exception {
    harness.processElement1(toUnionEvent(DELIVERYLOG1, 150), 150);
    harness.processElement1(toUnionEvent(IMPRESSION1, 200), 200);

    harness.processBothWatermarks(new Watermark(300));

    harness.processElement2(createInputTinyJoinedImpression1(), 300);

    harness.processBothWatermarks(new Watermark(350));

    assertEmptyIncompleteEventStates();
    assertThat(function.deliveryLogMerger.idToCombinedDeliveryLog.entries()).hasSize(1);
    assertThat(function.deliveryLogMerger.idToInsertion.entries()).hasSize(2);
    assertThat(function.impressionMerger.idToImpression.entries()).hasSize(1);

    assertThat(harness.extractOutputStreamRecords())
        .containsExactly(new StreamRecord(createExpectedJoinedImpression(), 300));

    harness.processBothWatermarks(new Watermark(1500));
    assertEmptyStates();
  }

  @Test
  public void fullImpression_outOfOrder() throws Exception {
    harness.processElement2(createInputTinyJoinedImpression1(), 100);
    assertThat(function.timeToIncompleteEvents.entries()).hasSize(1);
    assertThat(function.deliveryLogMerger.requestIdToIncompleteEventTimers.entries()).hasSize(1);
    assertThat(function.impressionMerger.impressionIdToIncompleteEventTimers.entries()).hasSize(1);
    assertThat(function.deliveryLogMerger.idToCombinedDeliveryLog.entries()).isEmpty();
    assertThat(function.deliveryLogMerger.idToInsertion.entries()).isEmpty();
    assertThat(function.impressionMerger.idToImpression.entries()).isEmpty();

    harness.processElement1(toUnionEvent(DELIVERYLOG1, 150), 150);
    harness.processElement1(toUnionEvent(IMPRESSION1, 200), 200);

    harness.processBothWatermarks(new Watermark(250));
    assertThat(function.timeToIncompleteEvents.entries()).isEmpty();
    assertThat(function.deliveryLogMerger.requestIdToIncompleteEventTimers.entries()).hasSize(1);
    assertThat(function.impressionMerger.impressionIdToIncompleteEventTimers.entries()).isEmpty();
    assertThat(function.deliveryLogMerger.idToCombinedDeliveryLog.entries()).hasSize(1);
    assertThat(function.deliveryLogMerger.idToInsertion.entries()).hasSize(2);
    assertThat(function.impressionMerger.idToImpression.entries()).hasSize(1);
    assertThat(harness.extractOutputStreamRecords())
        .containsExactly(new StreamRecord(createExpectedJoinedImpression(), 200));

    harness.processBothWatermarks(new Watermark(1500));
    assertEmptyStates();
  }

  @Test
  public void fullImpression_lateDeliveryLog() throws Exception {
    harness.processElement1(toUnionEvent(IMPRESSION1, 200), 200);

    harness.processElement2(createInputTinyJoinedImpression1(), 100);
    assertThat(function.timeToIncompleteEvents.entries()).hasSize(1);
    assertThat(function.deliveryLogMerger.requestIdToIncompleteEventTimers.entries()).hasSize(1);
    assertThat(function.impressionMerger.impressionIdToIncompleteEventTimers.entries()).isEmpty();
    assertThat(function.deliveryLogMerger.idToCombinedDeliveryLog.entries()).isEmpty();
    assertThat(function.deliveryLogMerger.idToInsertion.entries()).isEmpty();
    assertThat(function.impressionMerger.idToImpression.entries()).hasSize(1);

    harness.processElement1(toUnionEvent(DELIVERYLOG1, 300), 300);

    harness.processBothWatermarks(new Watermark(300));

    assertEmptyIncompleteEventStates();
    assertThat(function.deliveryLogMerger.idToCombinedDeliveryLog.entries()).hasSize(1);
    assertThat(function.deliveryLogMerger.idToInsertion.entries()).hasSize(2);
    assertThat(function.impressionMerger.idToImpression.entries()).hasSize(1);

    JoinedImpression.Builder expectedFlatImpression0 = createExpectedJoinedImpression().toBuilder();
    expectedFlatImpression0.getRequestBuilder().getTimingBuilder().setEventApiTimestamp(300);
    expectedFlatImpression0
        .getHiddenApiRequestBuilder()
        .getTimingBuilder()
        .setEventApiTimestamp(300);
    assertIterableEquals(
        ImmutableList.of(new StreamRecord(expectedFlatImpression0.build(), 300)),
        harness.extractOutputStreamRecords());

    harness.processBothWatermarks(new Watermark(1500));
    assertEmptyStates();
  }

  @Test
  public void incompleteImpression_missingDeliveryLog() throws Exception {
    // No view and no DeliveryLog.
    harness.processElement1(toUnionEvent(IMPRESSION1, 200), 200);

    harness.processBothWatermarks(new Watermark(300));

    TinyJoinedImpression impression1 = createInputTinyJoinedImpression1();
    // Code onTimers at +50ms to complete the partial event.
    harness.processElement2(impression1, 300);

    harness.processBothWatermarks(new Watermark(330));
    assertThat(function.timeToIncompleteEvents.entries()).hasSize(1);
    assertThat(function.deliveryLogMerger.requestIdToIncompleteEventTimers.entries()).hasSize(1);
    assertThat(function.impressionMerger.impressionIdToIncompleteEventTimers.entries()).isEmpty();
    assertThat(function.deliveryLogMerger.idToCombinedDeliveryLog.entries()).isEmpty();
    assertThat(function.deliveryLogMerger.idToInsertion.entries()).isEmpty();
    assertThat(function.impressionMerger.idToImpression.entries()).hasSize(1);
    assertThat(harness.extractOutputStreamRecords()).isEmpty();

    harness.processBothWatermarks(new Watermark(350));

    JoinedImpression.Builder expectedFlatImpression0 = createExpectedJoinedImpression().toBuilder();
    assertTrue(harness.extractOutputStreamRecords().isEmpty());

    assertThat(harness.getSideOutput(MergeImpressionDetails.DROPPED_TAG))
        .containsExactly(
            new StreamRecord(
                DroppedMergeImpressionDetails.newBuilder()
                    .setImpression(impression1)
                    .setJoinedImpression(
                        JoinedImpression.newBuilder()
                            .setTiming(
                                Timing.newBuilder()
                                    .setEventApiTimestamp(200)
                                    .setProcessingTimestamp(1L))
                            .setIds(
                                JoinedIdentifiers.newBuilder()
                                    .setInsertionId("insertion1")
                                    .setImpressionId("impression1"))
                            .setImpression(
                                Impression.newBuilder()
                                    .setTiming(Timing.newBuilder().setEventApiTimestamp(200))))
                    .build(),
                350));

    harness.processBothWatermarks(new Watermark(1500));
    assertEmptyStates();
  }

  @Test
  public void incompleteImpression_missingDeliveryLog_flipped() throws Exception {

    TinyJoinedImpression impression1 = createInputTinyJoinedImpression1();
    harness.processElement2(impression1, 300);
    // No view and no DeliveryLog.
    harness.processElement1(toUnionEvent(IMPRESSION1, 200), 300);

    harness.processBothWatermarks(new Watermark(330));
    assertThat(function.timeToIncompleteEvents.entries()).hasSize(1);
    assertThat(function.deliveryLogMerger.requestIdToIncompleteEventTimers.entries()).hasSize(1);
    assertThat(function.impressionMerger.impressionIdToIncompleteEventTimers.entries()).hasSize(1);
    assertThat(function.deliveryLogMerger.idToCombinedDeliveryLog.entries()).isEmpty();
    assertThat(function.deliveryLogMerger.idToInsertion.entries()).isEmpty();
    assertThat(function.impressionMerger.idToImpression.entries()).hasSize(1);
    assertThat(harness.extractOutputStreamRecords()).isEmpty();

    harness.processBothWatermarks(new Watermark(350));

    JoinedImpression.Builder expectedFlatImpression0 = createExpectedJoinedImpression().toBuilder();
    assertThat(harness.extractOutputStreamRecords()).isEmpty();

    assertThat(harness.getSideOutput(MergeImpressionDetails.DROPPED_TAG))
        .containsExactly(
            new StreamRecord(
                DroppedMergeImpressionDetails.newBuilder()
                    .setImpression(impression1)
                    .setJoinedImpression(
                        JoinedImpression.newBuilder()
                            .setTiming(
                                Timing.newBuilder()
                                    .setEventApiTimestamp(200)
                                    .setProcessingTimestamp(1L))
                            .setIds(
                                JoinedIdentifiers.newBuilder()
                                    .setInsertionId("insertion1")
                                    .setImpressionId("impression1"))
                            .setImpression(
                                Impression.newBuilder()
                                    .setTiming(Timing.newBuilder().setEventApiTimestamp(200))))
                    .build(),
                350));

    harness.processBothWatermarks(new Watermark(1500));
    assertEmptyStates();
  }

  @Test
  public void noTinyImpression() throws Exception {
    harness.processElement1(toUnionEvent(DELIVERYLOG1, 150), 150);
    harness.processElement1(toUnionEvent(IMPRESSION1, 200), 200);

    harness.processBothWatermarks(new Watermark(300));
    harness.processBothWatermarks(new Watermark(350));

    assertEmptyIncompleteEventStates();
    assertThat(function.deliveryLogMerger.idToCombinedDeliveryLog.entries()).hasSize(1);
    assertThat(function.deliveryLogMerger.idToInsertion.entries()).hasSize(2);
    assertThat(function.impressionMerger.idToImpression.entries()).hasSize(1);

    harness.processBothWatermarks(new Watermark(1500));
    assertThat(harness.extractOutputStreamRecords()).isEmpty();
    assertEmptyDetailsStates();
    assertEmptyStates();
  }

  @Test
  public void hasRequiredEvents() {
    assertTrue(function.hasRequiredEvents(EnumSet.noneOf(MissingEvent.class)));
    assertFalse(function.hasRequiredEvents(EnumSet.of(MissingEvent.DELIERY_LOG)));
    assertFalse(function.hasRequiredEvents(EnumSet.of(MissingEvent.IMPRESSION)));
    assertTrue(function.hasRequiredEvents(EnumSet.of(MissingEvent.ACTION)));
  }

  private void assertEmptyStates() throws Exception {
    assertEmptyIncompleteEventStates();
    assertEmptyDetailsStates();
  }

  private void assertEmptyIncompleteEventStates() throws Exception {
    assertThat(function.timeToIncompleteEvents.entries()).isEmpty();
    assertThat(function.deliveryLogMerger.requestIdToIncompleteEventTimers.entries()).isEmpty();
    assertThat(function.impressionMerger.impressionIdToIncompleteEventTimers.entries()).isEmpty();
  }

  private void assertEmptyDetailsStates() throws Exception {
    assertThat(function.deliveryLogMerger.idToCombinedDeliveryLog.entries()).isEmpty();
    assertThat(function.deliveryLogMerger.idToInsertion.entries()).isEmpty();
    assertThat(function.impressionMerger.idToImpression.entries()).isEmpty();
  }
}
