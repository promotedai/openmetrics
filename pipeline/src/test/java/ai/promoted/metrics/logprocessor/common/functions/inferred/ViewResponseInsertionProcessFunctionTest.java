package ai.promoted.metrics.logprocessor.common.functions.inferred;

import ai.promoted.metrics.error.MismatchError;
import ai.promoted.metrics.logprocessor.common.flink.operator.InferenceOperator;
import ai.promoted.proto.event.TinyDeliveryLog;
import ai.promoted.proto.event.TinyDeliveryLog.TinyInsertion;
import ai.promoted.proto.event.TinyEvent;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.twitter.chill.protobuf.ProtobufSerializer;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests mainly for the underlying BaseInferred join implementation. */
public class ViewResponseInsertionProcessFunctionTest {
  private static final TinyEvent VIEW1 = createFlatView("view1", "user").build();
  private static final TinyEvent VIEW2 = createFlatView("view2", "user").build();
  private static final TinyEvent VIEW3 = createFlatView("view3", "user").build();

  private static final TinyDeliveryLog VIEW1_DELIVERYLOG =
      addInsertions(
              createTinyDeliveryLog("request1", "view1", "user"),
              createInsertion("insertion1", "content1"),
              createInsertion("insertion2", "content2"))
          .build();
  private static final TinyDeliveryLog VIEW2_DELIVERYLOG =
      addInsertions(
              createTinyDeliveryLog("request2", "view2", "user"),
              createInsertion("insertion3", "content3"),
              createInsertion("insertion4", "content4"))
          .build();

  private static final TinyDeliveryLog DELIVERYLOG3 =
      addInsertions(
              createTinyDeliveryLog("request1", "user"),
              createInsertion("insertion5", "content5"),
              createInsertion("insertion6", "content6"))
          .build();
  private static final TinyDeliveryLog DELIVERYLOG4 =
      addInsertions(
              createTinyDeliveryLog("request2", "user"),
              createInsertion("insertion7", "content7"),
              createInsertion("insertion8", "content8"))
          .build();

  private static BiConsumer<OutputTag<MismatchError>, MismatchError> noErrorLogger =
      (tag, error) -> {};

  private KeyedTwoInputStreamOperatorTestHarness<
          Tuple2<Long, String>, TinyEvent, TinyDeliveryLog, TinyEvent>
      harness;
  private InferenceOperator<TinyDeliveryLog> operator;
  private ViewResponseInsertionProcessFunction function;

  @BeforeEach
  public void setUp() throws Exception {
    setUpHarness(Options.builder().setRightOuterJoin(false));
  }

  /** Separate setup method so we can override. */
  private void setUpHarness(Options.Builder optionsBuilder) throws Exception {
    if (harness != null) {
      harness.close();
    }
    function =
        new ViewResponseInsertionProcessFunction(
            optionsBuilder
                .setMaxTime(Duration.ofMillis(100))
                .setMaxOutOfOrder(Duration.ofMillis(50))
                .setCheckLateness(true)
                .setIdJoinDurationMultiplier(1)
                .build());
    operator = InferenceOperator.of(function, false);
    harness =
        new KeyedTwoInputStreamOperatorTestHarness<>(
            operator,
            // For purposes of testing, we're staying with in a single platform, so don't need to
            // add it to our key.
            flat -> Tuple2.of(flat.getPlatformId(), flat.getLogUserId()),
            deliveryLog -> Tuple2.of(deliveryLog.getPlatformId(), deliveryLog.getLogUserId()),
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

  @Test
  public void explicitIdJoins() throws Exception {
    harness.processElement1(withLogTimestamp(VIEW1, 10), 10);
    harness.processElement2(withLogTimestamp(VIEW1_DELIVERYLOG, 20), 20);
    harness.processElement2(withLogTimestamp(VIEW2_DELIVERYLOG, 40), 40);

    // One in and one out of order request events.
    harness.processBothWatermarks(new Watermark(50));
    Assertions.assertEquals(3, harness.numKeyedStateEntries());
    Assertions.assertEquals(2, harness.numEventTimeTimers());
    Assertions.assertEquals(1, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(1, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(1, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceCandidates.entries()));

    harness.processElement1(withLogTimestamp(VIEW2, 80), 80);
    harness.processElement1(withLogTimestamp(VIEW3, 90), 90);

    // Rest of the views, joins out of order request.
    harness.processBothWatermarks(new Watermark(100));
    Assertions.assertEquals(3, harness.numEventTimeTimers());
    Assertions.assertEquals(3, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(1, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceCandidates.entries()));

    // Verify output.
    List<TinyEvent.Builder> expectedInOrderList =
        function.join.apply(VIEW1.toBuilder(), VIEW1_DELIVERYLOG, noErrorLogger);
    Assertions.assertEquals(2, expectedInOrderList.size());
    TinyEvent.Builder expectedInOrder0 = expectedInOrderList.get(0);
    expectedInOrder0.setLogTimestamp(20);
    TinyEvent.Builder expectedInOrder1 = expectedInOrderList.get(1);
    expectedInOrder1.setLogTimestamp(20);
    List<TinyEvent.Builder> expectedOutOrderList =
        function.join.apply(VIEW2.toBuilder(), VIEW2_DELIVERYLOG, noErrorLogger);
    Assertions.assertEquals(2, expectedOutOrderList.size());
    TinyEvent.Builder expectedOutOrder0 = expectedOutOrderList.get(0);
    expectedOutOrder0.setLogTimestamp(40);
    TinyEvent.Builder expectedOutOrder1 = expectedOutOrderList.get(1);
    expectedOutOrder1.setLogTimestamp(40);
    ImmutableList<StreamRecord<TinyEvent>> expectedJoins =
        ImmutableList.of(
            new StreamRecord<>(expectedInOrder0.build(), 20),
            new StreamRecord<>(expectedInOrder1.build(), 20),
            new StreamRecord<>(expectedOutOrder0.build(), 80),
            new StreamRecord<>(expectedOutOrder1.build(), 80));
    Assertions.assertIterableEquals(expectedJoins, harness.extractOutputStreamRecords());

    // No events within timer bucket.
    harness.processBothWatermarks(new Watermark(150));
    Assertions.assertEquals(2, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(1, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceCandidates.entries()));

    // No events moving to next timer bucket.
    harness.processBothWatermarks(new Watermark(200));
    Assertions.assertEquals(0, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceCandidates.entries()));

    // No change in output.
    Assertions.assertIterableEquals(expectedJoins, harness.extractOutputStreamRecords());
  }

  @Test
  public void inferredJoin() throws Exception {
    harness.processElement1(withLogTimestamp(VIEW1, 10), 10);
    harness.processElement2(withLogTimestamp(DELIVERYLOG3, 20), 20);
    harness.processElement2(withLogTimestamp(DELIVERYLOG4, 50), 50);

    // One in and one out of order request events.
    harness.processBothWatermarks(new Watermark(50));
    Assertions.assertEquals(3, harness.numKeyedStateEntries());
    Assertions.assertEquals(3, harness.numEventTimeTimers());
    Assertions.assertEquals(1, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(1, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(2, Iterables.size(function.inferenceCandidates.entries()));

    // No output
    Assertions.assertIterableEquals(ImmutableList.of(), harness.extractOutputStreamRecords());

    harness.processElement1(withLogTimestamp(VIEW2, 80), 80);
    harness.processElement1(withLogTimestamp(VIEW3, 90), 90);

    // Rest of the views.  Triggers inference.
    harness.processBothWatermarks(new Watermark(100));
    Assertions.assertEquals(3, harness.numEventTimeTimers());
    Assertions.assertEquals(3, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(1, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceCandidates.entries()));

    // Verify output.
    List<TinyEvent.Builder> expectedInOrderList =
        function.join.apply(VIEW1.toBuilder(), DELIVERYLOG3, noErrorLogger);
    Assertions.assertEquals(2, expectedInOrderList.size());
    TinyEvent.Builder expectedInOrder0 = expectedInOrderList.get(0);
    expectedInOrder0.setLogTimestamp(20);
    TinyEvent.Builder expectedInOrder1 = expectedInOrderList.get(1);
    expectedInOrder1.setLogTimestamp(20);
    List<TinyEvent.Builder> expectedOutOrderList =
        function.join.apply(VIEW2.toBuilder(), DELIVERYLOG4, noErrorLogger);
    Assertions.assertEquals(2, expectedOutOrderList.size());
    TinyEvent.Builder expectedOutOrder0 = expectedOutOrderList.get(0);
    expectedOutOrder0.setLogTimestamp(50);
    TinyEvent.Builder expectedOutOrder1 = expectedOutOrderList.get(1);
    expectedOutOrder1.setLogTimestamp(50);
    Assertions.assertIterableEquals(
        ImmutableList.of(
            new StreamRecord<>(expectedInOrder0.build(), 70),
            new StreamRecord<>(expectedInOrder1.build(), 70),
            new StreamRecord<>(expectedOutOrder0.build(), 100),
            new StreamRecord<>(expectedOutOrder1.build(), 100)),
        harness.extractOutputStreamRecords());

    // No events within timer bucket.  Triggers inference.
    harness.processBothWatermarks(new Watermark(150));
    Assertions.assertEquals(2, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(1, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceCandidates.entries()));

    // Verify no additional outputs.
    Assertions.assertEquals(4, harness.extractOutputStreamRecords().size());

    // No events moving to next timer bucket.
    harness.processBothWatermarks(new Watermark(200));
    Assertions.assertEquals(0, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceCandidates.entries()));

    // Verify output is the same.
    Assertions.assertEquals(4, harness.extractOutputStreamRecords().size());
  }

  @Test
  public void inferenceScopesInOrder() throws Exception {
    // Don't set it too large, or it'll take a long time.
    int size = 100;
    TreeSet<Tuple2<Long, String>> expected = new TreeSet<>((a, b) -> a.f0.compareTo(b.f0));
    long[] rand = new Random().longs(0, 1000000000).distinct().limit(size).toArray();
    ;
    for (int i = 0; i < size; ++i) {
      String viewId = "view" + i;
      long timestamp = rand[i];
      TinyEvent view = withLogTimestamp(createFlatView(viewId, "user").build(), timestamp);
      harness.processElement1(new StreamRecord<>(view, timestamp));
      expected.add(Tuple2.of(timestamp, viewId));
    }
    Assertions.assertEquals(2, harness.numKeyedStateEntries());
    function.inferenceScopes.values().forEach(v -> Assertions.assertIterableEquals(expected, v));
  }

  @Test
  public void rightJoin_disabled() throws Exception {
    harness.processElement2(withLogTimestamp(DELIVERYLOG3, 20), 20);
    harness.processElement2(withLogTimestamp(DELIVERYLOG4, 50), 50);

    // One in and one out of order request events.
    harness.processBothWatermarks(new Watermark(50));
    Assertions.assertEquals(1, harness.numKeyedStateEntries());
    Assertions.assertEquals(2, harness.numEventTimeTimers());
    Assertions.assertEquals(0, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(2, Iterables.size(function.inferenceCandidates.entries()));

    // No output
    Assertions.assertIterableEquals(ImmutableList.of(), harness.extractOutputStreamRecords());

    // Rest of the views.  Triggers inference.
    harness.processBothWatermarks(new Watermark(100));
    Assertions.assertEquals(0, harness.numEventTimeTimers());
    Assertions.assertEquals(0, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceCandidates.entries()));

    Assertions.assertIterableEquals(ImmutableList.of(), harness.extractOutputStreamRecords());
  }

  @Test
  public void rightJoin_enabled_withoutForeignKey() throws Exception {
    setUpHarness(Options.builder().setRightOuterJoin(true));
    harness.processElement2(withLogTimestamp(DELIVERYLOG3, 20), 20);
    harness.processElement2(withLogTimestamp(DELIVERYLOG4, 50), 50);

    // One in and one out of order request events.
    harness.processBothWatermarks(new Watermark(50));
    Assertions.assertEquals(1, harness.numKeyedStateEntries());
    Assertions.assertEquals(2, harness.numEventTimeTimers());
    Assertions.assertEquals(0, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(2, Iterables.size(function.inferenceCandidates.entries()));

    // No output
    Assertions.assertIterableEquals(ImmutableList.of(), harness.extractOutputStreamRecords());

    // Rest of the views.  Triggers inference.
    harness.processBothWatermarks(new Watermark(100));
    Assertions.assertEquals(0, harness.numEventTimeTimers());
    Assertions.assertEquals(0, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceCandidates.entries()));

    // Verify output.
    List<TinyEvent.Builder> expectedInOrderList =
        function.join.apply(TinyEvent.newBuilder(), DELIVERYLOG3, noErrorLogger);
    Assertions.assertEquals(2, expectedInOrderList.size());
    TinyEvent.Builder expectedInOrder0 = expectedInOrderList.get(0);
    expectedInOrder0.setLogTimestamp(20);
    TinyEvent.Builder expectedInOrder1 = expectedInOrderList.get(1);
    expectedInOrder1.setLogTimestamp(20);
    List<TinyEvent.Builder> expectedOutOrderList =
        function.join.apply(TinyEvent.newBuilder(), DELIVERYLOG4, noErrorLogger);
    Assertions.assertEquals(2, expectedOutOrderList.size());
    TinyEvent.Builder expectedOutOrder0 = expectedOutOrderList.get(0);
    expectedOutOrder0.setLogTimestamp(50);
    TinyEvent.Builder expectedOutOrder1 = expectedOutOrderList.get(1);
    expectedOutOrder1.setLogTimestamp(50);
    Assertions.assertIterableEquals(
        ImmutableList.of(
            new StreamRecord<>(expectedInOrder0.build(), 70),
            new StreamRecord<>(expectedInOrder1.build(), 70),
            new StreamRecord<>(expectedOutOrder0.build(), 100),
            new StreamRecord<>(expectedOutOrder1.build(), 100)),
        harness.extractOutputStreamRecords());
  }

  @Test
  public void rightJoin_enabled_withForeignKey() throws Exception {
    setUpHarness(Options.builder().setRightOuterJoin(true));
    harness.processElement2(withLogTimestamp(VIEW1_DELIVERYLOG, 20), 20);
    harness.processElement2(withLogTimestamp(VIEW2_DELIVERYLOG, 50), 50);

    // One in and one out of order request events.
    harness.processBothWatermarks(new Watermark(50));
    Assertions.assertEquals(1, harness.numKeyedStateEntries());
    Assertions.assertEquals(2, harness.numEventTimeTimers());
    Assertions.assertEquals(0, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(2, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceCandidates.entries()));

    // No output
    Assertions.assertIterableEquals(ImmutableList.of(), harness.extractOutputStreamRecords());

    // Rest of the views.  Triggers inference.
    harness.processBothWatermarks(new Watermark(100));
    Assertions.assertEquals(0, harness.numEventTimeTimers());
    Assertions.assertEquals(0, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceCandidates.entries()));

    // Verify output.
    List<TinyEvent.Builder> expectedInOrderList =
        function.join.apply(TinyEvent.newBuilder(), VIEW1_DELIVERYLOG, noErrorLogger);
    Assertions.assertEquals(2, expectedInOrderList.size());
    TinyEvent.Builder expectedInOrder0 = expectedInOrderList.get(0);
    expectedInOrder0.setLogTimestamp(20);
    TinyEvent.Builder expectedInOrder1 = expectedInOrderList.get(1);
    expectedInOrder1.setLogTimestamp(20);
    List<TinyEvent.Builder> expectedOutOrderList =
        function.join.apply(TinyEvent.newBuilder(), VIEW2_DELIVERYLOG, noErrorLogger);
    Assertions.assertEquals(2, expectedOutOrderList.size());
    TinyEvent.Builder expectedOutOrder0 = expectedOutOrderList.get(0);
    expectedOutOrder0.setLogTimestamp(50);
    TinyEvent.Builder expectedOutOrder1 = expectedOutOrderList.get(1);
    expectedOutOrder1.setLogTimestamp(50);
    Assertions.assertIterableEquals(
        ImmutableList.of(
            new StreamRecord<>(expectedInOrder0.build(), 70),
            new StreamRecord<>(expectedInOrder1.build(), 70),
            new StreamRecord<>(expectedOutOrder0.build(), 100),
            new StreamRecord<>(expectedOutOrder1.build(), 100)),
        harness.extractOutputStreamRecords());
  }

  @Test
  public void skipJoin() throws Exception {
    setUpHarness(Options.builder().setSkipJoin(true));
    harness.processElement2(withLogTimestamp(DELIVERYLOG3, 20), 20);
    harness.processElement2(withLogTimestamp(DELIVERYLOG4, 50), 50);

    // Rest of the views.  Triggers inference.
    harness.processBothWatermarks(new Watermark(50));
    Assertions.assertEquals(0, harness.numEventTimeTimers());
    Assertions.assertEquals(0, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceCandidates.entries()));

    // Verify output.
    List<TinyEvent.Builder> expectedInOrderList =
        function.join.apply(TinyEvent.newBuilder(), DELIVERYLOG3, noErrorLogger);
    Assertions.assertEquals(2, expectedInOrderList.size());
    TinyEvent.Builder expectedInOrder0 = expectedInOrderList.get(0);
    expectedInOrder0.setLogTimestamp(20);
    TinyEvent.Builder expectedInOrder1 = expectedInOrderList.get(1);
    expectedInOrder1.setLogTimestamp(20);
    List<TinyEvent.Builder> expectedOutOrderList =
        function.join.apply(TinyEvent.newBuilder(), DELIVERYLOG4, noErrorLogger);
    Assertions.assertEquals(2, expectedOutOrderList.size());
    TinyEvent.Builder expectedOutOrder0 = expectedOutOrderList.get(0);
    expectedOutOrder0.setLogTimestamp(50);
    TinyEvent.Builder expectedOutOrder1 = expectedOutOrderList.get(1);
    expectedOutOrder1.setLogTimestamp(50);
    Assertions.assertIterableEquals(
        ImmutableList.of(
            new StreamRecord<>(expectedInOrder0.build(), 20),
            new StreamRecord<>(expectedInOrder1.build(), 20),
            new StreamRecord<>(expectedOutOrder0.build(), 50),
            new StreamRecord<>(expectedOutOrder1.build(), 50)),
        harness.extractOutputStreamRecords());
  }

  private static TinyEvent withLogTimestamp(TinyEvent event, long logTimestamp) {
    return event.toBuilder().setLogTimestamp(logTimestamp).build();
  }

  private static TinyDeliveryLog withLogTimestamp(TinyDeliveryLog event, long logTimestamp) {
    return event.toBuilder().setLogTimestamp(logTimestamp).build();
  }

  private static TinyEvent.Builder createFlatView(String viewId, String luid) {
    return TinyEvent.newBuilder().setViewId(viewId).setLogUserId(luid);
  }

  private static TinyDeliveryLog.Builder createTinyDeliveryLog(String requestId, String luid) {
    TinyDeliveryLog.Builder builder = TinyDeliveryLog.newBuilder().setRequestId(requestId);
    builder.setLogUserId(luid);
    return builder;
  }

  private static TinyDeliveryLog.Builder createTinyDeliveryLog(
      String requestId, String viewId, String luid) {
    return createTinyDeliveryLog(requestId, luid).setViewId(viewId);
  }

  private static TinyInsertion.Builder createInsertion(String insertionId, String contentId) {
    return TinyInsertion.newBuilder().setInsertionId(insertionId).setContentId(contentId);
  }

  private static TinyDeliveryLog.Builder addInsertions(
      TinyDeliveryLog.Builder builder, TinyDeliveryLog.TinyInsertion.Builder... insertions) {
    for (int i = 0; i < insertions.length; i++) {
      builder.addResponseInsertion(insertions[i]);
    }
    return builder;
  }
}
