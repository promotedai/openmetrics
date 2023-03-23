package ai.promoted.metrics.logprocessor.common.functions.inferred;

import ai.promoted.metrics.error.MismatchError;
import ai.promoted.metrics.logprocessor.common.flink.operator.InferenceOperator;
import ai.promoted.metrics.logprocessor.common.util.StringUtil;
import ai.promoted.proto.event.TinyEvent;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.twitter.chill.protobuf.ProtobufSerializer;
import java.time.Duration;
import java.util.List;
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

/** Tests mainly for the underlying ContentId join implementation. */
public class ImpressionActionProcessFunctionTest {
  private static final TinyEvent IMP1 = createFlatImpression("imp1", "content1", "user").build();
  private static final TinyEvent IMP2 = createFlatImpression("imp2", "content2", "user").build();
  private static final TinyEvent IMP3 = createFlatImpression("imp3", "content3", "user").build();

  private static final TinyEvent VIEWID_IMP1 =
      createFlatImpression("imp1", "content1", "user").setViewId("view1").build();
  private static final TinyEvent VIEWID_IMP2 =
      createFlatImpression("imp2", "content2", "user").setViewId("view1").build();
  private static final TinyEvent VIEWID_IMP3 =
      createFlatImpression("imp3", "content3", "user").setViewId("view1").build();

  private static final TinyEvent CONTENT1_ACTION = createAction("act1", "content1", "user").build();
  private static final TinyEvent CONTENT2_ACTION = createAction("act2", "content2", "user").build();
  private static final TinyEvent CONTENT3_ACTION = createAction("act3", "content3", "user").build();

  private static final TinyEvent VIEWID_CONTENT1_ACTION =
      createAction("act1", "content1", "user").setViewId("view1").build();
  private static final TinyEvent VIEWID_CONTENT2_ACTION =
      createAction("act2", "content2", "user").setViewId("view1").build();
  private static final TinyEvent VIEWID_CONTENT3_ACTION =
      createAction("act3", "content3", "user").setViewId("view1").build();

  // Will not get joined since content/insertion/impression IDs are missing.
  private static final TinyEvent ACTION3 = createAction("act3", "user").build();
  private static final TinyEvent ACTION4 = createAction("act4", "user").build();

  private static BiConsumer<OutputTag<MismatchError>, MismatchError> noErrorLogger =
      (tag, error) -> {};

  private KeyedTwoInputStreamOperatorTestHarness<
          Tuple2<Long, String>, TinyEvent, TinyEvent, TinyEvent>
      harness;
  private InferenceOperator<TinyEvent> operator;
  private ImpressionActionProcessFunction function;

  @BeforeEach
  public void setUp() throws Exception {
    setUpEnv(ImmutableList.of());
  }

  private void setUpEnv(List<String> otherDimensionIds) throws Exception {
    function =
        new ImpressionActionProcessFunction(
            Options.builder()
                .setMaxTime(Duration.ofMillis(100))
                .setMaxOutOfOrder(Duration.ofMillis(50))
                .setCheckLateness(true)
                .setIdJoinDurationMultiplier(1)
                .build(),
            otherDimensionIds);
    operator = InferenceOperator.of(function, false);
    harness =
        new KeyedTwoInputStreamOperatorTestHarness<
            Tuple2<Long, String>, TinyEvent, TinyEvent, TinyEvent>(
            operator,
            // For purposes of testing, we're staying with in a single platform, so don't need to
            // add it to our key.
            flat -> Tuple2.of(flat.getPlatformId(), flat.getLogUserId()),
            flat -> Tuple2.of(flat.getPlatformId(), flat.getLogUserId()),
            Types.TUPLE(Types.LONG, Types.STRING));
    ExecutionConfig config = harness.getExecutionConfig();
    config.registerTypeWithKryoSerializer(TinyEvent.class, ProtobufSerializer.class);
    harness.setup();
    harness.open();
  }

  @AfterEach
  public void tearDown() throws Exception {
    harness.close();
  }

  @Test
  public void inferredJoin_withContentId() throws Exception {
    harness.processElement1(withLogTimestamp(IMP1, 10), 10);
    harness.processElement2(withLogTimestamp(CONTENT2_ACTION, 20), 20);
    harness.processElement2(withLogTimestamp(CONTENT1_ACTION, 50), 50);
    harness.processElement2(withLogTimestamp(CONTENT3_ACTION, 60), 60);

    // One in and one out of order action events.
    harness.processBothWatermarks(new Watermark(50));
    Assertions.assertEquals(3, harness.numKeyedStateEntries());
    Assertions.assertEquals(3, harness.numEventTimeTimers());
    Assertions.assertEquals(1, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(1, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(3, Iterables.size(function.inferenceCandidates.entries()));

    // No output
    Assertions.assertIterableEquals(ImmutableList.of(), harness.extractOutputStreamRecords());

    harness.processElement1(withLogTimestamp(IMP2, 80), 80);
    harness.processElement1(withLogTimestamp(IMP3, 90), 90);

    // Rest of the impressions.  Triggers inference.
    harness.processBothWatermarks(new Watermark(100));
    Assertions.assertEquals(3, harness.numEventTimeTimers());
    Assertions.assertEquals(3, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(3, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(1, Iterables.size(function.inferenceCandidates.entries()));

    // Verify output.
    TinyEvent.Builder expectedInOrder =
        function.join.apply(IMP1.toBuilder(), CONTENT1_ACTION, noErrorLogger).get(0);
    expectedInOrder.setLogTimestamp(50);
    Assertions.assertIterableEquals(
        ImmutableList.of(new StreamRecord<>(expectedInOrder.build(), 100)),
        harness.extractOutputStreamRecords());

    // No events within timer bucket.  Triggers inference.
    harness.processBothWatermarks(new Watermark(150));
    Assertions.assertEquals(2, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(2, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceCandidates.entries()));

    // Verify output.
    TinyEvent.Builder expectedOutOrder =
        function.join.apply(IMP3.toBuilder(), CONTENT3_ACTION, noErrorLogger).get(0);
    expectedOutOrder.setLogTimestamp(60);
    Assertions.assertIterableEquals(
        ImmutableList.of(
            new StreamRecord<>(expectedInOrder.build(), 100),
            new StreamRecord<>(expectedOutOrder.build(), 110)),
        harness.extractOutputStreamRecords());

    // No events moving to next timer bucket.
    harness.processBothWatermarks(new Watermark(200));
    Assertions.assertEquals(0, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceCandidates.entries()));

    // Verify output.
    Assertions.assertIterableEquals(
        ImmutableList.of(
            new StreamRecord<>(expectedInOrder.build(), 100),
            new StreamRecord<>(expectedOutOrder.build(), 110)),
        harness.extractOutputStreamRecords());
    Assertions.assertEquals(1, harness.getSideOutput(function.getDroppedEventsTag()).size());
    Assertions.assertNull(harness.getSideOutput(ImpressionActionProcessFunction.LATE_EVENTS_TAG));
    Assertions.assertNull(
        harness.getSideOutput(ImpressionActionProcessFunction.DUPLICATE_EVENTS_TAG));
  }

  @Test
  public void inferredJoin_withContentId_withViewId() throws Exception {
    harness.processElement1(withLogTimestamp(VIEWID_IMP1, 10), 10);
    harness.processElement2(withLogTimestamp(VIEWID_CONTENT2_ACTION, 20), 20);
    harness.processElement2(withLogTimestamp(VIEWID_CONTENT1_ACTION, 50), 50);
    harness.processElement2(withLogTimestamp(VIEWID_CONTENT3_ACTION, 60), 60);

    // One in and one out of order action events.
    harness.processBothWatermarks(new Watermark(50));
    Assertions.assertEquals(3, harness.numKeyedStateEntries());
    Assertions.assertEquals(3, harness.numEventTimeTimers());
    Assertions.assertEquals(1, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(2, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(3, Iterables.size(function.inferenceCandidates.entries()));

    // No output
    Assertions.assertIterableEquals(ImmutableList.of(), harness.extractOutputStreamRecords());

    harness.processElement1(withLogTimestamp(VIEWID_IMP2, 80), 80);
    harness.processElement1(withLogTimestamp(VIEWID_IMP3, 90), 90);

    // Rest of the impressions.
    harness.processBothWatermarks(new Watermark(100));
    Assertions.assertEquals(3, harness.numEventTimeTimers());
    Assertions.assertEquals(3, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(4, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(1, Iterables.size(function.inferenceCandidates.entries()));

    // Verify output.
    TinyEvent.Builder expectedInOrder =
        function.join.apply(IMP1.toBuilder(), VIEWID_CONTENT1_ACTION, noErrorLogger).get(0);
    expectedInOrder.setLogTimestamp(50);
    Assertions.assertIterableEquals(
        ImmutableList.of(new StreamRecord<>(expectedInOrder.build(), 100)),
        harness.extractOutputStreamRecords());

    // No events within timer bucket.  Triggers inference.
    harness.processBothWatermarks(new Watermark(150));
    Assertions.assertEquals(2, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(3, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceCandidates.entries()));

    // Verify output.
    TinyEvent.Builder expectedOutOrder =
        function.join.apply(IMP3.toBuilder(), VIEWID_CONTENT3_ACTION, noErrorLogger).get(0);
    expectedOutOrder.setLogTimestamp(60);
    Assertions.assertIterableEquals(
        ImmutableList.of(
            new StreamRecord<>(expectedInOrder.build(), 100),
            new StreamRecord<>(expectedOutOrder.build(), 110)),
        harness.extractOutputStreamRecords());

    // No events moving to next timer bucket.
    harness.processBothWatermarks(new Watermark(200));
    Assertions.assertEquals(0, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceCandidates.entries()));

    // Verify output.
    Assertions.assertIterableEquals(
        ImmutableList.of(
            new StreamRecord<>(expectedInOrder.build(), 100),
            new StreamRecord<>(expectedOutOrder.build(), 110)),
        harness.extractOutputStreamRecords());
    Assertions.assertEquals(1, harness.getSideOutput(function.getDroppedEventsTag()).size());
    Assertions.assertNull(harness.getSideOutput(ImpressionActionProcessFunction.LATE_EVENTS_TAG));
    Assertions.assertNull(
        harness.getSideOutput(ImpressionActionProcessFunction.DUPLICATE_EVENTS_TAG));
  }

  @Test
  public void inferredJoin_withoutContentId_noOutput() throws Exception {
    harness.processElement1(withLogTimestamp(IMP1, 10), 10);
    harness.processElement2(withLogTimestamp(ACTION3, 20), 20);
    harness.processElement2(withLogTimestamp(ACTION4, 50), 50);

    // One in and one out of order action events.
    harness.processBothWatermarks(new Watermark(50));
    Assertions.assertEquals(3, harness.numKeyedStateEntries());
    Assertions.assertEquals(3, harness.numEventTimeTimers());
    Assertions.assertEquals(1, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(1, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(2, Iterables.size(function.inferenceCandidates.entries()));

    // No output
    Assertions.assertIterableEquals(ImmutableList.of(), harness.extractOutputStreamRecords());

    harness.processElement1(withLogTimestamp(IMP2, 80), 80);
    harness.processElement1(withLogTimestamp(IMP3, 90), 90);

    // Rest of the impressions.  Triggers inference.
    harness.processBothWatermarks(new Watermark(100));
    Assertions.assertEquals(3, harness.numEventTimeTimers());
    Assertions.assertEquals(3, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(3, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceCandidates.entries()));

    // Verify output.
    TinyEvent.Builder expectedInOrder =
        function.join.apply(IMP1.toBuilder(), ACTION3, noErrorLogger).get(0);
    expectedInOrder.setLogTimestamp(20);
    Assertions.assertEquals(0, harness.extractOutputStreamRecords().size());

    // No events within timer bucket.  Triggers inference.
    harness.processBothWatermarks(new Watermark(150));
    Assertions.assertEquals(2, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(2, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceCandidates.entries()));

    // Verify output.
    TinyEvent.Builder expectedOutOrder =
        function.join.apply(IMP2.toBuilder(), ACTION4, noErrorLogger).get(0);
    expectedOutOrder.setLogTimestamp(50);
    Assertions.assertEquals(0, harness.extractOutputStreamRecords().size());

    // No events moving to next timer bucket.
    harness.processBothWatermarks(new Watermark(200));
    Assertions.assertEquals(0, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceCandidates.entries()));
    Assertions.assertEquals(2, harness.getSideOutput(function.getDroppedEventsTag()).size());
    Assertions.assertNull(harness.getSideOutput(ImpressionActionProcessFunction.LATE_EVENTS_TAG));
    Assertions.assertNull(
        harness.getSideOutput(ImpressionActionProcessFunction.DUPLICATE_EVENTS_TAG));

    // Verify output.
    Assertions.assertEquals(0, harness.extractOutputStreamRecords().size());
    Assertions.assertEquals(2, harness.getSideOutput(function.getDroppedEventsTag()).size());
    Assertions.assertNull(harness.getSideOutput(ImpressionActionProcessFunction.LATE_EVENTS_TAG));
    Assertions.assertNull(
        harness.getSideOutput(ImpressionActionProcessFunction.DUPLICATE_EVENTS_TAG));
  }

  @Test
  public void inferredJoin_storeToItem_noOtherContentFieldsSelected() throws Exception {
    harness.processElement1(withLogTimestamp(IMP1, 10), 10);
    // Treats content1 as the storeId and content2 as the itemId.
    harness.processElement2(
        withLogTimestamp(
            CONTENT2_ACTION.toBuilder()
                .putOtherContentIds(StringUtil.hash("storeId"), "content1")
                .build(),
            20),
        20);

    // No output because otherDimensionIds does not contain "storeId".
    harness.processBothWatermarks(new Watermark(200));
    Assertions.assertTrue(harness.extractOutputStreamRecords().isEmpty());
    Assertions.assertEquals(0, harness.numEventTimeTimers());
    Assertions.assertEquals(0, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceCandidates.entries()));
    Assertions.assertEquals(1, harness.getSideOutput(function.getDroppedEventsTag()).size());
    Assertions.assertNull(harness.getSideOutput(ImpressionActionProcessFunction.LATE_EVENTS_TAG));
    Assertions.assertNull(
        harness.getSideOutput(ImpressionActionProcessFunction.DUPLICATE_EVENTS_TAG));
  }

  // Use case: first page is a list of stores.  The checkout eventually happens on a specific item
  // in the store.
  @Test
  public void inferredJoin_storeToItem() throws Exception {
    harness.close();
    setUpEnv(ImmutableList.of("storeId"));

    harness.processElement1(withLogTimestamp(IMP1, 10), 10);
    // Treats content1 as the storeId and content2 as the itemId.
    harness.processElement2(
        withLogTimestamp(
            CONTENT2_ACTION.toBuilder()
                .putOtherContentIds(StringUtil.hash("storeId"), "content1")
                .build(),
            20),
        20);

    // One in and one out of order action events.
    harness.processBothWatermarks(new Watermark(50));
    Assertions.assertEquals(3, harness.numKeyedStateEntries());
    Assertions.assertEquals(2, harness.numEventTimeTimers());
    Assertions.assertEquals(1, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(1, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(1, Iterables.size(function.inferenceCandidates.entries()));

    // No output
    Assertions.assertIterableEquals(ImmutableList.of(), harness.extractOutputStreamRecords());

    // Rest of the impressions.  Triggers inference.
    harness.processBothWatermarks(new Watermark(100));
    Assertions.assertEquals(1, harness.numEventTimeTimers());
    Assertions.assertEquals(1, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(1, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceCandidates.entries()));

    // Verify output.
    TinyEvent.Builder expectedInOrder =
        function.join.apply(IMP1.toBuilder(), CONTENT2_ACTION, noErrorLogger).get(0);
    expectedInOrder.setLogTimestamp(20);
    expectedInOrder.putOtherContentIds(StringUtil.hash("storeId"), "content1");
    Assertions.assertIterableEquals(
        ImmutableList.of(new StreamRecord<>(expectedInOrder.build(), 70)),
        harness.extractOutputStreamRecords());

    harness.processBothWatermarks(new Watermark(200));
    Assertions.assertEquals(0, harness.numEventTimeTimers());
    Assertions.assertEquals(0, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceCandidates.entries()));
    Assertions.assertNull(harness.getSideOutput(function.getDroppedEventsTag()));
    Assertions.assertNull(harness.getSideOutput(ImpressionActionProcessFunction.LATE_EVENTS_TAG));
    Assertions.assertNull(
        harness.getSideOutput(ImpressionActionProcessFunction.DUPLICATE_EVENTS_TAG));
  }

  @Test
  public void inferredJoin_itemToStore_noOtherContentFieldsSelected() throws Exception {
    // Treats content1 as the itemId and content2 as the storeId.
    harness.processElement1(
        withLogTimestamp(
            IMP1.toBuilder().putOtherContentIds(StringUtil.hash("storeId"), "content2").build(),
            10),
        10);
    harness.processElement2(withLogTimestamp(CONTENT2_ACTION, 20), 20);

    // No output because otherDimensionIds does not contain "storeId".
    harness.processBothWatermarks(new Watermark(200));
    Assertions.assertTrue(harness.extractOutputStreamRecords().isEmpty());
    Assertions.assertEquals(0, harness.numEventTimeTimers());
    Assertions.assertEquals(0, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceCandidates.entries()));
    Assertions.assertEquals(1, harness.getSideOutput(function.getDroppedEventsTag()).size());
    Assertions.assertNull(harness.getSideOutput(ImpressionActionProcessFunction.LATE_EVENTS_TAG));
    Assertions.assertNull(
        harness.getSideOutput(ImpressionActionProcessFunction.DUPLICATE_EVENTS_TAG));
  }

  // Use case: first page is a list of items.  The checkout eventually happens on the store for the
  // item.
  @Test
  public void inferredJoin_itemToStore() throws Exception {
    harness.close();
    setUpEnv(ImmutableList.of("storeId"));

    // Treats content1 as the itemId and content2 as the storeId.
    harness.processElement1(
        withLogTimestamp(
            IMP1.toBuilder().putOtherContentIds(StringUtil.hash("storeId"), "content2").build(),
            10),
        10);
    harness.processElement2(withLogTimestamp(CONTENT2_ACTION, 20), 20);

    // One in and one out of order action events.
    harness.processBothWatermarks(new Watermark(50));
    Assertions.assertEquals(3, harness.numKeyedStateEntries());
    Assertions.assertEquals(2, harness.numEventTimeTimers());
    Assertions.assertEquals(1, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(2, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(1, Iterables.size(function.inferenceCandidates.entries()));

    // No output
    Assertions.assertIterableEquals(ImmutableList.of(), harness.extractOutputStreamRecords());

    // Rest of the impressions.  Triggers inference.
    harness.processBothWatermarks(new Watermark(100));
    Assertions.assertEquals(1, harness.numEventTimeTimers());
    Assertions.assertEquals(1, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(2, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceCandidates.entries()));

    // Verify output.
    TinyEvent.Builder expectedInOrder =
        function.join.apply(IMP1.toBuilder(), CONTENT2_ACTION, noErrorLogger).get(0);
    expectedInOrder.setLogTimestamp(20);
    expectedInOrder.putOtherContentIds(StringUtil.hash("storeId"), "content2");
    Assertions.assertIterableEquals(
        ImmutableList.of(new StreamRecord<>(expectedInOrder.build(), 70)),
        harness.extractOutputStreamRecords());

    harness.processBothWatermarks(new Watermark(200));
    Assertions.assertEquals(0, harness.numEventTimeTimers());
    Assertions.assertEquals(0, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceCandidates.entries()));
    Assertions.assertNull(harness.getSideOutput(function.getDroppedEventsTag()));
    Assertions.assertNull(harness.getSideOutput(ImpressionActionProcessFunction.LATE_EVENTS_TAG));
    Assertions.assertNull(
        harness.getSideOutput(ImpressionActionProcessFunction.DUPLICATE_EVENTS_TAG));
  }

  @Test
  public void inferredJoin_partialCleanup() throws Exception {
    harness.processElement1(withLogTimestamp(IMP1, 10), 10);

    // One impression early.
    harness.processBothWatermarks(new Watermark(100));
    Assertions.assertEquals(2, harness.numKeyedStateEntries());
    Assertions.assertEquals(1, harness.numEventTimeTimers());
    Assertions.assertEquals(1, Iterables.size(function.idToLeft.entries()));
    Assertions.assertIterableEquals(ImmutableList.of("imp1"), function.idToLeft.keys());
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(1, Iterables.size(function.inferenceScopes.entries()));
    function
        .inferenceScopes
        .values()
        .forEach(v -> Assertions.assertIterableEquals(ImmutableList.of(Tuple2.of(10L, "imp1")), v));
    Assertions.assertEquals(0, Iterables.size(function.inferenceCandidates.entries()));

    harness.processElement1(withLogTimestamp(IMP1, 110), 110);
    harness.processElement1(withLogTimestamp(IMP2, 120), 120);

    // Another impression.
    harness.processBothWatermarks(new Watermark(200));
    Assertions.assertEquals(1, Iterables.size(function.idToLeft.entries()));
    Assertions.assertIterableEquals(ImmutableList.of("imp2"), function.idToLeft.keys());
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(1, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertIterableEquals(
        ImmutableList.of(
            Maps.immutableEntry("content2", ImmutableList.of(Tuple2.of(120L, "imp2")))),
        function.inferenceScopes.entries());
    Assertions.assertEquals(0, Iterables.size(function.inferenceCandidates.entries()));

    harness.processElement1(withLogTimestamp(IMP1, 210), 210);
    harness.processElement1(withLogTimestamp(IMP2, 220), 220);
    harness.processElement1(withLogTimestamp(IMP3, 230), 230);
    harness.processElement2(withLogTimestamp(CONTENT3_ACTION, 230), 230);

    // Last impression and an inferrable action.  Triggers inference.
    harness.processBothWatermarks(new Watermark(300));
    Assertions.assertEquals(2, Iterables.size(function.idToLeft.entries()));
    Assertions.assertIterableEquals(ImmutableList.of("imp1", "imp3"), function.idToLeft.keys());
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(2, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertIterableEquals(
        ImmutableList.of(
            Maps.immutableEntry("content3", ImmutableList.of(Tuple2.of(230L, "imp3"))),
            Maps.immutableEntry("content1", ImmutableList.of(Tuple2.of(210L, "imp1")))),
        function.inferenceScopes.entries());
    Assertions.assertEquals(0, Iterables.size(function.inferenceCandidates.entries()));

    // Verify output.
    TinyEvent.Builder expectedInOrder =
        function.join.apply(IMP3.toBuilder(), CONTENT3_ACTION, noErrorLogger).get(0);
    expectedInOrder.setLogTimestamp(230);
    Assertions.assertIterableEquals(
        ImmutableList.of(new StreamRecord<>(expectedInOrder.build(), 280)),
        harness.extractOutputStreamRecords());

    // No events moving to next timer bucket.
    harness.processBothWatermarks(new Watermark(400));
    Assertions.assertEquals(0, Iterables.size(function.idToLeft.entries()));
    Assertions.assertEquals(0, Iterables.size(function.ooIdJoin.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceScopes.entries()));
    Assertions.assertEquals(0, Iterables.size(function.inferenceCandidates.entries()));

    // Verify output.
    Assertions.assertIterableEquals(
        ImmutableList.of(new StreamRecord<>(expectedInOrder.build(), 280)),
        harness.extractOutputStreamRecords());
    Assertions.assertNull(harness.getSideOutput(function.getDroppedEventsTag()));
    Assertions.assertNull(harness.getSideOutput(ImpressionActionProcessFunction.LATE_EVENTS_TAG));
    Assertions.assertEquals(
        2, harness.getSideOutput(ImpressionActionProcessFunction.DUPLICATE_EVENTS_TAG).size());
  }

  private static TinyEvent withLogTimestamp(TinyEvent event, long logTimestamp) {
    return event.toBuilder().setLogTimestamp(logTimestamp).build();
  }

  private static TinyEvent.Builder createFlatImpression(String id, String contentId, String luid) {
    return createImpression(id, contentId, luid);
  }

  private static TinyEvent.Builder createImpression(String id, String contentId, String luid) {
    return TinyEvent.newBuilder()
        .setPlatformId(1L)
        .setLogUserId(luid)
        .setImpressionId(id)
        .setContentId(contentId);
  }

  private static TinyEvent.Builder createAction(String id, String luid) {
    return TinyEvent.newBuilder().setPlatformId(1L).setLogUserId(luid).setActionId(id);
  }

  private static TinyEvent.Builder createAction(String id, String contentId, String luid) {
    TinyEvent.Builder builder = createAction(id, luid);
    return builder.setContentId(contentId);
  }
}
