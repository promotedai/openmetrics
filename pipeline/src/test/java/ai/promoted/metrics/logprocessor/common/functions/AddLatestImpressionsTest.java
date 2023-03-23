package ai.promoted.metrics.logprocessor.common.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.promoted.metrics.logprocessor.common.constant.Constants;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.JoinedEvent;
import ai.promoted.proto.event.JoinedIdentifiers;
import ai.promoted.proto.event.LatestImpression;
import ai.promoted.proto.event.LatestImpressions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.twitter.chill.protobuf.ProtobufSerializer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

public class AddLatestImpressionsTest {

  private static Duration EVENT_API_TIMESTAMP_DELAY = Duration.ofMillis(100);
  private static Duration KAFKA_TIMESTAMP_DELAY = Duration.ofMillis(110);

  private static JoinedEvent createFlatAction(
      String impressionId, String contentId, long clientLogTimestamp) {
    return JoinedEvent.newBuilder()
        .setIds(JoinedIdentifiers.newBuilder().setImpressionId(impressionId))
        .setResponseInsertion(Insertion.newBuilder().setContentId(contentId))
        .setAction(
            Action.newBuilder()
                .setTiming(
                    Timing.newBuilder()
                        .setClientLogTimestamp(clientLogTimestamp)
                        .setEventApiTimestamp(
                            clientLogTimestamp + EVENT_API_TIMESTAMP_DELAY.toMillis())))
        .build();
  }

  private static JoinedEvent addFlatImpressions(
      JoinedEvent joinedEvent, List<LatestImpression> latestImpressions) {
    return joinedEvent.toBuilder()
        .setLatestImpressions(LatestImpressions.newBuilder().addAllImpression(latestImpressions))
        .build();
  }

  private JoinedEvent createFlatImpression(
      String impressionId, String contentId, long clientLogTimestamp) {
    return JoinedEvent.newBuilder()
        .setIds(JoinedIdentifiers.newBuilder().setImpressionId(impressionId))
        .setResponseInsertion(Insertion.newBuilder().setContentId(contentId))
        .setImpression(
            Impression.newBuilder()
                .setTiming(
                    Timing.newBuilder()
                        .setClientLogTimestamp(clientLogTimestamp)
                        .setEventApiTimestamp(
                            clientLogTimestamp + EVENT_API_TIMESTAMP_DELAY.toMillis())))
        .build();
  }

  private KeyedTwoInputStreamOperatorTestHarness createHarness(
      AddLatestImpressions addLatestImpressions) throws Exception {
    KeyedTwoInputStreamOperatorTestHarness harness =
        ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(
            addLatestImpressions,
            joinedEvent ->
                Tuple2.of(
                    joinedEvent.getIds().getPlatformId(), joinedEvent.getIds().getLogUserId()),
            joinedEvent ->
                Tuple2.of(
                    joinedEvent.getIds().getPlatformId(), joinedEvent.getIds().getLogUserId()),
            Types.TUPLE(Types.LONG, Types.STRING));
    harness.setTimeCharacteristic(TimeCharacteristic.EventTime);
    harness
        .getExecutionConfig()
        .registerTypeWithKryoSerializer(LatestImpression.class, ProtobufSerializer.class);
    harness
        .getExecutionConfig()
        .registerTypeWithKryoSerializer(JoinedEvent.class, ProtobufSerializer.class);
    return harness;
  }

  private static void assertSize(int expected, ListState<?> listState) throws Exception {
    assertEquals(expected, Iterables.size(listState.get()));
  }

  /** Simplifies specifying a kafka delay to the event time. */
  private static void processElement1(
      KeyedTwoInputStreamOperatorTestHarness harness, JoinedEvent action) throws Exception {
    harness.processElement1(
        action,
        action.getAction().getTiming().getClientLogTimestamp() + KAFKA_TIMESTAMP_DELAY.toMillis());
  }

  /** Simplifies specifying a kafka delay to the event time. */
  private static void processElement2(
      KeyedTwoInputStreamOperatorTestHarness harness, JoinedEvent impression) throws Exception {
    harness.processElement2(
        impression,
        impression.getImpression().getTiming().getClientLogTimestamp()
            + KAFKA_TIMESTAMP_DELAY.toMillis());
  }

  // These tests with a small number of events assert state since the state is important.

  @Test
  public void noEvents() throws Exception {
    AddLatestImpressions addLatestImpressions =
        new AddLatestImpressions(Duration.ofSeconds(1), true);
    KeyedTwoInputStreamOperatorTestHarness harness = createHarness(addLatestImpressions);

    harness.processBothWatermarks(new Watermark(2000));

    assertEquals(ImmutableList.of(), harness.extractOutputValues());

    // Assert state after cleanup timers.
    assertEquals(0, harness.numEventTimeTimers());
    harness.processBothWatermarks(
        new Watermark(TimeUnit.HOURS.toMillis(Constants.SESSION_DURATION_HOURS + 1)));
    assertEquals(0, harness.numEventTimeTimers());
  }

  @Test
  public void oneImpression() throws Exception {
    AddLatestImpressions addLatestImpressions =
        new AddLatestImpressions(Duration.ofSeconds(1), true);
    KeyedTwoInputStreamOperatorTestHarness harness = createHarness(addLatestImpressions);

    processElement2(harness, createFlatImpression("123", "itemABC", 500));

    assertEquals(2, harness.numEventTimeTimers());
    assertSize(0, addLatestImpressions.actions);
    assertSize(1, addLatestImpressions.impressions);
    assertSize(0, addLatestImpressions.actionedImpressions);

    harness.processBothWatermarks(new Watermark(2000));
    assertEquals(ImmutableList.of(), harness.extractOutputValues());

    // Assert state after cleanup timers.
    assertEquals(2, harness.numEventTimeTimers());
    assertSize(0, addLatestImpressions.actions);
    assertSize(1, addLatestImpressions.impressions);
    assertSize(0, addLatestImpressions.actionedImpressions);

    harness.processBothWatermarks(new Watermark(120000));
    assertEquals(1, harness.numEventTimeTimers());
    assertSize(0, addLatestImpressions.actions);
    assertSize(1, addLatestImpressions.impressions);
    assertSize(0, addLatestImpressions.actionedImpressions);

    harness.processBothWatermarks(
        new Watermark(TimeUnit.HOURS.toMillis(Constants.SESSION_DURATION_HOURS + 1)));
    assertEquals(0, harness.numEventTimeTimers());
    assertSize(0, addLatestImpressions.actions);
    assertSize(0, addLatestImpressions.impressions);
    assertSize(0, addLatestImpressions.actionedImpressions);
  }

  @Test
  public void multipleImpressionsVeryQuick() throws Exception {
    AddLatestImpressions addLatestImpressions =
        new AddLatestImpressions(Duration.ofSeconds(0), true);
    KeyedTwoInputStreamOperatorTestHarness harness = createHarness(addLatestImpressions);

    processElement2(harness, createFlatImpression("123", "itemABC", 500));
    processElement2(harness, createFlatImpression("234", "itemBCD", 600));
    processElement2(harness, createFlatImpression("345", "itemCDE", 700));

    assertEquals(2, harness.numEventTimeTimers());
    assertSize(0, addLatestImpressions.actions);
    assertSize(3, addLatestImpressions.impressions);
    assertSize(0, addLatestImpressions.actionedImpressions);

    harness.processBothWatermarks(new Watermark(1000));
    assertEquals(ImmutableList.of(), harness.extractOutputValues());

    // Assert state after cleanup timers.
    assertEquals(2, harness.numEventTimeTimers());
    assertSize(0, addLatestImpressions.actions);
    assertSize(3, addLatestImpressions.impressions);
    assertSize(0, addLatestImpressions.actionedImpressions);

    harness.processBothWatermarks(new Watermark(120000));
    assertEquals(1, harness.numEventTimeTimers());
    assertSize(0, addLatestImpressions.actions);
    assertSize(3, addLatestImpressions.impressions);
    assertSize(0, addLatestImpressions.actionedImpressions);

    harness.processBothWatermarks(
        new Watermark(TimeUnit.HOURS.toMillis(Constants.SESSION_DURATION_HOURS + 1)));
    assertEquals(0, harness.numEventTimeTimers());
    assertSize(0, addLatestImpressions.actions);
    assertSize(0, addLatestImpressions.impressions);
    assertSize(0, addLatestImpressions.actionedImpressions);
  }

  @Test
  public void multipleImpressionsAcrossWatermarks() throws Exception {
    AddLatestImpressions addLatestImpressions =
        new AddLatestImpressions(Duration.ofSeconds(1), true);
    KeyedTwoInputStreamOperatorTestHarness harness = createHarness(addLatestImpressions);

    processElement2(harness, createFlatImpression("123", "itemABC", 500));
    harness.processBothWatermarks(new Watermark(1000));
    processElement2(harness, createFlatImpression("234", "itemBCD", 1500));
    harness.processBothWatermarks(new Watermark(2000));
    processElement2(harness, createFlatImpression("345", "itemCDE", 2500));
    harness.processBothWatermarks(new Watermark(3000));

    assertEquals(2, harness.numEventTimeTimers());
    assertSize(0, addLatestImpressions.actions);
    assertSize(3, addLatestImpressions.impressions);
    assertSize(0, addLatestImpressions.actionedImpressions);

    harness.processBothWatermarks(new Watermark(4000));
    assertEquals(ImmutableList.of(), harness.extractOutputValues());

    // Assert state after cleanup timers.
    assertEquals(2, harness.numEventTimeTimers());
    assertSize(0, addLatestImpressions.actions);
    assertSize(3, addLatestImpressions.impressions);
    assertSize(0, addLatestImpressions.actionedImpressions);

    harness.processBothWatermarks(new Watermark(120000));
    assertEquals(1, harness.numEventTimeTimers());
    assertSize(0, addLatestImpressions.actions);
    assertSize(3, addLatestImpressions.impressions);
    assertSize(0, addLatestImpressions.actionedImpressions);

    harness.processBothWatermarks(
        new Watermark(TimeUnit.HOURS.toMillis(Constants.SESSION_DURATION_HOURS + 1)));
    assertEquals(0, harness.numEventTimeTimers());
    assertSize(0, addLatestImpressions.actions);
    assertSize(0, addLatestImpressions.impressions);
    assertSize(0, addLatestImpressions.actionedImpressions);
  }

  @Test
  public void multipleImpressionsSessionClears() throws Exception {
    AddLatestImpressions addLatestImpressions =
        new AddLatestImpressions(Duration.ofSeconds(1), true);
    KeyedTwoInputStreamOperatorTestHarness harness = createHarness(addLatestImpressions);

    processElement2(harness, createFlatImpression("123", "itemABC", 500));
    harness.processBothWatermarks(new Watermark(1000));
    long timestamp2 = TimeUnit.HOURS.toMillis(Constants.SESSION_DURATION_HOURS + 1);
    processElement2(harness, createFlatImpression("234", "itemBCD", timestamp2));
    harness.processBothWatermarks(new Watermark(timestamp2 + 1000));
    long timestamp3 = TimeUnit.HOURS.toMillis(Constants.SESSION_DURATION_HOURS * 2 + 3);
    processElement2(harness, createFlatImpression("345", "itemCDE", timestamp3));
    harness.processBothWatermarks(new Watermark(timestamp3 + 1000));

    assertEquals(2, harness.numEventTimeTimers());
    assertSize(0, addLatestImpressions.actions);
    assertSize(1, addLatestImpressions.impressions);
    assertSize(0, addLatestImpressions.actionedImpressions);

    harness.processBothWatermarks(new Watermark(timestamp3 + 2000));
    assertEquals(ImmutableList.of(), harness.extractOutputValues());

    // Assert state after cleanup timers.
    assertEquals(2, harness.numEventTimeTimers());
    assertSize(0, addLatestImpressions.actions);
    assertSize(1, addLatestImpressions.impressions);
    assertSize(0, addLatestImpressions.actionedImpressions);

    harness.processBothWatermarks(new Watermark(timestamp3 + 120000));
    assertEquals(1, harness.numEventTimeTimers());
    assertSize(0, addLatestImpressions.actions);
    assertSize(1, addLatestImpressions.impressions);
    assertSize(0, addLatestImpressions.actionedImpressions);

    harness.processBothWatermarks(
        new Watermark(timestamp3 + TimeUnit.HOURS.toMillis(Constants.SESSION_DURATION_HOURS + 1)));
    assertEquals(0, harness.numEventTimeTimers());
    assertSize(0, addLatestImpressions.actions);
    assertSize(0, addLatestImpressions.impressions);
    assertSize(0, addLatestImpressions.actionedImpressions);
  }

  @Test
  public void oneAction() throws Exception {
    AddLatestImpressions addLatestImpressions =
        new AddLatestImpressions(Duration.ofSeconds(1), true);
    KeyedTwoInputStreamOperatorTestHarness harness = createHarness(addLatestImpressions);

    processElement1(harness, createFlatAction("123", "itemABC", 1000));

    assertEquals(2, harness.numEventTimeTimers());
    assertSize(1, addLatestImpressions.actions);
    assertSize(0, addLatestImpressions.impressions);
    assertSize(1, addLatestImpressions.actionedImpressions);

    harness.processBothWatermarks(new Watermark(2001 + KAFKA_TIMESTAMP_DELAY.toMillis()));
    assertEquals(
        ImmutableList.of(
            addFlatImpressions(createFlatAction("123", "itemABC", 1000), ImmutableList.of())),
        harness.extractOutputValues());

    // Assert state after cleanup timers.
    assertEquals(1, harness.numEventTimeTimers());
    assertSize(0, addLatestImpressions.actions);
    assertSize(0, addLatestImpressions.impressions);
    assertSize(1, addLatestImpressions.actionedImpressions);

    harness.processBothWatermarks(
        new Watermark(TimeUnit.HOURS.toMillis(Constants.SESSION_DURATION_HOURS + 1)));
    assertEquals(0, harness.numEventTimeTimers());
    assertSize(0, addLatestImpressions.actions);
    assertSize(0, addLatestImpressions.impressions);
    assertSize(0, addLatestImpressions.actionedImpressions);
  }

  @Test
  public void multipleActionsSessionClears() throws Exception {
    AddLatestImpressions addLatestImpressions =
        new AddLatestImpressions(Duration.ofSeconds(1), true);
    KeyedTwoInputStreamOperatorTestHarness harness = createHarness(addLatestImpressions);

    processElement1(harness, createFlatAction("123", "itemABC", 1000));
    harness.processBothWatermarks(new Watermark(2001));
    long timestamp2 = TimeUnit.HOURS.toMillis(Constants.SESSION_DURATION_HOURS + 1);
    processElement1(harness, createFlatAction("234", "itemBCD", timestamp2));
    harness.processBothWatermarks(new Watermark(timestamp2 + 1001));
    long timestamp3 = TimeUnit.HOURS.toMillis(Constants.SESSION_DURATION_HOURS * 2 + 3);
    processElement1(harness, createFlatAction("345", "itemCDE", timestamp3));

    harness.processBothWatermarks(
        new Watermark(timestamp3 + 1001 + KAFKA_TIMESTAMP_DELAY.toMillis()));

    assertEquals(
        ImmutableList.of(
            addFlatImpressions(createFlatAction("123", "itemABC", 1000), ImmutableList.of()),
            addFlatImpressions(createFlatAction("234", "itemBCD", timestamp2), ImmutableList.of()),
            addFlatImpressions(createFlatAction("345", "itemCDE", timestamp3), ImmutableList.of())),
        harness.extractOutputValues());
    assertEquals(1, harness.numEventTimeTimers());
    assertSize(0, addLatestImpressions.actions);
    assertSize(0, addLatestImpressions.impressions);
    assertSize(1, addLatestImpressions.actionedImpressions);

    // Assert state after cleanup timers.
    assertEquals(1, harness.numEventTimeTimers());
    assertSize(0, addLatestImpressions.actions);
    assertSize(0, addLatestImpressions.impressions);
    assertSize(1, addLatestImpressions.actionedImpressions);

    harness.processBothWatermarks(new Watermark(TimeUnit.HOURS.toMillis(24)));
    assertEquals(0, harness.numEventTimeTimers());
    assertSize(0, addLatestImpressions.actions);
    assertSize(0, addLatestImpressions.impressions);
    assertSize(0, addLatestImpressions.actionedImpressions);
  }

  private void assertOneImpressionAndOneAction(Duration maxOutOfOrderness) throws Exception {
    AddLatestImpressions addLatestImpressions = new AddLatestImpressions(maxOutOfOrderness, true);
    KeyedTwoInputStreamOperatorTestHarness harness = createHarness(addLatestImpressions);

    processElement2(harness, createFlatImpression("123", "itemABC", 500));
    processElement1(harness, createFlatAction("123", "itemABC", 1000));

    assertEquals(3, harness.numEventTimeTimers());
    assertSize(1, addLatestImpressions.actions);
    assertSize(1, addLatestImpressions.impressions);
    assertSize(1, addLatestImpressions.actionedImpressions);

    harness.processBothWatermarks(
        new Watermark(999 + +KAFKA_TIMESTAMP_DELAY.toMillis() + maxOutOfOrderness.toMillis()));
    assertEquals(ImmutableList.of(), harness.extractOutputValues());

    harness.processBothWatermarks(
        new Watermark(1000 + KAFKA_TIMESTAMP_DELAY.toMillis() + maxOutOfOrderness.toMillis()));
    assertEquals(
        ImmutableList.of(
            addFlatImpressions(createFlatAction("123", "itemABC", 1000), ImmutableList.of())),
        harness.extractOutputValues());

    // Assert state after cleanup timers.
    assertEquals(2, harness.numEventTimeTimers());
    assertSize(0, addLatestImpressions.actions);
    assertSize(1, addLatestImpressions.impressions);
    assertSize(1, addLatestImpressions.actionedImpressions);

    harness.processBothWatermarks(new Watermark(120000));
    assertEquals(1, harness.numEventTimeTimers());
    assertSize(0, addLatestImpressions.actions);
    assertSize(1, addLatestImpressions.impressions);
    assertSize(1, addLatestImpressions.actionedImpressions);

    harness.processBothWatermarks(
        new Watermark(TimeUnit.HOURS.toMillis(Constants.SESSION_DURATION_HOURS + 1)));
    assertEquals(0, harness.numEventTimeTimers());
    assertSize(0, addLatestImpressions.actions);
    assertSize(0, addLatestImpressions.impressions);
    assertSize(0, addLatestImpressions.actionedImpressions);
  }

  @Test
  public void oneImpressionAndOneAction() throws Exception {
    assertOneImpressionAndOneAction(Duration.ofSeconds(0));
  }

  @Test
  public void oneImpressionAndOneAction_withDelay() throws Exception {
    assertOneImpressionAndOneAction(Duration.ofSeconds(1));
  }

  private void assertMultipleImpressionsAndOneAction(Duration maxOutOfOrderness) throws Exception {
    AddLatestImpressions addLatestImpressions = new AddLatestImpressions(maxOutOfOrderness, true);
    KeyedTwoInputStreamOperatorTestHarness harness = createHarness(addLatestImpressions);

    processElement2(harness, createFlatImpression("1", "itemA", 500));
    processElement2(harness, createFlatImpression("2", "itemB", 500));
    processElement2(harness, createFlatImpression("3", "itemC", 500));
    processElement2(harness, createFlatImpression("4", "itemD", 1000));
    processElement2(harness, createFlatImpression("5", "itemE", 1000));
    processElement2(harness, createFlatImpression("6", "itemF", 1000));
    processElement1(harness, createFlatAction("3", "itemC", 10000));

    harness.processBothWatermarks(
        new Watermark(9999 + KAFKA_TIMESTAMP_DELAY.toMillis() + maxOutOfOrderness.toMillis()));
    assertEquals(ImmutableList.of(), harness.extractOutputValues());
    harness.processBothWatermarks(
        new Watermark(10000 + KAFKA_TIMESTAMP_DELAY.toMillis() + maxOutOfOrderness.toMillis()));
    assertEquals(
        ImmutableList.of(
            addFlatImpressions(
                createFlatAction("3", "itemC", 10000),
                ImmutableList.of(
                    LatestImpression.newBuilder()
                        .setImpressionId("1")
                        .setContentId("itemA")
                        .setClientLogTimestamp(500)
                        .setEventApiTimestamp(600)
                        .setEventTimestamp(610)
                        .build(),
                    LatestImpression.newBuilder()
                        .setImpressionId("2")
                        .setContentId("itemB")
                        .setClientLogTimestamp(500)
                        .setEventApiTimestamp(600)
                        .setEventTimestamp(610)
                        .build(),
                    LatestImpression.newBuilder()
                        .setImpressionId("4")
                        .setContentId("itemD")
                        .setClientLogTimestamp(1000)
                        .setEventApiTimestamp(1100)
                        .setEventTimestamp(1110)
                        .build(),
                    LatestImpression.newBuilder()
                        .setImpressionId("5")
                        .setContentId("itemE")
                        .setClientLogTimestamp(1000)
                        .setEventApiTimestamp(1100)
                        .setEventTimestamp(1110)
                        .build(),
                    LatestImpression.newBuilder()
                        .setImpressionId("6")
                        .setContentId("itemF")
                        .setClientLogTimestamp(1000)
                        .setEventApiTimestamp(1100)
                        .setEventTimestamp(1110)
                        .build()))),
        harness.extractOutputValues());

    // Assert state after cleanup timers.
    assertEquals(2, harness.numEventTimeTimers());
    assertSize(0, addLatestImpressions.actions);
    assertSize(6, addLatestImpressions.impressions);
    assertSize(1, addLatestImpressions.actionedImpressions);

    harness.processBothWatermarks(new Watermark(120000));
    assertEquals(1, harness.numEventTimeTimers());
    assertSize(0, addLatestImpressions.actions);
    assertSize(6, addLatestImpressions.impressions);
    assertSize(1, addLatestImpressions.actionedImpressions);

    harness.processBothWatermarks(
        new Watermark(TimeUnit.HOURS.toMillis(Constants.SESSION_DURATION_HOURS + 1)));
    assertEquals(0, harness.numEventTimeTimers());
    assertSize(0, addLatestImpressions.actions);
    assertSize(0, addLatestImpressions.impressions);
    assertSize(0, addLatestImpressions.actionedImpressions);
  }

  @Test
  public void multipleImpressionsAndOneAction() throws Exception {
    assertMultipleImpressionsAndOneAction(Duration.ofSeconds(0));
  }

  @Test
  public void multipleImpressionsAndOneAction_withDelay() throws Exception {
    assertMultipleImpressionsAndOneAction(Duration.ofSeconds(1));
  }

  @Test
  public void multipleImpressionsAndActions() throws Exception {
    // Having maxStateImpressions be higher is important for this test.  Otherwise we drop itemF in
    // the last group.
    AddLatestImpressions addLatestImpressions =
        new AddLatestImpressions(Duration.ofSeconds(1), 3, 5, true);
    KeyedTwoInputStreamOperatorTestHarness harness = createHarness(addLatestImpressions);

    processElement2(harness, createFlatImpression("1", "itemA", 500));
    processElement2(harness, createFlatImpression("2", "itemB", 500));
    processElement2(harness, createFlatImpression("3", "itemC", 500));
    processElement1(harness, createFlatAction("3", "itemC", 10000));
    processElement2(harness, createFlatImpression("4", "itemD", 11000));
    processElement2(harness, createFlatImpression("5", "itemE", 11000));
    processElement2(harness, createFlatImpression("6", "itemF", 11000));
    processElement2(harness, createFlatImpression("7", "itemA", 15000));
    processElement2(harness, createFlatImpression("8", "itemB", 15000));
    processElement2(harness, createFlatImpression("9", "itemC", 15000));
    processElement1(harness, createFlatAction("7", "itemB", 20000));
    processElement2(harness, createFlatImpression("10", "itemG", 25000));
    processElement2(harness, createFlatImpression("11", "itemH", 25050));
    processElement1(harness, createFlatAction("11", "itemH", 30000));

    harness.processBothWatermarks(new Watermark(50000));
    assertEquals(
        ImmutableList.of(
            addFlatImpressions(
                createFlatAction("3", "itemC", 10000),
                ImmutableList.of(
                    // We don't want to include itemC because it was actioned on and this list is to
                    // give
                    // a negative signal to Personalize.
                    LatestImpression.newBuilder()
                        .setImpressionId("1")
                        .setContentId("itemA")
                        .setClientLogTimestamp(500)
                        .setEventApiTimestamp(600)
                        .setEventTimestamp(610)
                        .build(),
                    LatestImpression.newBuilder()
                        .setImpressionId("2")
                        .setContentId("itemB")
                        .setClientLogTimestamp(500)
                        .setEventApiTimestamp(600)
                        .setEventTimestamp(610)
                        .build())),
            addFlatImpressions(
                createFlatAction("7", "itemB", 20000),
                ImmutableList.of(
                    // We don't want to include items that have already been actioned on.
                    LatestImpression.newBuilder()
                        .setImpressionId("5")
                        .setContentId("itemE")
                        .setClientLogTimestamp(11000)
                        .setEventApiTimestamp(11100)
                        .setEventTimestamp(11110)
                        .build(),
                    LatestImpression.newBuilder()
                        .setImpressionId("6")
                        .setContentId("itemF")
                        .setClientLogTimestamp(11000)
                        .setEventApiTimestamp(11100)
                        .setEventTimestamp(11110)
                        .build(),
                    LatestImpression.newBuilder()
                        .setImpressionId("7")
                        .setContentId("itemA")
                        .setClientLogTimestamp(15000)
                        .setEventApiTimestamp(15100)
                        .setEventTimestamp(15110)
                        .build())),
            addFlatImpressions(
                createFlatAction("11", "itemH", 30000),
                ImmutableList.of(
                    // We don't want to include items that have already been actioned on.
                    LatestImpression.newBuilder()
                        .setImpressionId("6")
                        .setContentId("itemF")
                        .setClientLogTimestamp(11000)
                        .setEventApiTimestamp(11100)
                        .setEventTimestamp(11110)
                        .build(),
                    LatestImpression.newBuilder()
                        .setImpressionId("7")
                        .setContentId("itemA")
                        .setClientLogTimestamp(15000)
                        .setEventApiTimestamp(15100)
                        .setEventTimestamp(15110)
                        .build(),
                    LatestImpression.newBuilder()
                        .setImpressionId("10")
                        .setContentId("itemG")
                        .setClientLogTimestamp(25000)
                        .setEventApiTimestamp(25100)
                        .setEventTimestamp(25110)
                        .build()))),
        harness.extractOutputValues());

    harness.processBothWatermarks(
        new Watermark(TimeUnit.HOURS.toMillis(Constants.SESSION_DURATION_HOURS + 1)));
    assertEquals(0, harness.numEventTimeTimers());
    assertSize(0, addLatestImpressions.actions);
    assertSize(0, addLatestImpressions.impressions);
    assertSize(0, addLatestImpressions.actionedImpressions);
  }

  @Test
  public void multipleImpressionsAndActionsOnSameContent() throws Exception {
    // Having maxStateImpressions be higher is important for this test.  Otherwise we drop itemF in
    // the last group.
    AddLatestImpressions addLatestImpressions =
        new AddLatestImpressions(Duration.ofSeconds(1), 3, 5, true);
    KeyedTwoInputStreamOperatorTestHarness harness = createHarness(addLatestImpressions);

    processElement2(harness, createFlatImpression("1", "itemA", 500));
    processElement2(harness, createFlatImpression("2", "itemB", 500));
    processElement2(harness, createFlatImpression("3", "itemC", 500));
    processElement1(harness, createFlatAction("3", "itemC", 10000));
    processElement2(harness, createFlatImpression("4", "itemD", 11000));
    processElement2(harness, createFlatImpression("5", "itemE", 11000));
    processElement2(harness, createFlatImpression("6", "itemF", 11000));
    processElement2(harness, createFlatImpression("7", "itemA", 15000));
    processElement2(harness, createFlatImpression("8", "itemB", 15000));
    processElement2(harness, createFlatImpression("9", "itemC", 15000));
    processElement1(harness, createFlatAction("9", "itemC", 20000));
    processElement2(harness, createFlatImpression("10", "itemG", 25000));
    processElement2(harness, createFlatImpression("11", "itemH", 25050));
    processElement2(harness, createFlatImpression("12", "itemC", 25050));
    processElement1(harness, createFlatAction("12", "itemC", 30000));

    harness.processBothWatermarks(new Watermark(50000));
    assertEquals(
        ImmutableList.of(
            addFlatImpressions(
                createFlatAction("3", "itemC", 10000),
                ImmutableList.of(
                    // We don't want to include itemC because it was actioned on and this list is to
                    // give
                    // a negative signal to Personalize.
                    LatestImpression.newBuilder()
                        .setImpressionId("1")
                        .setContentId("itemA")
                        .setClientLogTimestamp(500)
                        .setEventApiTimestamp(600)
                        .setEventTimestamp(610)
                        .build(),
                    LatestImpression.newBuilder()
                        .setImpressionId("2")
                        .setContentId("itemB")
                        .setClientLogTimestamp(500)
                        .setEventApiTimestamp(600)
                        .setEventTimestamp(610)
                        .build())),
            addFlatImpressions(
                createFlatAction("9", "itemC", 20000),
                ImmutableList.of(
                    // We don't want to include items that have already been actioned on.
                    LatestImpression.newBuilder()
                        .setImpressionId("6")
                        .setContentId("itemF")
                        .setClientLogTimestamp(11000)
                        .setEventApiTimestamp(11100)
                        .setEventTimestamp(11110)
                        .build(),
                    LatestImpression.newBuilder()
                        .setImpressionId("7")
                        .setContentId("itemA")
                        .setClientLogTimestamp(15000)
                        .setEventApiTimestamp(15100)
                        .setEventTimestamp(15110)
                        .build(),
                    LatestImpression.newBuilder()
                        .setImpressionId("8")
                        .setContentId("itemB")
                        .setClientLogTimestamp(15000)
                        .setEventApiTimestamp(15100)
                        .setEventTimestamp(15110)
                        .build())),
            addFlatImpressions(
                createFlatAction("12", "itemC", 30000),
                ImmutableList.of(
                    // We don't want to include items that have already been actioned on.
                    LatestImpression.newBuilder()
                        .setImpressionId("8")
                        .setContentId("itemB")
                        .setClientLogTimestamp(15000)
                        .setEventApiTimestamp(15100)
                        .setEventTimestamp(15110)
                        .build(),
                    LatestImpression.newBuilder()
                        .setImpressionId("10")
                        .setContentId("itemG")
                        .setClientLogTimestamp(25000)
                        .setEventApiTimestamp(25100)
                        .setEventTimestamp(25110)
                        .build(),
                    LatestImpression.newBuilder()
                        .setImpressionId("11")
                        .setContentId("itemH")
                        .setClientLogTimestamp(25050)
                        .setEventApiTimestamp(25150)
                        .setEventTimestamp(25160)
                        .build()))),
        harness.extractOutputValues());

    harness.processBothWatermarks(
        new Watermark(TimeUnit.HOURS.toMillis(Constants.SESSION_DURATION_HOURS + 1)));
    assertEquals(0, harness.numEventTimeTimers());
    assertSize(0, addLatestImpressions.actions);
    assertSize(0, addLatestImpressions.impressions);
    assertSize(0, addLatestImpressions.actionedImpressions);
  }

  @Test
  public void getActionTimestamp() throws Exception {
    assertEquals(123, AddLatestImpressions.getActionTimestamp(createFlatAction("", "", 123)));
    assertEquals(
        456,
        AddLatestImpressions.getActionTimestamp(
            JoinedEvent.newBuilder()
                .setAction(
                    Action.newBuilder().setTiming(Timing.newBuilder().setEventApiTimestamp(456)))
                .build()));
    assertEquals(
        123,
        AddLatestImpressions.getActionTimestamp(
            JoinedEvent.newBuilder()
                .setAction(
                    Action.newBuilder()
                        .setTiming(
                            Timing.newBuilder()
                                .setClientLogTimestamp(123)
                                .setEventApiTimestamp(456)))
                .build()));
  }

  @Test
  public void getImpressionTimestamp() throws Exception {
    assertEquals(
        123, AddLatestImpressions.getImpressionTimestamp(createFlatImpression("", "", 123)));
    assertEquals(
        456,
        AddLatestImpressions.getImpressionTimestamp(
            JoinedEvent.newBuilder()
                .setImpression(
                    Impression.newBuilder()
                        .setTiming(Timing.newBuilder().setEventApiTimestamp(456)))
                .build()));
    assertEquals(
        123,
        AddLatestImpressions.getImpressionTimestamp(
            JoinedEvent.newBuilder()
                .setImpression(
                    Impression.newBuilder()
                        .setTiming(
                            Timing.newBuilder()
                                .setClientLogTimestamp(123)
                                .setEventApiTimestamp(456)))
                .build()));
  }
}
