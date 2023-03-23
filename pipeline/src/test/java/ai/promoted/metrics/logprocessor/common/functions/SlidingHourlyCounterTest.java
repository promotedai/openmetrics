package ai.promoted.metrics.logprocessor.common.functions;

import static java.time.temporal.ChronoUnit.HOURS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.promoted.metrics.logprocessor.common.counter.WindowAggResult;
import ai.promoted.metrics.logprocessor.common.util.FlatUtil;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.CartContent;
import ai.promoted.proto.event.JoinedEvent;
import com.google.common.collect.FluentIterable;
import java.time.Duration;
import java.time.LocalDateTime;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

public class SlidingHourlyCounterTest {
  private static SlidingHourlyCounter<Void, JoinedEvent> createSlidingHourlyCounter(
      Duration emitWindow) {
    return new SlidingHourlyCounter<>(
        Types.VOID, emitWindow, FlatUtil::getEventApiTimestamp, CounterUtil::getCount, false);
  }

  private static JoinedEvent joinedEvent(Action.Builder action) {
    return FlatUtil.setFlatAction(JoinedEvent.newBuilder(), action.build(), (tag, error) -> {})
        .build();
  }

  private static Action.Builder createAction(ActionType actionType, long eventTime) {
    Action.Builder builder = Action.newBuilder().setActionType(actionType);
    builder.getTimingBuilder().setEventApiTimestamp(eventTime);
    return builder;
  }

  private static Action.Builder createAction(String customAction, long eventTime) {
    Action.Builder builder = createAction(ActionType.CUSTOM_ACTION_TYPE, eventTime);
    return builder.setCustomActionType(customAction);
  }

  @Test
  public void testCounting() throws Exception {
    SlidingHourlyCounter<String, JoinedEvent> function =
        new SlidingHourlyCounter<>(
            Types.STRING,
            Duration.ofMinutes(15),
            FlatUtil::getEventApiTimestamp,
            CounterUtil::getCount,
            true);
    KeyedOneInputStreamOperatorTestHarness<String, JoinedEvent, WindowAggResult<String>> harness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(function), FlatUtil::getActionString, Types.STRING);

    harness.setStateBackend(new EmbeddedRocksDBStateBackend());
    harness.open();

    harness.processElement(joinedEvent(createAction(ActionType.UNKNOWN_ACTION_TYPE, 10)), 10);

    harness.processWatermark(20);
    assertEquals(2, harness.numEventTimeTimers());
    FluentIterable<StreamRecord<WindowAggResult<String>>> expected = FluentIterable.of();
    assertIterableEquals(expected, harness.extractOutputStreamRecords());

    harness.processElement(
        joinedEvent(createAction(ActionType.NAVIGATE, 21)), Duration.ofSeconds(11).toMillis());

    harness.processWatermark(Duration.ofMinutes(14).toMillis());
    assertEquals(4, harness.numEventTimeTimers());
    assertIterableEquals(expected, harness.extractOutputStreamRecords());

    harness.processElement(
        // Delayed a window.
        joinedEvent(createAction(ActionType.NAVIGATE, function.emitWindow - 10)),
        function.emitWindow);
    harness.processElement(
        joinedEvent(createAction("15m border", Duration.ofMinutes(15).toMillis())),
        Duration.ofMinutes(15).toMillis());

    harness.processWatermark(function.emitWindow + 20);
    assertEquals(4, harness.numEventTimeTimers());
    expected =
        expected.append(
            new StreamRecord<>(
                new WindowAggResult<>("NAVIGATE", 1, HOURS, 2L, 0),
                Duration.ofMinutes(15).toMillis()),
            new StreamRecord<>(
                new WindowAggResult<>("UNKNOWN_ACTION_TYPE", 1, HOURS, 1L, 0),
                Duration.ofMinutes(15).toMillis()));
    assertIterableEquals(expected, harness.extractOutputStreamRecords());

    harness.processWatermark(Duration.ofMinutes(16).toMillis());
    assertEquals(4, harness.numEventTimeTimers());
    assertIterableEquals(expected, harness.extractOutputStreamRecords());

    harness.processWatermark(Duration.ofMinutes(17).toMillis());
    assertEquals(4, harness.numEventTimeTimers());
    assertIterableEquals(expected, harness.extractOutputStreamRecords());

    harness.processWatermark(Duration.ofMinutes(31).toMillis());
    assertEquals(3, harness.numEventTimeTimers());
    expected =
        expected.append(
            new StreamRecord<>(
                new WindowAggResult<>("15m border", 1, HOURS, 1L, 0),
                Duration.ofMinutes(30).toMillis()));
    assertIterableEquals(expected, harness.extractOutputStreamRecords());

    harness.processElement(
        joinedEvent(createAction(ActionType.NAVIGATE, Duration.ofMinutes(33).toMillis())),
        Duration.ofMinutes(33).toMillis());

    harness.processWatermark(Duration.ofMinutes(37).toMillis());
    assertEquals(5, harness.numEventTimeTimers());
    assertIterableEquals(expected, harness.extractOutputStreamRecords());

    harness.processWatermark(Duration.ofMinutes(48).toMillis());
    assertEquals(4, harness.numEventTimeTimers());
    expected =
        expected.append(
            new StreamRecord<>(
                new WindowAggResult<>("NAVIGATE", 1, HOURS, 3L, 0),
                Duration.ofMinutes(45).toMillis()));
    assertIterableEquals(expected, harness.extractOutputStreamRecords());

    harness.processWatermark(Duration.ofMinutes(52).toMillis());
    assertEquals(4, harness.numEventTimeTimers());
    assertIterableEquals(expected, harness.extractOutputStreamRecords());

    harness.processWatermark(Duration.ofMinutes(61).toMillis());
    assertEquals(4, harness.numEventTimeTimers());
    assertIterableEquals(expected, harness.extractOutputStreamRecords());

    harness.processElement(
        joinedEvent(createAction(ActionType.LIKE, Duration.ofMinutes(92).toMillis())),
        Duration.ofMinutes(92).toMillis());

    harness.processWatermark(Duration.ofMinutes(102).toMillis());
    assertEquals(3, harness.numEventTimeTimers());
    expected =
        expected.append(
            new StreamRecord<>(
                new WindowAggResult<>("NAVIGATE", 1, HOURS, 1L, 0),
                Duration.ofMinutes(75).toMillis()),
            new StreamRecord<>(
                new WindowAggResult<>("UNKNOWN_ACTION_TYPE", 1, HOURS, 0L, 0),
                Duration.ofMinutes(75).toMillis()),
            new StreamRecord<>(
                new WindowAggResult<>("15m border", 1, HOURS, 0L, 0),
                Duration.ofMinutes(90).toMillis()));
    assertIterableEquals(expected, harness.extractOutputStreamRecords());

    harness.processWatermark(Duration.ofMinutes(115).toMillis());
    assertEquals(1, harness.numEventTimeTimers());
    expected =
        expected.append(
            new StreamRecord<>(
                new WindowAggResult<>("LIKE", 1, HOURS, 1L, 0), Duration.ofMinutes(105).toMillis()),
            new StreamRecord<>(
                new WindowAggResult<>("NAVIGATE", 1, HOURS, 0L, 0),
                Duration.ofMinutes(105).toMillis()));
    assertIterableEquals(expected, harness.extractOutputStreamRecords());

    harness.processWatermark(Duration.ofMinutes(132).toMillis());
    assertEquals(1, harness.numEventTimeTimers());
    assertIterableEquals(expected, harness.extractOutputStreamRecords());

    harness.processWatermark(Duration.ofMinutes(165).toMillis());
    assertEquals(0, harness.numEventTimeTimers());
    expected =
        expected.append(
            new StreamRecord<>(
                new WindowAggResult<>("LIKE", 1, HOURS, 0L, 0),
                Duration.ofMinutes(165).toMillis()));
    assertIterableEquals(expected, harness.extractOutputStreamRecords());
  }

  @SuppressWarnings("CheckReturnValue")
  @Test
  public void testShoppingCartCount() throws Exception {
    SlidingHourlyCounter<String, JoinedEvent> function =
        new SlidingHourlyCounter<>(
            Types.STRING,
            Duration.ofMinutes(15),
            FlatUtil::getEventApiTimestamp,
            CounterUtil::getCount,
            false);
    KeyedOneInputStreamOperatorTestHarness<String, JoinedEvent, WindowAggResult<String>> harness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(function), FlatUtil::getActionString, Types.STRING);
    harness.setStateBackend(new EmbeddedRocksDBStateBackend());
    harness.open();

    harness.processElement(
        joinedEvent(
            createAction(ActionType.PURCHASE, 10)
                .setSingleCartContent(CartContent.newBuilder().setQuantity(5))),
        10);

    harness.processWatermark(function.emitWindow + 20);
    assertEquals(1, harness.numEventTimeTimers());
    FluentIterable<StreamRecord<WindowAggResult<?>>> expected = FluentIterable.of();
    expected =
        expected.append(
            new StreamRecord<>(
                new WindowAggResult("PURCHASE", 1, HOURS, 5L, 0),
                Duration.ofMinutes(15).toMillis()));
    assertIterableEquals(expected, harness.extractOutputStreamRecords());
  }

  @Test
  public void testToWindowKey() {
    SlidingHourlyCounter<String, JoinedEvent> function =
        new SlidingHourlyCounter<>(
            Types.STRING,
            Duration.ofMinutes(15),
            FlatUtil::getEventApiTimestamp,
            CounterUtil::getCount,
            false);
    // 1979-08-29 07:15:30.000
    assertEquals(197908290730L, function.toWindowKey(SlidingCounter.toDateTime(304758930000L)));
    // 2001-09-11 12:46:00.000
    assertEquals(200109111300L, function.toWindowKey(SlidingCounter.toDateTime(1000212360000L)));
    // 2020-02-29 23:14:59.999
    assertEquals(202002292315L, function.toWindowKey(SlidingCounter.toDateTime(1583018099999L)));
    // 2020-02-29 23:59:59.999
    assertEquals(202003010000L, function.toWindowKey(SlidingCounter.toDateTime(1583020799999L)));
  }

  @Test
  public void testWindowDateTime() {
    SlidingHourlyCounter<String, JoinedEvent> function =
        new SlidingHourlyCounter<>(
            Types.STRING,
            Duration.ofMinutes(15),
            FlatUtil::getEventApiTimestamp,
            CounterUtil::getCount,
            false);
    assertEquals(LocalDateTime.of(1979, 8, 29, 7, 15), function.windowDateTime(197908290715L));
    assertEquals(LocalDateTime.of(2001, 9, 11, 12, 45), function.windowDateTime(200109111245L));
    assertEquals(LocalDateTime.of(2020, 2, 29, 23, 0), function.windowDateTime(202002292300L));
  }

  @Test
  public void testEmitWindow() {
    // Factors are fine.
    assertThrows(
        IllegalArgumentException.class, () -> createSlidingHourlyCounter(Duration.ofMinutes(0)));
    createSlidingHourlyCounter(Duration.ofMinutes(1));
    createSlidingHourlyCounter(Duration.ofMinutes(2));
    createSlidingHourlyCounter(Duration.ofMinutes(3));
    assertThrows(
        IllegalArgumentException.class, () -> createSlidingHourlyCounter(Duration.ofMinutes(7)));
    assertThrows(
        IllegalArgumentException.class, () -> createSlidingHourlyCounter(Duration.ofMinutes(8)));
    createSlidingHourlyCounter(Duration.ofMinutes(10));
    createSlidingHourlyCounter(Duration.ofMinutes(12));
    createSlidingHourlyCounter(Duration.ofMinutes(15));
    createSlidingHourlyCounter(Duration.ofMinutes(20));
    createSlidingHourlyCounter(Duration.ofMinutes(30));
    assertThrows(
        IllegalArgumentException.class, () -> createSlidingHourlyCounter(Duration.ofMinutes(31)));
    createSlidingHourlyCounter(Duration.ofMinutes(60));
    assertThrows(
        IllegalArgumentException.class, () -> createSlidingHourlyCounter(Duration.ofMinutes(120)));
  }
}
