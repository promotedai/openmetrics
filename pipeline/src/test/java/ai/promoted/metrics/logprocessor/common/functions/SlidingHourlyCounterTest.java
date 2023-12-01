package ai.promoted.metrics.logprocessor.common.functions;

import static java.time.temporal.ChronoUnit.HOURS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.promoted.metrics.logprocessor.common.counter.WindowAggResult;
import ai.promoted.metrics.logprocessor.common.util.FlatUtil;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.AttributedAction;
import ai.promoted.proto.event.CartContent;
import com.google.common.collect.FluentIterable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

public class SlidingHourlyCounterTest {
  private static SlidingHourlyCounter<String, AttributedAction> createSlidingHourlyCounter(
      Duration windowSlide) {
    return createSlidingHourlyCounter(windowSlide, false);
  }

  private static SlidingHourlyCounter<String, AttributedAction> createSlidingHourlyCounter(
      Duration windowSlide, boolean sideOutputDebugLogging) {
    return new SlidingHourlyCounter<>(
        Types.STRING,
        windowSlide,
        action -> action.getAction().getTiming().getEventApiTimestamp(),
        CounterUtil::getCount,
        sideOutputDebugLogging);
  }

  private static AttributedAction newAttributedAction(Action action) {
    return AttributedAction.newBuilder().setAction(action).build();
  }

  private static AttributedAction newAttributedAction(Action.Builder action) {
    return newAttributedAction(action.build());
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
    SlidingHourlyCounter<String, AttributedAction> function =
        createSlidingHourlyCounter(Duration.ofMinutes(15), true);
    KeyedOneInputStreamOperatorTestHarness<String, AttributedAction, WindowAggResult<String>>
        harness =
            new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(function), FlatUtil::getActionString, Types.STRING);

    harness.setStateBackend(new EmbeddedRocksDBStateBackend());
    harness.open();

    harness.processElement(
        newAttributedAction(createAction(ActionType.UNKNOWN_ACTION_TYPE, 10)), 10);

    harness.processWatermark(20);
    assertEquals(2, harness.numEventTimeTimers());
    FluentIterable<StreamRecord<WindowAggResult<String>>> expected = FluentIterable.of();
    assertIterableEquals(expected, harness.extractOutputStreamRecords());

    harness.processElement(
        newAttributedAction(createAction(ActionType.NAVIGATE, 21)),
        Duration.ofSeconds(11).toMillis());

    harness.processWatermark(Duration.ofMinutes(14).toMillis());
    assertEquals(4, harness.numEventTimeTimers());
    assertIterableEquals(expected, harness.extractOutputStreamRecords());

    harness.processElement(
        // Delayed a window.
        newAttributedAction(createAction(ActionType.NAVIGATE, function.windowSlide - 10)),
        function.windowSlide);
    harness.processElement(
        newAttributedAction(createAction("15m border", Duration.ofMinutes(15).toMillis())),
        Duration.ofMinutes(15).toMillis());

    harness.processWatermark(function.windowSlide + 20);
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
        newAttributedAction(createAction(ActionType.NAVIGATE, Duration.ofMinutes(33).toMillis())),
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
        newAttributedAction(createAction(ActionType.LIKE, Duration.ofMinutes(92).toMillis())),
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
    SlidingHourlyCounter<String, AttributedAction> function =
        createSlidingHourlyCounter(Duration.ofMinutes(15), false);
    KeyedOneInputStreamOperatorTestHarness<String, AttributedAction, WindowAggResult<String>>
        harness =
            new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(function), FlatUtil::getActionString, Types.STRING);
    harness.setStateBackend(new EmbeddedRocksDBStateBackend());
    harness.open();

    harness.processElement(
        newAttributedAction(
            createAction(ActionType.PURCHASE, 10)
                .setSingleCartContent(CartContent.newBuilder().setQuantity(5))),
        10);

    harness.processWatermark(function.windowSlide + 20);
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
    SlidingHourlyCounter<String, AttributedAction> function =
        createSlidingHourlyCounter(Duration.ofMinutes(15), false);
    // 304758930000L = GMT: Wednesday, August 29, 1979 7:15:30 AM
    // 304759800000L = GMT: Wednesday, August 29, 1979 7:30:00 AM
    assertEquals(304759800000L, function.toWindowKey(SlidingCounter.toDateTime(304758930000L)));
    // 1000212360000L = GMT: Tuesday, September 11, 2001 12:46:00 PM
    // 1000213200000L = GMT: Tuesday, September 11, 2001 1:00:00 PM
    assertEquals(1000213200000L, function.toWindowKey(SlidingCounter.toDateTime(1000212360000L)));
    // 1583018099999L = GMT: Saturday, February 29, 2020 11:14:59.999 PM
    // 1583018100000L = GMT: Saturday, February 29, 2020 11:15:00 PM
    assertEquals(1583018100000L, function.toWindowKey(SlidingCounter.toDateTime(1583018099999L)));
    // 1583020799999L = GMT: Saturday, February 29, 2020 11:59:59.999 PM
    // 1583020800000L = GMT: Sunday, March 1, 2020 12:00:00 AM
    assertEquals(1583020800000L, function.toWindowKey(SlidingCounter.toDateTime(1583020799999L)));
  }

  @Test
  public void testWindowDateTime() {
    SlidingHourlyCounter<String, AttributedAction> function =
        createSlidingHourlyCounter(Duration.ofMinutes(15), false);
    assertEquals(
        LocalDateTime.of(1979, 8, 29, 7, 15, 30).toInstant(ZoneOffset.UTC),
        function.windowDateTime(304758930000L));
    assertEquals(
        LocalDateTime.of(2001, 9, 11, 12, 46).toInstant(ZoneOffset.UTC),
        function.windowDateTime(1000212360000L));
  }

  @Test
  public void testWindowSlide() {
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
