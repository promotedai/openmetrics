package ai.promoted.metrics.logprocessor.common.functions;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.promoted.metrics.logprocessor.common.util.FlatUtil;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.CartContent;
import ai.promoted.proto.event.JoinedEvent;
import com.google.common.collect.FluentIterable;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;

public class SlidingDailyCounterTest {
    @Test
    public void testCounting() throws Exception {
        SlidingDailyCounter<String, JoinedEvent> function =
                new SlidingDailyCounter<>(Types.STRING, FlatUtil::getEventApiTimestamp, CounterUtil::getCount, true);
        KeyedOneInputStreamOperatorTestHarness<String, JoinedEvent, Tuple4<String, Integer, Long, Integer>> harness =
            new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(function),
                FlatUtil::getActionString,
                Types.STRING);

        harness.open();

        harness.processElement(joinedEvent(createAction(ActionType.UNKNOWN_ACTION_TYPE, 10)), 10);

        harness.processWatermark(function.emitWindow - 10);
        assertEquals(1, harness.numKeyedStateEntries());
        assertEquals(4, harness.numEventTimeTimers());

        harness.processElement(
                joinedEvent(createAction("window border", function.emitWindow)),
                function.emitWindow);

        harness.processWatermark(function.emitWindow + 20);
        assertEquals(2, harness.numKeyedStateEntries());
        assertEquals(7, harness.numEventTimeTimers());
        FluentIterable<StreamRecord<Tuple4<String, Integer, Long, Integer>>> expected = FluentIterable.of();
        expected = expected.append(
                new StreamRecord<>(Tuple4.of("UNKNOWN_ACTION_TYPE", 1, 1L, 0), Duration.ofHours(4).toMillis()),
                new StreamRecord<>(Tuple4.of("UNKNOWN_ACTION_TYPE", 30, 1L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(4).toMillis()),
                new StreamRecord<>(Tuple4.of("UNKNOWN_ACTION_TYPE", 7, 1L, 0), Duration.ofHours(4).toMillis()));
        assertIterableEquals(expected, harness.extractOutputStreamRecords());

        harness.processElement(
                joinedEvent(createAction(ActionType.NAVIGATE, Duration.ofHours(11).toMillis())),
                Duration.ofHours(11).toMillis());
        harness.processElement(
                joinedEvent(createAction("1d border", Duration.ofHours(24).toMillis())),
                Duration.ofHours(24).toMillis());

        harness.processWatermark(Duration.ofHours(24).toMillis());
        assertEquals(4, harness.numKeyedStateEntries());
        assertEquals(13, harness.numEventTimeTimers());
        expected = expected.append(
                new StreamRecord<>(Tuple4.of("window border", 1, 1L, 0), Duration.ofHours(8).toMillis()),
                new StreamRecord<>(Tuple4.of("window border", 30, 1L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(8).toMillis()),
                new StreamRecord<>(Tuple4.of("window border", 7, 1L, 0), Duration.ofHours(8).toMillis()),
                new StreamRecord<>(Tuple4.of("NAVIGATE", 1, 1L, 0), Duration.ofHours(12).toMillis()),
                new StreamRecord<>(Tuple4.of("NAVIGATE", 30, 1L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(12).toMillis()),
                new StreamRecord<>(Tuple4.of("NAVIGATE", 7, 1L, 0), Duration.ofHours(12).toMillis()));
        assertIterableEquals(expected, harness.extractOutputStreamRecords());

        harness.processWatermark(Duration.ofHours(29).toMillis());
        assertEquals(4, harness.numKeyedStateEntries());
        assertEquals(11, harness.numEventTimeTimers());
        expected = expected.append(
                new StreamRecord<>(Tuple4.of("UNKNOWN_ACTION_TYPE", 1, 0L, 0), Duration.ofHours(28).toMillis()),
                new StreamRecord<>(Tuple4.of("UNKNOWN_ACTION_TYPE", 30, 1L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(28).toMillis()),
                new StreamRecord<>(Tuple4.of("UNKNOWN_ACTION_TYPE", 7, 1L, 0), Duration.ofHours(28).toMillis()),
                new StreamRecord<>(Tuple4.of("1d border", 1, 1L, 0), Duration.ofHours(28).toMillis()),
                new StreamRecord<>(Tuple4.of("1d border", 30, 1L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(28).toMillis()),
                new StreamRecord<>(Tuple4.of("1d border", 7, 1L, 0), Duration.ofHours(28).toMillis()));
        assertIterableEquals(expected, harness.extractOutputStreamRecords());

        harness.processElement(
                joinedEvent(createAction(ActionType.NAVIGATE, Duration.ofHours(33).toMillis())),
                Duration.ofHours(33).toMillis());

        harness.processWatermark(Duration.ofHours(37).toMillis());
        assertEquals(4, harness.numKeyedStateEntries());
        assertEquals(12, harness.numEventTimeTimers());
        expected = expected.append(
                new StreamRecord<>(Tuple4.of("window border", 1, 0L, 0), Duration.ofHours(32).toMillis()),
                new StreamRecord<>(Tuple4.of("window border", 30, 1L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(32).toMillis()),
                new StreamRecord<>(Tuple4.of("window border", 7, 1L, 0), Duration.ofHours(32).toMillis()),
                new StreamRecord<>(Tuple4.of("NAVIGATE", 1, 1L, 0), Duration.ofHours(36).toMillis()),
                new StreamRecord<>(Tuple4.of("NAVIGATE", 30, 2L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(36).toMillis()),
                new StreamRecord<>(Tuple4.of("NAVIGATE", 7, 2L, 0), Duration.ofHours(36).toMillis()));
        assertIterableEquals(expected, harness.extractOutputStreamRecords());

        harness.processWatermark(Duration.ofHours(48).toMillis());
        assertEquals(4, harness.numKeyedStateEntries());
        assertEquals(12, harness.numEventTimeTimers());
        assertIterableEquals(expected, harness.extractOutputStreamRecords());

        harness.processWatermark(Duration.ofHours(52).toMillis());
        assertEquals(4, harness.numKeyedStateEntries());
        assertEquals(11, harness.numEventTimeTimers());
        expected = expected.append(
                new StreamRecord<>(Tuple4.of("1d border", 1, 0L, 0), Duration.ofHours(52).toMillis()),
                new StreamRecord<>(Tuple4.of("1d border", 30, 1L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(52).toMillis()),
                new StreamRecord<>(Tuple4.of("1d border", 7, 1L, 0), Duration.ofHours(52).toMillis()));
        assertIterableEquals(expected, harness.extractOutputStreamRecords());

        harness.processWatermark(Duration.ofHours(61).toMillis());
        assertEquals(4, harness.numKeyedStateEntries());
        assertEquals(10, harness.numEventTimeTimers());
        expected = expected.append(
                new StreamRecord<>(Tuple4.of("NAVIGATE", 1, 0L, 0), Duration.ofHours(60).toMillis()),
                new StreamRecord<>(Tuple4.of("NAVIGATE", 30, 2L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(60).toMillis()),
                new StreamRecord<>(Tuple4.of("NAVIGATE", 7, 2L, 0), Duration.ofHours(60).toMillis()));
        assertIterableEquals(expected, harness.extractOutputStreamRecords());

        // Like at 4d.
        harness.processElement(
                joinedEvent(createAction(ActionType.LIKE, Duration.ofHours(96).toMillis())),
                Duration.ofHours(96).toMillis());

        harness.processWatermark(Duration.ofDays(6).toMillis());
        assertEquals(5, harness.numKeyedStateEntries());
        assertEquals(12, harness.numEventTimeTimers());
        expected = expected.append(
                new StreamRecord<>(Tuple4.of("LIKE", 1, 1L, 0), Duration.ofHours(100).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 30, 1L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(100).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 7, 1L, 0), Duration.ofHours(100).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 1, 0L, 0), Duration.ofHours(124).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 30, 1L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(124).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 7, 1L, 0), Duration.ofHours(124).toMillis()));
        assertIterableEquals(expected, harness.extractOutputStreamRecords());

        harness.processElement(
                joinedEvent(createAction("7d border", Duration.ofHours(168).toMillis())),
                Duration.ofHours(168).toMillis());

        harness.processWatermark(Duration.ofDays(9).toMillis());
        assertEquals(6, harness.numKeyedStateEntries());
        assertEquals(9, harness.numEventTimeTimers());
        expected = expected.append(
                new StreamRecord<>(Tuple4.of("UNKNOWN_ACTION_TYPE", 1, 0L, 0), Duration.ofHours(172).toMillis()),
                new StreamRecord<>(Tuple4.of("UNKNOWN_ACTION_TYPE", 30, 1L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(172).toMillis()),
                new StreamRecord<>(Tuple4.of("UNKNOWN_ACTION_TYPE", 7, 0L, 0), Duration.ofHours(172).toMillis()),
                new StreamRecord<>(Tuple4.of("7d border", 1, 1L, 0), Duration.ofHours(172).toMillis()),
                new StreamRecord<>(Tuple4.of("7d border", 30, 1L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(172).toMillis()),
                new StreamRecord<>(Tuple4.of("7d border", 7, 1L, 0), Duration.ofHours(172).toMillis()),
                new StreamRecord<>(Tuple4.of("window border", 1, 0L, 0), Duration.ofHours(176).toMillis()),
                new StreamRecord<>(Tuple4.of("window border", 30, 1L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(176).toMillis()),
                new StreamRecord<>(Tuple4.of("window border", 7, 0L, 0), Duration.ofHours(176).toMillis()),
                new StreamRecord<>(Tuple4.of("NAVIGATE", 1, 0L, 0), Duration.ofHours(180).toMillis()),
                new StreamRecord<>(Tuple4.of("NAVIGATE", 30, 2L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(180).toMillis()),
                new StreamRecord<>(Tuple4.of("NAVIGATE", 7, 1L, 0), Duration.ofHours(180).toMillis()),
                new StreamRecord<>(Tuple4.of("1d border", 1, 0L, 0), Duration.ofHours(196).toMillis()),
                new StreamRecord<>(Tuple4.of("1d border", 30, 1L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(196).toMillis()),
                new StreamRecord<>(Tuple4.of("1d border", 7, 0L, 0), Duration.ofHours(196).toMillis()),
                new StreamRecord<>(Tuple4.of("7d border", 1, 0L, 0), Duration.ofHours(196).toMillis()),
                new StreamRecord<>(Tuple4.of("7d border", 30, 1L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(196).toMillis()),
                new StreamRecord<>(Tuple4.of("7d border", 7, 1L, 0), Duration.ofHours(196).toMillis()),
                new StreamRecord<>(Tuple4.of("NAVIGATE", 1, 0L, 0), Duration.ofHours(204).toMillis()),
                new StreamRecord<>(Tuple4.of("NAVIGATE", 30, 2L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(204).toMillis()),
                new StreamRecord<>(Tuple4.of("NAVIGATE", 7, 0L, 0), Duration.ofHours(204).toMillis()));
        assertIterableEquals(expected, harness.extractOutputStreamRecords());

        harness.processWatermark(Duration.ofDays(30).toMillis());
        assertEquals(6, harness.numKeyedStateEntries());
        assertEquals(7, harness.numEventTimeTimers());
        expected = expected.append(
                new StreamRecord<>(Tuple4.of("LIKE", 1, 0L, 0), Duration.ofHours(268).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 30, 1L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(268).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 7, 0L, 0), Duration.ofHours(268).toMillis()),
                new StreamRecord<>(Tuple4.of("7d border", 1, 0L, 0), Duration.ofHours(340).toMillis()),
                new StreamRecord<>(Tuple4.of("7d border", 30, 1L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(340).toMillis()),
                new StreamRecord<>(Tuple4.of("7d border", 7, 0L, 0), Duration.ofHours(340).toMillis()));
        assertIterableEquals(expected, harness.extractOutputStreamRecords());

        // Like every day from day 32 through 61 (inclusive, so 30 days).
        for (int i = 32; i < 62; i++) {
            harness.processElement(
                    joinedEvent(createAction(ActionType.LIKE, Duration.ofDays(i).toMillis())),
                    Duration.ofDays(i).toMillis());
        }

        harness.processWatermark(Duration.ofDays(95).toMillis());
        assertEquals(0, harness.numKeyedStateEntries());
        assertEquals(0, harness.numEventTimeTimers());
        expected = expected.append(
                new StreamRecord<>(Tuple4.of("UNKNOWN_ACTION_TYPE", 1, 0L, 0), Duration.ofHours(724).toMillis()),
                new StreamRecord<>(Tuple4.of("UNKNOWN_ACTION_TYPE", 30, 0L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(724).toMillis()),
                new StreamRecord<>(Tuple4.of("UNKNOWN_ACTION_TYPE", 7, 0L, 0), Duration.ofHours(724).toMillis()),
                new StreamRecord<>(Tuple4.of("window border", 1, 0L, 0), Duration.ofHours(728).toMillis()),
                new StreamRecord<>(Tuple4.of("window border", 30, 0L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(728).toMillis()),
                new StreamRecord<>(Tuple4.of("window border", 7, 0L, 0), Duration.ofHours(728).toMillis()),
                new StreamRecord<>(Tuple4.of("NAVIGATE", 1, 0L, 0), Duration.ofHours(732).toMillis()),
                new StreamRecord<>(Tuple4.of("NAVIGATE", 30, 1L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(732).toMillis()),
                new StreamRecord<>(Tuple4.of("NAVIGATE", 7, 0L, 0), Duration.ofHours(732).toMillis()),
                new StreamRecord<>(Tuple4.of("1d border", 1, 0L, 0), Duration.ofHours(748).toMillis()),
                new StreamRecord<>(Tuple4.of("1d border", 30, 0L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(748).toMillis()),
                new StreamRecord<>(Tuple4.of("1d border", 7, 0L, 0), Duration.ofHours(748).toMillis()),
                new StreamRecord<>(Tuple4.of("NAVIGATE", 1, 0L, 0), Duration.ofHours(756).toMillis()),
                new StreamRecord<>(Tuple4.of("NAVIGATE", 30, 0L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(756).toMillis()),
                new StreamRecord<>(Tuple4.of("NAVIGATE", 7, 0L, 0), Duration.ofHours(756).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 1, 1L, 0), Duration.ofHours(772).toMillis()),
                // There's a like from day 4.
                new StreamRecord<>(Tuple4.of("LIKE", 30, 2L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(772).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 7, 1L, 0), Duration.ofHours(772).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 1, 1L, 0), Duration.ofHours(796).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 30, 3L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(796).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 7, 2L, 0), Duration.ofHours(796).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 1, 1L, 0), Duration.ofHours(820).toMillis()),
                // The day 4 like drops as day 34 comes in.
                new StreamRecord<>(Tuple4.of("LIKE", 30, 3L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(820).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 7, 3L, 0), Duration.ofHours(820).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 1, 1L, 0), Duration.ofHours(844).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 30, 4L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(844).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 7, 4L, 0), Duration.ofHours(844).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 1, 1L, 0), Duration.ofHours(868).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 30, 5L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(868).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 7, 5L, 0), Duration.ofHours(868).toMillis()),
                new StreamRecord<>(Tuple4.of("7d border", 1, 0L, 0), Duration.ofHours(892).toMillis()),
                new StreamRecord<>(Tuple4.of("7d border", 30, 0L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(892).toMillis()),
                new StreamRecord<>(Tuple4.of("7d border", 7, 0L, 0), Duration.ofHours(892).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 1, 1L, 0), Duration.ofHours(892).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 30, 6L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(892).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 7, 6L, 0), Duration.ofHours(892).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 1, 1L, 0), Duration.ofHours(916).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 30, 7L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(916).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 7, 7L, 0), Duration.ofHours(916).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 1, 1L, 0), Duration.ofHours(940).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 30, 8L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(940).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 7, 7L, 0), Duration.ofHours(940).toMillis()));

        // Because of the daily likes for a month, there's a pattern here until around the 60th day.
        for (int i = 0; i < 21; i++) {
            int watermark = 964 + (24 * i);
            expected = expected.append(
                    new StreamRecord<>(Tuple4.of("LIKE", 1, 1L, 0), Duration.ofHours(watermark).toMillis()),
                    new StreamRecord<>(Tuple4.of("LIKE", 30, 9L + i, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(watermark).toMillis()),
                    new StreamRecord<>(Tuple4.of("LIKE", 7, 7L, 0), Duration.ofHours(watermark).toMillis()));
        }

        expected = expected.append(
                // Max counts of daily likes on day 61.
                new StreamRecord<>(Tuple4.of("LIKE", 1, 1L, 0), Duration.ofHours(1468).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 30, 30L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(1468).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 7, 7L, 0), Duration.ofHours(1468).toMillis()),
                // Things start to decrease day 62.
                new StreamRecord<>(Tuple4.of("LIKE", 1, 0L, 0), Duration.ofHours(1492).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 30, 29L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(1492).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", 7, 6L, 0), Duration.ofHours(1492).toMillis()));

        // Now a new pattern from day 63 where we're continue decreasing.
        for (int i = 0; i < 29; i++) {
            int watermark = 1516 + (24 * i);
            long count7d = Math.max(0, 5 - i);
            expected = expected.append(
                    new StreamRecord<>(Tuple4.of("LIKE", 1, 0L, 0), Duration.ofHours(watermark).toMillis()),
                    new StreamRecord<>(Tuple4.of("LIKE", 30, 28L - i, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(watermark).toMillis()),
                    new StreamRecord<>(Tuple4.of("LIKE", 7, count7d, 0), Duration.ofHours(watermark).toMillis()));
        }
        assertIterableEquals(expected, harness.extractOutputStreamRecords());
    }

    @Test
    public void testShoppingCartCount() throws Exception {
        SlidingDailyCounter<String, JoinedEvent> function =
                new SlidingDailyCounter<>(Types.STRING, FlatUtil::getEventApiTimestamp, CounterUtil::getCount, false);
        KeyedOneInputStreamOperatorTestHarness<String, JoinedEvent, Tuple4<String, Integer, Long, Integer>> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(function),
                        FlatUtil::getActionString,
                        Types.STRING);

        harness.open();

        harness.processElement(joinedEvent(
                createAction(ActionType.PURCHASE, 10)
                    .setSingleCartContent(CartContent.newBuilder()
                            .setQuantity(5))),
                10);

        harness.processWatermark(function.emitWindow + 20);
        assertEquals(1, harness.numKeyedStateEntries());
        assertEquals(3, harness.numEventTimeTimers());
        FluentIterable<StreamRecord<Tuple4<String, Integer, Long, Integer>>> expected = FluentIterable.of();
        expected = expected.append(
                new StreamRecord<>(Tuple4.of("PURCHASE", 1, 5L, 0), Duration.ofHours(4).toMillis()),
                new StreamRecord<>(Tuple4.of("PURCHASE", 30, 5L, SlidingDailyCounter.EXPIRE_TTL_SECONDS), Duration.ofHours(4).toMillis()),
                new StreamRecord<>(Tuple4.of("PURCHASE", 7, 5L, 0), Duration.ofHours(4).toMillis()));
        assertIterableEquals(expected, harness.extractOutputStreamRecords());
    }

    @Test
    public void testToWindowKey() {
        SlidingDailyCounter<String, JoinedEvent> function =
                new SlidingDailyCounter<>(Types.STRING, FlatUtil::getEventApiTimestamp, CounterUtil::getCount, false);
        // 1979-08-29 07:00:00.000
        assertEquals(1979082908, function.toWindowKey(SlidingCounter.toDateTime(304758000000L)));
        // 2001-09-11 12:46:00.000
        assertEquals(2001091116, function.toWindowKey(SlidingCounter.toDateTime(1000212360000L)));
        // 2020-02-29 23:59:59.999
        assertEquals(2020030100, function.toWindowKey(SlidingCounter.toDateTime(1583020799999L)));
    }

    @Test
    public void testWindowDateTime() {
        SlidingDailyCounter<String, JoinedEvent> function =
                new SlidingDailyCounter<>(Types.STRING, FlatUtil::getEventApiTimestamp, CounterUtil::getCount, false);
        assertEquals(LocalDateTime.of(1979, 8, 29, 4, 0), function.windowDateTime(1979082904));
        assertEquals(LocalDateTime.of(2001, 9, 11, 12, 0), function.windowDateTime(2001091112));
        assertEquals(LocalDateTime.of(2020, 2, 29, 20, 0), function.windowDateTime(2020022920));
    }

    private static JoinedEvent joinedEvent(Action.Builder action) {
        return FlatUtil.setFlatAction(JoinedEvent.newBuilder(), action.build(), (tag, error) -> {}).build();
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
}
