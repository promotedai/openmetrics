package ai.promoted.metrics.logprocessor.common.functions;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.promoted.metrics.logprocessor.common.util.FlatUtil;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.JoinedEvent;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;

public class LastTimeAndCountTest {

    private static final Duration emitWindow = Duration.ofMinutes(1);

    @Test
    public void testCounting() throws Exception {
        LastTimeAndCount<String, JoinedEvent> function = new LastTimeAndCount<>(Types.STRING, FlatUtil::getEventApiTimestamp, emitWindow, true);
        KeyedOneInputStreamOperatorTestHarness<String, JoinedEvent, Tuple4<String, Long, Long, Integer>> harness =
            new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(function),
                FlatUtil::getActionString,
                Types.STRING);

        harness.open();

        harness.processElement(joinedEvent(createAction(ActionType.UNKNOWN_ACTION_TYPE, 10)), 10);

        harness.processWatermark(emitWindow.toMillis() - 10);
        assertEquals(2, harness.numKeyedStateEntries());
        assertEquals(2, harness.numEventTimeTimers());

        harness.processElement(
                joinedEvent(createAction("window border", emitWindow.toMillis())),
                emitWindow.toMillis());

        harness.processWatermark(emitWindow.toMillis() + 20);
        assertEquals(4, harness.numKeyedStateEntries());
        assertEquals(3, harness.numEventTimeTimers());
        FluentIterable<StreamRecord<Tuple4<String, Long, Long, Integer>>> expected = FluentIterable.of();
        expected = expected.append(
                new StreamRecord<>(Tuple4.of("UNKNOWN_ACTION_TYPE", 10L, 1L, LastTimeAndCount.KEEP_TTL_SECONDS), Duration.ofMinutes(1).toMillis()));
        assertIterableEquals(expected, harness.extractOutputStreamRecords());

        harness.processElement(
                joinedEvent(createAction(ActionType.NAVIGATE, Duration.ofHours(11).toMillis())),
                Duration.ofHours(11).toMillis());
        harness.processElement(
                joinedEvent(createAction("1d border", Duration.ofHours(24).toMillis())),
                Duration.ofHours(24).toMillis());

        harness.processWatermark(Duration.ofHours(24).toMillis());
        assertEquals(8, harness.numKeyedStateEntries());
        assertEquals(5, harness.numEventTimeTimers());
        expected = expected.append(
                new StreamRecord<>(Tuple4.of("window border", 60000L, 1L, LastTimeAndCount.KEEP_TTL_SECONDS), Duration.ofMinutes(2).toMillis()),
                new StreamRecord<>(Tuple4.of("NAVIGATE", 39600000L, 1L, LastTimeAndCount.KEEP_TTL_SECONDS), Duration.ofHours(11).plusMinutes(1).toMillis()));
        assertIterableEquals(expected, harness.extractOutputStreamRecords());

        harness.processWatermark(Duration.ofHours(29).toMillis());
        assertEquals(8, harness.numKeyedStateEntries());
        assertEquals(4, harness.numEventTimeTimers());
        expected = expected.append(
                new StreamRecord<>(Tuple4.of("1d border", 86400000L, 1L, LastTimeAndCount.KEEP_TTL_SECONDS), Duration.ofHours(24).plusMinutes(1).toMillis()));
        assertIterableEquals(expected, harness.extractOutputStreamRecords());

        harness.processElement(
                joinedEvent(createAction(ActionType.NAVIGATE, Duration.ofHours(33).toMillis())),
                Duration.ofHours(33).toMillis());

        harness.processWatermark(Duration.ofHours(37).toMillis());
        assertEquals(8, harness.numKeyedStateEntries());
        assertEquals(5, harness.numEventTimeTimers());
        expected = expected.append(
                new StreamRecord<>(Tuple4.of("NAVIGATE", 118800000L, 2L, LastTimeAndCount.KEEP_TTL_SECONDS), Duration.ofHours(33).plusMinutes(1).toMillis()));
        assertIterableEquals(expected, harness.extractOutputStreamRecords());

        harness.processWatermark(Duration.ofHours(61).toMillis());
        assertEquals(8, harness.numKeyedStateEntries());
        assertEquals(5, harness.numEventTimeTimers());
        assertIterableEquals(expected, harness.extractOutputStreamRecords());

        harness.processElement(
                joinedEvent(createAction(ActionType.LIKE, Duration.ofHours(96).toMillis())),
                Duration.ofHours(96).toMillis());

        harness.processWatermark(Duration.ofDays(6).toMillis());
        assertEquals(10, harness.numKeyedStateEntries());
        assertEquals(6, harness.numEventTimeTimers());
        expected = expected.append(
                new StreamRecord<>(Tuple4.of("LIKE", 345600000L, 1L, LastTimeAndCount.KEEP_TTL_SECONDS), Duration.ofHours(96).plusMinutes(1).toMillis()));
        assertIterableEquals(expected, harness.extractOutputStreamRecords());

        harness.processElement(
                joinedEvent(createAction("7d border", Duration.ofHours(168).toMillis())),
                Duration.ofHours(168).toMillis());

        harness.processWatermark(Duration.ofDays(9).toMillis());
        assertEquals(12, harness.numKeyedStateEntries());
        assertEquals(7, harness.numEventTimeTimers());
        expected = expected.append(
                new StreamRecord<>(Tuple4.of("7d border", 604800000L, 1L, LastTimeAndCount.KEEP_TTL_SECONDS), Duration.ofHours(168).plusMinutes(1).toMillis()));
        assertIterableEquals(expected, harness.extractOutputStreamRecords());

        harness.processWatermark(Duration.ofDays(30).toMillis());
        assertEquals(12, harness.numKeyedStateEntries());
        assertEquals(7, harness.numEventTimeTimers());
        assertIterableEquals(expected, harness.extractOutputStreamRecords());

        // Like every day from day 32 through 100.
        for (int i = 32; i < 100; i++) {
            harness.processWatermark(Duration.ofDays(i).toMillis());
            harness.processElement(
                    joinedEvent(createAction(ActionType.LIKE, Duration.ofDays(i).toMillis())),
                    Duration.ofDays(i).toMillis());
        }

        harness.processWatermark(Duration.ofDays(200).toMillis());
        assertEquals(0, harness.numKeyedStateEntries());
        assertEquals(0, harness.numEventTimeTimers());
        expected = expected.append(
                new StreamRecord<>(Tuple4.of("LIKE", Duration.ofDays(32).toMillis(), 2L, LastTimeAndCount.KEEP_TTL_SECONDS), Duration.ofDays(32).plusMinutes(1).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", Duration.ofDays(33).toMillis(), 3L, LastTimeAndCount.KEEP_TTL_SECONDS), Duration.ofDays(33).plusMinutes(1).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", Duration.ofDays(34).toMillis(), 4L, LastTimeAndCount.KEEP_TTL_SECONDS), Duration.ofDays(34).plusMinutes(1).toMillis()));

        // Because of the daily likes, there's a pattern here until the 90th day...
        for (int i = 0; i < 55; i++) {
            expected = expected.append(
                    new StreamRecord<>(
                            Tuple4.of("LIKE", Duration.ofDays(35 + i).toMillis(), 5L + i, LastTimeAndCount.KEEP_TTL_SECONDS),
                            Duration.ofDays(35 + i).plusMinutes(1).toMillis()));
        }

        expected = expected.append(
                new StreamRecord<>(Tuple4.of("UNKNOWN_ACTION_TYPE", 10L, 0L, LastTimeAndCount.EXPIRE_TTL_SECONDS), Duration.ofDays(90).plusMinutes(1).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", Duration.ofDays(90).toMillis(), 60L, LastTimeAndCount.KEEP_TTL_SECONDS), Duration.ofDays(90).plusMinutes(1).toMillis()),
                new StreamRecord<>(Tuple4.of("window border", 60000L, 0L, LastTimeAndCount.EXPIRE_TTL_SECONDS), Duration.ofDays(90).plusMinutes(1).toMillis()),
                new StreamRecord<>(Tuple4.of("NAVIGATE", 118800000L, 1L, LastTimeAndCount.KEEP_TTL_SECONDS), Duration.ofDays(90).plusMinutes(1).toMillis()),
                new StreamRecord<>(Tuple4.of("1d border", 86400000L, 0L, LastTimeAndCount.EXPIRE_TTL_SECONDS), Duration.ofDays(91).plusMinutes(1).toMillis()),
                new StreamRecord<>(Tuple4.of("NAVIGATE", 118800000L, 0L, LastTimeAndCount.EXPIRE_TTL_SECONDS), Duration.ofDays(91).plusMinutes(1).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", Duration.ofDays(91).toMillis(), 61L, LastTimeAndCount.KEEP_TTL_SECONDS), Duration.ofDays(91).plusMinutes(1).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", Duration.ofDays(92).toMillis(), 62L, LastTimeAndCount.KEEP_TTL_SECONDS), Duration.ofDays(92).plusMinutes(1).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", Duration.ofDays(93).toMillis(), 63L, LastTimeAndCount.KEEP_TTL_SECONDS), Duration.ofDays(93).plusMinutes(1).toMillis()),
                // There was an "extra" like on day 4 that's dropped now.
                new StreamRecord<>(Tuple4.of("LIKE", Duration.ofDays(94).toMillis(), 63L, LastTimeAndCount.KEEP_TTL_SECONDS), Duration.ofDays(94).plusMinutes(1).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", Duration.ofDays(95).toMillis(), 64L, LastTimeAndCount.KEEP_TTL_SECONDS), Duration.ofDays(95).plusMinutes(1).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", Duration.ofDays(96).toMillis(), 65L, LastTimeAndCount.KEEP_TTL_SECONDS), Duration.ofDays(96).plusMinutes(1).toMillis()),
                new StreamRecord<>(Tuple4.of("7d border", Duration.ofDays(7).toMillis(), 0L, LastTimeAndCount.EXPIRE_TTL_SECONDS), Duration.ofDays(97).plusMinutes(1).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", Duration.ofDays(97).toMillis(), 66L, LastTimeAndCount.KEEP_TTL_SECONDS), Duration.ofDays(97).plusMinutes(1).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", Duration.ofDays(98).toMillis(), 67L, LastTimeAndCount.KEEP_TTL_SECONDS), Duration.ofDays(98).plusMinutes(1).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", Duration.ofDays(99).toMillis(), 68L, LastTimeAndCount.KEEP_TTL_SECONDS), Duration.ofDays(99).plusMinutes(1).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", Duration.ofDays(99).toMillis(), 67L, LastTimeAndCount.KEEP_TTL_SECONDS), Duration.ofDays(122).plusMinutes(1).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", Duration.ofDays(99).toMillis(), 66L, LastTimeAndCount.KEEP_TTL_SECONDS), Duration.ofDays(123).plusMinutes(1).toMillis()),
                new StreamRecord<>(Tuple4.of("LIKE", Duration.ofDays(99).toMillis(), 65L, LastTimeAndCount.KEEP_TTL_SECONDS), Duration.ofDays(124).plusMinutes(1).toMillis()));

        // Now a new pattern from day 125 where we continue decreasing.
        for (int i = 0; i < 65; i++) {
            expected = expected.append(
                    new StreamRecord<>(
                            Tuple4.of("LIKE", Duration.ofDays(99).toMillis(), 64L - i, (64L - i) == 0 ? LastTimeAndCount.EXPIRE_TTL_SECONDS : LastTimeAndCount.KEEP_TTL_SECONDS),
                            Duration.ofDays(125 + i).plusMinutes(1).toMillis()));
        }
        assertIterableEquals(expected, harness.extractOutputStreamRecords());
    }

    // We don't need to retest the full version here.  Just test that records emit immediately.
    @Test
    public void testCounting_noEmitWindow() throws Exception {
        LastTimeAndCount<String, JoinedEvent> function = new LastTimeAndCount<>(Types.STRING, FlatUtil::getEventApiTimestamp, Duration.ZERO, true);
        KeyedOneInputStreamOperatorTestHarness<String, JoinedEvent, Tuple4<String, Long, Long, Integer>> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(function),
                        FlatUtil::getActionString,
                        Types.STRING);

        harness.open();
        harness.processElement(joinedEvent(createAction(ActionType.UNKNOWN_ACTION_TYPE, 10)), 10);

        FluentIterable<StreamRecord<Tuple4<String, Long, Long, Integer>>> expected = FluentIterable.of();
        expected = expected.append(
                new StreamRecord<>(Tuple4.of("UNKNOWN_ACTION_TYPE", 10L, 1L, LastTimeAndCount.KEEP_TTL_SECONDS), Duration.ofMillis(10).toMillis()));
        assertIterableEquals(expected, harness.extractOutputStreamRecords());

        assertEquals(2, harness.numKeyedStateEntries());
        assertEquals(1, harness.numEventTimeTimers());

        harness.processElement(
                joinedEvent(createAction(ActionType.NAVIGATE, Duration.ofHours(11).toMillis())),
                Duration.ofHours(12).toMillis());

        assertEquals(4, harness.numKeyedStateEntries());
        assertEquals(2, harness.numEventTimeTimers());
        expected = expected.append(new StreamRecord<>(Tuple4.of("NAVIGATE", 39600000L, 1L, LastTimeAndCount.KEEP_TTL_SECONDS), Duration.ofHours(12).toMillis()));
        assertIterableEquals(expected, harness.extractOutputStreamRecords());

        harness.processWatermark(Duration.ofDays(200).toMillis());
        assertEquals(0, harness.numKeyedStateEntries());
        assertEquals(0, harness.numEventTimeTimers());

        expected = expected.append(
                new StreamRecord<>(Tuple4.of("UNKNOWN_ACTION_TYPE", 10L, 0L, LastTimeAndCount.EXPIRE_TTL_SECONDS), Duration.ofDays(90).toMillis()),
                new StreamRecord<>(Tuple4.of("NAVIGATE", 39600000L, 0L, LastTimeAndCount.EXPIRE_TTL_SECONDS), Duration.ofDays(90).toMillis()));

        assertIterableEquals(expected, harness.extractOutputStreamRecords());
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
