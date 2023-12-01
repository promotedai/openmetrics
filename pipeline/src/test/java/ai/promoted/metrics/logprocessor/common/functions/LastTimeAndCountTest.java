package ai.promoted.metrics.logprocessor.common.functions;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import ai.promoted.metrics.logprocessor.common.counter.LastTimeAggResult;
import ai.promoted.metrics.logprocessor.common.util.FlatUtil;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.AttributedAction;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import java.time.Duration;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

public class LastTimeAndCountTest {

  private static final Duration TTL_DAYS_FLAG = Duration.ofDays(90);

  private static AttributedAction attributedAction(Action.Builder action) {
    return AttributedAction.newBuilder().setAction(action).build();
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
    LastTimeAndCount<String, AttributedAction> function =
        new LastTimeAndCount<>(
            false,
            Types.STRING,
            action -> action.getAction().getTiming().getEventApiTimestamp(),
            TTL_DAYS_FLAG,
            true,
            hash -> 0L);
    try (KeyedOneInputStreamOperatorTestHarness<String, AttributedAction, LastTimeAggResult<String>>
        harness =
            new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(function), FlatUtil::getActionString, Types.STRING)) {
      harness.setStateBackend(new EmbeddedRocksDBStateBackend());

      harness.open();

      harness.processElement(
          attributedAction(createAction(ActionType.UNKNOWN_ACTION_TYPE, 10)), 10);

      assertEquals(1, harness.numEventTimeTimers());

      harness.processWatermark(10);
      assertEquals(1, harness.numEventTimeTimers());

      harness.processWatermark(1000);
      assertEquals(1, harness.numEventTimeTimers());
      FluentIterable<StreamRecord<LastTimeAggResult<String>>> expected =
          FluentIterable.from(
              ImmutableList.of(
                  new StreamRecord<>(
                      new LastTimeAggResult<>(
                          "UNKNOWN_ACTION_TYPE", 10L, 1L, function.keepTtlSeconds),
                      10)));
      assertIterableEquals(expected, harness.extractOutputStreamRecords());

      // Simulate events out of order
      harness.processElement(
          attributedAction(createAction(ActionType.NAVIGATE, Duration.ofHours(11).toMillis())),
          Duration.ofHours(11).toMillis());
      harness.processElement(
          attributedAction(createAction("1d border", Duration.ofHours(24).toMillis())),
          Duration.ofHours(24).toMillis());

      harness.processWatermark(Duration.ofHours(24).toMillis());
      assertEquals(3, harness.numEventTimeTimers());
      expected =
          expected.append(
              new StreamRecord<>(
                  new LastTimeAggResult<>("NAVIGATE", 39600000L, 1L, function.keepTtlSeconds),
                  Duration.ofHours(11).toMillis()),
              new StreamRecord<>(
                  new LastTimeAggResult<>("1d border", 86400000L, 1L, function.keepTtlSeconds),
                  Duration.ofHours(24).toMillis()));
      assertIterableEquals(expected, harness.extractOutputStreamRecords());

      // Simulate out of order
      harness.processElement(
          attributedAction(createAction(ActionType.NAVIGATE, Duration.ofHours(10).toMillis())),
          Duration.ofHours(10).toMillis());

      harness.processWatermark(Duration.ofHours(37).toMillis());
      assertEquals(3, harness.numEventTimeTimers());
      expected =
          expected.append(
              new StreamRecord<>(
                  // The last timestamp should still be 11 hours.
                  new LastTimeAggResult<>("NAVIGATE", 39600000L, 2L, function.keepTtlSeconds),
                  Duration.ofHours(10).toMillis()));
      assertIterableEquals(expected, harness.extractOutputStreamRecords());

      harness.processWatermark(Duration.ofHours(61).toMillis());
      assertEquals(3, harness.numEventTimeTimers());
      assertIterableEquals(expected, harness.extractOutputStreamRecords());

      harness.processElement(
          attributedAction(createAction(ActionType.LIKE, Duration.ofHours(96).toMillis())),
          Duration.ofHours(96).toMillis());

      harness.processWatermark(Duration.ofDays(6).toMillis());
      assertEquals(4, harness.numEventTimeTimers());
      expected =
          expected.append(
              new StreamRecord<>(
                  new LastTimeAggResult<>("LIKE", 345600000L, 1L, function.keepTtlSeconds),
                  Duration.ofHours(96).toMillis()));
      assertIterableEquals(expected, harness.extractOutputStreamRecords());

      harness.processElement(
          attributedAction(createAction("7d border", Duration.ofHours(168).toMillis())),
          Duration.ofHours(168).toMillis());

      harness.processWatermark(Duration.ofDays(9).toMillis());
      assertEquals(5, harness.numEventTimeTimers());
      expected =
          expected.append(
              new StreamRecord<>(
                  new LastTimeAggResult<>("7d border", 604800000L, 1L, function.keepTtlSeconds),
                  Duration.ofHours(168).toMillis()));
      assertIterableEquals(expected, harness.extractOutputStreamRecords());

      harness.processWatermark(Duration.ofDays(30).toMillis());
      assertEquals(5, harness.numEventTimeTimers());
      assertIterableEquals(expected, harness.extractOutputStreamRecords());

      // Like every day from day 32 through 100.
      for (int i = 32; i < 100; i++) {
        harness.processWatermark(Duration.ofDays(i).toMillis());
        harness.processElement(
            attributedAction(createAction(ActionType.LIKE, Duration.ofDays(i).toMillis())),
            Duration.ofDays(i).toMillis());
      }

      harness.processWatermark(Duration.ofDays(200).toMillis());
      assertEquals(0, harness.numEventTimeTimers());
      expected =
          expected.append(
              new StreamRecord<>(
                  new LastTimeAggResult<>(
                      "LIKE", Duration.ofDays(32).toMillis(), 2L, function.keepTtlSeconds),
                  Duration.ofDays(32).toMillis()),
              new StreamRecord<>(
                  new LastTimeAggResult<>(
                      "LIKE", Duration.ofDays(33).toMillis(), 3L, function.keepTtlSeconds),
                  Duration.ofDays(33).toMillis()),
              new StreamRecord<>(
                  new LastTimeAggResult<>(
                      "LIKE", Duration.ofDays(34).toMillis(), 4L, function.keepTtlSeconds),
                  Duration.ofDays(34).toMillis()));

      // Because of the daily likes, there's a pattern here until the 90th day...
      for (int i = 0; i < 57; i++) {
        expected =
            expected.append(
                new StreamRecord<>(
                    new LastTimeAggResult<>(
                        "LIKE",
                        Duration.ofDays(35 + i).toMillis(),
                        5L + i,
                        function.keepTtlSeconds),
                    Duration.ofDays(35 + i).toMillis()));
      }

      expected =
          expected.append(
              new StreamRecord<>(
                  new LastTimeAggResult<>(
                      "NAVIGATE", 39600000L, 0L, LastTimeAndCount.EXPIRE_TTL_SECONDS),
                  Duration.ofDays(92).toMillis()),
              new StreamRecord<>(
                  new LastTimeAggResult<>(
                      "UNKNOWN_ACTION_TYPE", 10L, 0L, LastTimeAndCount.EXPIRE_TTL_SECONDS),
                  Duration.ofDays(92).toMillis()),
              //              new StreamRecord<>(
              //                  new LastTimeAggResult<>(
              //                      "LIKE", Duration.ofDays(90).toMillis(), 60L,
              // function.keepTtlSeconds),
              //                  Duration.ofDays(90).toMillis()),
              new StreamRecord<>(
                  new LastTimeAggResult<>(
                      "1d border", 86400000L, 0L, LastTimeAndCount.EXPIRE_TTL_SECONDS),
                  Duration.ofDays(93).toMillis()),
              //              new StreamRecord<>(
              //                  new LastTimeAggResult<>(
              //                      "LIKE", Duration.ofDays(91).toMillis(), 61L,
              // function.keepTtlSeconds),
              //                  Duration.ofDays(91).toMillis()),
              new StreamRecord<>(
                  new LastTimeAggResult<>(
                      "LIKE", Duration.ofDays(92).toMillis(), 62L, function.keepTtlSeconds),
                  Duration.ofDays(92).toMillis()),
              new StreamRecord<>(
                  new LastTimeAggResult<>(
                      "LIKE", Duration.ofDays(93).toMillis(), 63L, function.keepTtlSeconds),
                  Duration.ofDays(93).toMillis()),
              new StreamRecord<>(
                  new LastTimeAggResult<>(
                      "LIKE", Duration.ofDays(94).toMillis(), 63L, function.keepTtlSeconds),
                  Duration.ofDays(94).toMillis()),
              new StreamRecord<>(
                  new LastTimeAggResult<>(
                      "LIKE", Duration.ofDays(95).toMillis(), 64L, function.keepTtlSeconds),
                  Duration.ofDays(95).toMillis()),
              // The count stays the same because a new LIKE is logged on the day but this offsets
              // the
              // LIKE that is logged on day 4.
              new StreamRecord<>(
                  new LastTimeAggResult<>(
                      "LIKE", Duration.ofDays(95).toMillis(), 64L, function.keepTtlSeconds),
                  Duration.ofDays(96).toMillis()),
              new StreamRecord<>(
                  new LastTimeAggResult<>(
                      "LIKE", Duration.ofDays(96).toMillis(), 65L, function.keepTtlSeconds),
                  Duration.ofDays(96).toMillis()),
              new StreamRecord<>(
                  new LastTimeAggResult<>(
                      "7d border",
                      Duration.ofDays(7).toMillis(),
                      0L,
                      LastTimeAndCount.EXPIRE_TTL_SECONDS),
                  Duration.ofDays(99).toMillis()),
              new StreamRecord<>(
                  new LastTimeAggResult<>(
                      "LIKE", Duration.ofDays(97).toMillis(), 66L, function.keepTtlSeconds),
                  Duration.ofDays(97).toMillis()),
              new StreamRecord<>(
                  new LastTimeAggResult<>(
                      "LIKE", Duration.ofDays(98).toMillis(), 67L, function.keepTtlSeconds),
                  Duration.ofDays(98).toMillis()),
              new StreamRecord<>(
                  new LastTimeAggResult<>(
                      "LIKE", Duration.ofDays(99).toMillis(), 68L, function.keepTtlSeconds),
                  Duration.ofDays(99).toMillis()),
              new StreamRecord<>(
                  new LastTimeAggResult<>(
                      "LIKE", Duration.ofDays(99).toMillis(), 65L, function.keepTtlSeconds),
                  Duration.ofDays(124).toMillis()));

      // Now a new pattern from day 125 where we continue decreasing.
      for (int i = 0; i < 65; i++) {
        expected =
            expected.append(
                new StreamRecord<>(
                    new LastTimeAggResult<>(
                        "LIKE",
                        Duration.ofDays(99).toMillis(),
                        64L - i,
                        (64L - i) == 0
                            ? LastTimeAndCount.EXPIRE_TTL_SECONDS
                            : function.keepTtlSeconds),
                    Duration.ofDays(125 + i).toMillis()));
      }
      assertThat(harness.extractOutputStreamRecords()).containsExactlyElementsIn(expected);
    }
  }

  @Test
  public void pseudoRandomTimerOffset() {
    assertEquals(0L, LastTimeAndCount.pseudoRandomTimerOffset(0));
    // 2:02:52 AM GMT
    assertEquals(20656972L, LastTimeAndCount.pseudoRandomTimerOffset(1));
    // 10:18:28 AM GMT
    assertEquals(47038708L, LastTimeAndCount.pseudoRandomTimerOffset(2432));
  }
}
