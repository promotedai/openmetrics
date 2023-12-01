package ai.promoted.metrics.logprocessor.job.join.common;

import ai.promoted.metrics.logprocessor.common.functions.base.SerializableFunction;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializablePredicate;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableSupplier;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableToLongFunction;
import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import java.time.Duration;
import java.util.Comparator;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

// TODO - we can implement two out-of-order times: soft and hard.

/**
 * Takes a keyed stream and prioritizes the records. This is currently implemented to assume the
 * input stream will contain prioritized matches (joins).
 *
 * <p>The K is the primary key for the RHS.
 *
 * <p>Input events should be the event time for the RHS.
 *
 * <p>The job will attempt to output just the highest priority (greater comparator result). In the
 * case where there are late events, multiple records will be outputted.
 *
 * <p>A timer is created for event's time. The timer will output a record if there is not already an
 * output. That timer will then clear up all state. This operator assumes that the caller will wrap
 * with a `KeyedProcessOperatorWithWatermarkDelay`.
 */
public class FilterToHighestPriority<
        K extends Serializable, T extends Serializable, OUT extends Serializable>
    extends KeyedProcessFunction<K, T, OUT> {

  // Indicates whether records have been outputted.
  // Null: no result was output
  // False: a result was output but the next timer is the result-output timer
  // True: a result was output and the next timer is the clear timer
  @VisibleForTesting ValueState<Boolean> readyToClear;

  // Keep track
  // LHS is outputted from an interval join of LHS and RHS.
  // Event times will be the max(LHS, RHS).
  // We only need to keep the closest record for this K.
  @VisibleForTesting ValueState<T> bestJoinState;

  private final TypeInformation<T> eventType;
  private final SerializableFunction<T, OUT> formatOut;
  private final SerializablePredicate<T> isExactParentIdSpecified;
  private final SerializableSupplier<Comparator<T>> comparatorSupplier;
  // Simplifies the inner Comparator.
  private final SerializableToLongFunction<T> getEventTime;
  private final SerializableToLongFunction<T> getParentTime;
  private final SerializableFunction<T, String> getParentPrimaryId;
  private final long cleanUpDelayMillis;
  transient Comparator<T> betterMatch;
  private transient OutputTag<T> droppedEventsTag;

  public FilterToHighestPriority(
      TypeInformation<T> eventType,
      SerializableFunction<T, OUT> formatOut,
      // Instead of making a bunch of Comparators Serializable, just make the supplier serializable.
      SerializablePredicate<T> isExactParentIdSpecified,
      SerializableSupplier<Comparator<T>> comparatorSupplier,
      SerializableFunction<T, String> getParentPrimaryId,
      SerializableToLongFunction<T> getEventTime,
      SerializableToLongFunction<T> getParentTime,
      Duration cleanUpDelay) {
    this.eventType = eventType;
    this.formatOut = formatOut;
    this.isExactParentIdSpecified = isExactParentIdSpecified;
    this.comparatorSupplier = comparatorSupplier;
    this.getParentPrimaryId = getParentPrimaryId;
    this.getEventTime = getEventTime;
    this.getParentTime = getParentTime;
    Preconditions.checkArgument(!cleanUpDelay.isNegative(), "cleanUpDelay cannot be negative");
    this.cleanUpDelayMillis = cleanUpDelay.toMillis();
  }

  @Override
  public void open(Configuration config) {
    readyToClear =
        getRuntimeContext().getState(new ValueStateDescriptor<>("ready-to-clear", Types.BOOLEAN));
    bestJoinState =
        getRuntimeContext().getState(new ValueStateDescriptor<>("best-join", eventType));

    // Prioritize by:
    // 1. Priority.  Prefer this.
    // 2. Time difference.
    //    2a. Prefer parent events that happen before or equal to the events.
    //    2b. Prefer parent events that happen closer to the event.
    // 3. Parent primary key.  This is a fallback in case parent records are batched at the same
    //    time.  This happens in automated tests.
    betterMatch =
        // TODO - resolve how priorities work.
        comparatorSupplier
            .get()
            .thenComparing(
                (T event1, T event2) -> {
                  // eventTime should be the same for both.
                  long eventTime = getEventTime.applyAsLong(event1);
                  long diff1 = eventTime - getParentTime.applyAsLong(event1);
                  long diff2 = eventTime - getParentTime.applyAsLong(event2);

                  // TODO - if we flip from low to high, what happens?
                  if (diff1 >= 0 && diff2 >= 0) {
                    return Long.compare(diff2, diff1);
                  } else if (diff1 < 0 && diff2 < 0) {
                    return Long.compare(diff1, diff2);
                  } else {
                    return (diff1 < 0) ? -1 : 1;
                  }
                })
            .thenComparing(Comparator.comparing(getParentPrimaryId).reversed());
  }

  /** Returns OutputTag to get dropped RHS events in a side channel. */
  public final OutputTag<T> getDroppedEventsTag() {
    if (droppedEventsTag == null) {
      droppedEventsTag = new OutputTag<T>("dropped-events", eventType) {};
    }
    return droppedEventsTag;
  }

  // The event time should be the leaf event time.  It'll be available on each input.
  public void processElement(
      T event, KeyedProcessFunction<K, T, OUT>.Context ctx, Collector<OUT> out) throws Exception {
    if (null != readyToClear.value()) {
      return;
    }

    // In case there's a bug upstream, always create the timer again if there's no state.
    // This could result in multiple outputs.  That's better than leaking state.
    T previousBestJoin = bestJoinState.value();

    // The first boolean is the most specific.
    if (isExactParentIdSpecified.test(event)) {
      out.collect(formatOut.apply(event));
      if (null != previousBestJoin) {
        // Clear to save space.
        readyToClear.update(false);
        bestJoinState.clear();
      } else {
        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + cleanUpDelayMillis);
        readyToClear.update(true);
      }
    } else {
      if (previousBestJoin == null) {
        ctx.timerService().registerEventTimeTimer(ctx.timestamp());
        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + cleanUpDelayMillis);
        bestJoinState.update(event);
      } else if (betterMatch.compare(event, previousBestJoin) > 0) {
        bestJoinState.update(event);
      }
    }
  }

  public void onTimer(
      long timestamp, KeyedProcessFunction<K, T, OUT>.OnTimerContext ctx, Collector<OUT> out)
      throws Exception {
    Boolean ready = this.readyToClear.value();
    if (Boolean.TRUE.equals(ready)) {
      // Must be the clean-up timer.
      this.readyToClear.clear();
      return;
    }

    if (null == ready) {
      T bestJoin = bestJoinState.value();
      Preconditions.checkState(bestJoin != null, "in timer, bestJoin should have a value");

      // If the parent ID is empty, assume no parent.
      if (!getParentPrimaryId.apply(bestJoin).isEmpty()) {
        out.collect(formatOut.apply(bestJoin));
      } else {
        ctx.output(getDroppedEventsTag(), bestJoin);
      }
    }

    // Clean up the full output details.  Keep the output metadata.
    readyToClear.update(true);
    bestJoinState.clear();
  }
}
