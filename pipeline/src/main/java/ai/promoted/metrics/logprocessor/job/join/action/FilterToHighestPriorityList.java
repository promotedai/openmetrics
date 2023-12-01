package ai.promoted.metrics.logprocessor.job.join.action;

import ai.promoted.metrics.logprocessor.common.functions.base.SerializableFunction;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableSupplier;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableToLongFunction;
import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

// TODO - we can implement two out-of-order times: soft and hard.
// TODO - redo comments.

/**
 * Takes a keyed stream of joins. In onTimer, outputs the subset of records that are the highest
 * priority of the received records.
 *
 * <p>This is currently implemented to assume the input stream will contain prioritized matches
 * (joins).
 *
 * <p>The K is the primary key for the join. The system currently uses actionId as K.
 *
 * <p>Input events should be the event time for the RHS.
 *
 * <p>The job will attempt to output just the highest priority (higher compare value). In the case
 * where there are late events, multiple records will be outputted.
 *
 * <p>A timer is created for event's time. The timer will output a record if there is not already an
 * output. That timer will then clear up all state. This operator assumes that the caller will wrap
 * with a `KeyedProcessOperatorWithWatermarkDelay`.
 */
public class FilterToHighestPriorityList<K extends Serializable, T extends Serializable>
    extends KeyedProcessFunction<K, T, List<T>> {

  @VisibleForTesting ValueState<Boolean> alreadyExecutedOutputTimer;

  // Keyed based on parent Id to handle deduplicating multiple join outputs.
  // All candidates in the map are from the same priority grouping (see comparator).
  @VisibleForTesting MapState<String, T> parentIdToJoin;

  private final TypeInformation<T> eventType;
  private final SerializableSupplier<Comparator<T>> comparatorSupplier;
  // Simplifies the inner Comparator.
  private final SerializableToLongFunction<T> getEventTime;
  private final SerializableToLongFunction<T> getParentTime;
  private final SerializableFunction<T, String> getParentPrimaryId;
  private final long cleanUpDelayMillis;
  transient Comparator<T> betterMatch;
  private transient Comparator<T> parentTimeComparator;

  public FilterToHighestPriorityList(
      TypeInformation<T> eventType,
      SerializableSupplier<Comparator<T>> comparatorSupplier,
      SerializableFunction<T, String> getParentPrimaryId,
      SerializableToLongFunction<T> getEventTime,
      SerializableToLongFunction<T> getParentTime,
      Duration cleanUpDelay) {
    this.eventType = eventType;
    this.comparatorSupplier = comparatorSupplier;
    this.getParentPrimaryId = getParentPrimaryId;
    this.getEventTime = getEventTime;
    this.getParentTime = getParentTime;
    Preconditions.checkArgument(!cleanUpDelay.isNegative(), "cleanUpDelay cannot be negative");
    this.cleanUpDelayMillis = cleanUpDelay.toMillis();
  }

  @Override
  public void open(Configuration config) {
    alreadyExecutedOutputTimer =
        getRuntimeContext()
            .getState(new ValueStateDescriptor<>("already-executed-output-timer", Types.BOOLEAN));
    parentIdToJoin =
        getRuntimeContext()
            .getMapState(new MapStateDescriptor<>("parent-id-to-join", Types.STRING, eventType));

    // Prioritize by:
    // 1. Priority.  Prefer this.  This factors in the parent ID.
    // 2. Time bucket.  Prefer parent events before the event.
    betterMatch =
        // TODO - swap the priorities.
        comparatorSupplier
            .get()
            .thenComparing(
                (T event1, T event2) -> {
                  // eventTime should be the same for both.
                  long eventTime = getEventTime.applyAsLong(event1);
                  // OutOfOrder is lower priority than in order.  OutOfOrder=False.
                  return Boolean.compare(
                      getParentTime.applyAsLong(event1) <= eventTime,
                      getParentTime.applyAsLong(event2) <= eventTime);
                });
    parentTimeComparator = Comparator.comparingLong(getParentTime);
  }

  // The event time should be the leaf event time.  It'll be available on each input.
  public void processElement(
      T event, KeyedProcessFunction<K, T, List<T>>.Context ctx, Collector<List<T>> out)
      throws Exception {
    if (alreadyExecutedOutputTimer.value() == Boolean.TRUE) {
      // Ignore additional records if we've already outputted the result list.
      return;
    }

    // In case there's a bug upstream, always create the timer again if there's no state.
    // This could result in multiple outputs.  That's better than leaking state.
    boolean addToState = false;
    if (parentIdToJoin.isEmpty()) {
      ctx.timerService().registerEventTimeTimer(ctx.timestamp());
      ctx.timerService().registerEventTimeTimer(ctx.timestamp() + cleanUpDelayMillis);
      addToState = true;
    } else {
      // Just pick the first entry.  They should all be from the same priority group.
      T anyPreviousEvent = parentIdToJoin.iterator().next().getValue();
      int comp = betterMatch.compare(event, anyPreviousEvent);
      if (comp > 0) {
        // If this event is more important, clear out the existing map.
        parentIdToJoin.clear();
      }
      if (comp >= 0) {
        addToState = true;
      }
    }

    if (addToState) {
      parentIdToJoin.put(getParentPrimaryId.apply(event), event);
    }
  }

  public void onTimer(
      long timestamp,
      KeyedProcessFunction<K, T, List<T>>.OnTimerContext ctx,
      Collector<List<T>> out)
      throws Exception {
    if (alreadyExecutedOutputTimer.value() == Boolean.TRUE) {
      // The next timer is the clean-up timer.
      alreadyExecutedOutputTimer.clear();
      return;
    }

    alreadyExecutedOutputTimer.update(Boolean.TRUE);
    List<T> results =
        StreamSupport.stream(parentIdToJoin.entries().spliterator(), false)
            .map(Map.Entry::getValue)
            .sorted(parentTimeComparator)
            .collect(Collectors.toList());
    out.collect(results);

    // Clean up state.
    parentIdToJoin.clear();
  }
}
