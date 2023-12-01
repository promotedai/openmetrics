package ai.promoted.metrics.logprocessor.common.functions.redundantevent;

import static ai.promoted.metrics.logprocessor.common.util.PagingIdUtil.isPagingIdEmpty;

import ai.promoted.metrics.common.RedundantImpression;
import ai.promoted.metrics.logprocessor.common.util.TinyFlatUtil;
import ai.promoted.proto.event.TinyActionPath;
import ai.promoted.proto.event.TinyImpression;
import ai.promoted.proto.event.TinyJoinedImpression;
import ai.promoted.proto.event.TinyTouchpoint;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// TODO - need to review the use of times in this class.  The code heavily uses eventApiTimestamp
// but the inputs will delay the watermarks for the inferred join.  It's fine to use a slower
// watermark.  The output event times can be confusing since the timers use the event api
// timestamps.  Events that are both non-redundant and out-of-order might not behave correctly.
/**
 * Used to filter out redundant {@code TinyJoinedImpression}s.
 *
 * <p>This function takes {@code TinyActionPath} so the references to redundant impressions can be
 * updated too.
 *
 * <p>In order to reduce redundant Impressions for {@code TinyActionPaths}, the key for reduction is
 * split across:
 *
 * <ul>
 *   <li>The key {@code K} for the operator is expected to be common across all touchpoints in the
 *       ActionPath. This is expected to be {@code (platformId, anonUserId, insertionContentId)}.
 *   <li>
 *   <li>The {@code getKey} for MapState contains extra key dimensions to identify different
 *       impressions. This is {@code (coalesce(pagingId, viewId), position)}.
 * </ul>
 *
 * <p>The function outputs in {@code onTimer} to improve consistency in reprocessing. The function
 * tries to have only 1 timer. {@code onTimer} calculates the next timer.
 *
 * <p>IN = TinyEvent for both flat impressions and actions
 *
 * <p>OUT = TinyEvent for both flat impressions and actions
 *
 * @param <K> Part of the grouping dimension for reduction.
 */
public class ReduceRedundantTinyImpressions<K>
    extends KeyedCoProcessFunction<K, TinyJoinedImpression, TinyActionPath, TinyJoinedImpression> {

  /** OutputTag for redundant impressions so we can track the issues over time. */
  public static final OutputTag<TinyActionPath> OUTPUT_ACTION_PATH =
      new OutputTag<>("action-path") {};

  /** OutputTag for redundant impressions so we can track the issues over time. */
  public static final OutputTag<RedundantImpression> REDUNDANT_IMPRESSION =
      new OutputTag<>("redundant-impression") {};

  // For testing, there's a higher chance of encountering multiple impressions with the same
  // event_api_timestamp.  When this happens, fallback to the impression_id.
  private static final Comparator<TinyJoinedImpression>
      TINY_JOINED_IMPRESSION_TIMESTAMP_COMPARATOR =
          Comparator.<TinyJoinedImpression>comparingLong(
                  impression -> impression.getImpression().getCommon().getEventApiTimestamp())
              .thenComparing(
                  Comparator.comparing(impression -> impression.getImpression().getImpressionId()));

  private static final Logger LOGGER = LogManager.getLogger(ReduceRedundantTinyImpressions.class);

  // Takes the ttl input and removes 1ms.  Clean-up is done at the end so we want to have an
  // inclusive end.
  private final long inclusiveTtlMillis;

  // Key = (pagingId or viewId, position)
  // Value = (TinyJoinedImpression, cleanup timer).  Keeps track and updates the cleanup timer as
  // we encounter redundant events.  There's not currently a limit to how long this can be extended.
  @VisibleForTesting
  transient MapState<Tuple2<String, Long>, Tuple2<TinyJoinedImpression, Long>>
      keyToReducedImpression;

  // Output all events on a delay so we can order the reductions consistently.
  @VisibleForTesting transient ListState<TinyJoinedImpression> delayOutputImpressions;
  @VisibleForTesting transient ListState<TinyActionPath> delayOutputActionPaths;

  // We'll try to keep just one timer.  If we encounter an event that should have an earlier timer,
  // we won't
  @VisibleForTesting transient ValueState<Long> nextTimer;

  public ReduceRedundantTinyImpressions(Duration ttl) {
    Preconditions.checkArgument(
        ttl.toMillis() > 0, "ReduceRedundantTinyImpressions TTL needs to be a positive number");
    this.inclusiveTtlMillis = ttl.toMillis() - 1;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    keyToReducedImpression =
        getRuntimeContext()
            .getMapState(
                new MapStateDescriptor<>(
                    "key-to-reduced-impression",
                    Types.TUPLE(Types.STRING, Types.LONG),
                    Types.TUPLE(TypeInformation.of(TinyJoinedImpression.class), Types.LONG)));

    delayOutputImpressions =
        getRuntimeContext()
            .getListState(
                new ListStateDescriptor<>(
                    "delay-output-impressions", TypeInformation.of(TinyJoinedImpression.class)));

    delayOutputActionPaths =
        getRuntimeContext()
            .getListState(
                new ListStateDescriptor<>(
                    "delay-output-actions", TypeInformation.of(TinyActionPath.class)));

    nextTimer = getRuntimeContext().getState(new ValueStateDescriptor<>("next-timer", Types.LONG));
  }

  @Override
  public void processElement1(
      TinyJoinedImpression joinedImpression,
      Context context,
      Collector<TinyJoinedImpression> collector)
      throws Exception {
    // TODO - there's probably a more efficient processing for impressions that already have a key.
    delayOutputImpressions.add(joinedImpression);
    updateNextTimer(context, joinedImpression.getImpression().getCommon().getEventApiTimestamp());
  }

  @Override
  public void processElement2(
      TinyActionPath actionPath, Context context, Collector<TinyJoinedImpression> collector)
      throws Exception {
    delayOutputActionPaths.add(actionPath);
    updateNextTimer(context, getMaxTime(actionPath));
  }

  private void updateNextTimer(Context context, long eventTime) throws IOException {
    Long nextTimeValue = nextTimer.value();
    if (null == nextTimeValue) {
      context.timerService().registerEventTimeTimer(eventTime);
      nextTimer.update(eventTime);
    } else if (eventTime < nextTimeValue) {
      context.timerService().deleteEventTimeTimer(nextTimeValue);
      context.timerService().registerEventTimeTimer(eventTime);
      nextTimer.update(eventTime);
    }
  }

  public void onTimer(long timestamp, OnTimerContext ctx, Collector<TinyJoinedImpression> out)
      throws Exception {
    super.onTimer(timestamp, ctx, out);

    long nextImpressionTimer = processDelayedImpressions(timestamp, ctx, out);
    // Process Actions after Impressions since updated Impression state is used in this function.
    long nextActionTimer = processDelayedActions(timestamp, ctx);
    long nextCleanupTimer = clearOldData(timestamp);

    long nextTimerValue =
        Math.min(Math.min(nextImpressionTimer, nextActionTimer), nextCleanupTimer);
    if (Long.MAX_VALUE == nextTimerValue) {
      // No more work.  Clear our nextTimer state.
      // TODO - might make sense to limit these asserts to some debug runs.
      Preconditions.checkArgument(
          keyToReducedImpression.isEmpty(),
          "keyToReducedImpression should be empty if there's no nextTimer.");
      if (delayOutputImpressions.get().iterator().hasNext()) {
        throw new IllegalArgumentException(
            "delayOutputImpressions should be empty if there's no nextTimer.  delayOutputImpressions="
                + delayOutputImpressions.get());
      }
      Preconditions.checkArgument(
          !delayOutputActionPaths.get().iterator().hasNext(),
          "delayOutputImpressions should be empty if there's no nextTimer.");
      nextTimer.clear();
    } else {
      nextTimer.update(nextTimerValue);
      ctx.timerService().registerEventTimeTimer(nextTimerValue);
    }
  }

  /**
   * Process Impressions that happen before {@code timer}. Removes earlier impressions from the
   * state, sorts them and keeps the first Impressions by {@code getKey}.
   *
   * @return the next timer.
   */
  private long processDelayedImpressions(
      long timer, OnTimerContext ctx, Collector<TinyJoinedImpression> out) throws Exception {
    long nextTimer = Long.MAX_VALUE;
    List<TinyJoinedImpression> willProcess = new ArrayList<>();
    // Impressions with times greater than the watermark are not processed.  It might be more
    // efficient to remove redundant events that have not been processed but the current solution
    // keeps the code simple.
    List<TinyJoinedImpression> updatedDelayOutputImpressions = new ArrayList<>();
    for (TinyJoinedImpression delayedImpression : delayOutputImpressions.get()) {
      if (delayedImpression.getImpression().getCommon().getEventApiTimestamp() <= timer) {
        willProcess.add(delayedImpression);
      } else {
        updatedDelayOutputImpressions.add(delayedImpression);
        nextTimer =
            Math.min(
                nextTimer, delayedImpression.getImpression().getCommon().getEventApiTimestamp());
      }
    }
    delayOutputImpressions.update(updatedDelayOutputImpressions);

    willProcess.sort(TINY_JOINED_IMPRESSION_TIMESTAMP_COMPARATOR);

    // TODO - should we cache the map updates in-memory instead of relying on the Flink MapState?
    for (TinyJoinedImpression joinedImpression : willProcess) {
      Tuple2<String, Long> key = getKey(joinedImpression);
      Tuple2<TinyJoinedImpression, Long> reducedState = keyToReducedImpression.get(key);
      TinyJoinedImpression reducedImpression;
      long cleanUpTime;
      if (reducedState == null) {
        reducedImpression = joinedImpression;
        cleanUpTime =
            joinedImpression.getImpression().getCommon().getEventApiTimestamp()
                + inclusiveTtlMillis;
        out.collect(joinedImpression);
      } else {
        reducedImpression = reducedState.f0;
        TinyImpression impression = joinedImpression.getImpression();
        ctx.output(
            REDUNDANT_IMPRESSION,
            RedundantImpression.newBuilder()
                .setPlatformId(impression.getCommon().getPlatformId())
                .setEventApiTimestamp(impression.getCommon().getEventApiTimestamp())
                .setRedundantImpressionId(impression.getImpressionId())
                .setImpressionId(reducedImpression.getImpression().getImpressionId())
                .build());
        // Extend the clean-up timer.
        cleanUpTime =
            Math.max(
                reducedState.f1,
                joinedImpression.getImpression().getCommon().getEventApiTimestamp()
                    + inclusiveTtlMillis);
      }
      keyToReducedImpression.put(key, Tuple2.of(reducedImpression, cleanUpTime));
    }

    return nextTimer;
  }

  /**
   * Process Actions that happen before {@code timer}. Removes earlier actions from the state and
   * updates impression references with the reduced impressions.
   *
   * @return the next timer.
   */
  private long processDelayedActions(long timer, OnTimerContext ctx) throws Exception {
    long nextTimer = Long.MAX_VALUE;
    List<TinyActionPath> updatedDelayOutputActionPaths = new ArrayList<>();
    for (TinyActionPath delayedAction : delayOutputActionPaths.get()) {
      long time = getMaxTime(delayedAction);
      if (time <= timer) {
        ctx.output(OUTPUT_ACTION_PATH, reduceRedundantImpressions(delayedAction));
      } else {
        updatedDelayOutputActionPaths.add(delayedAction);
        nextTimer = Math.min(nextTimer, time);
      }
    }
    delayOutputActionPaths.update(updatedDelayOutputActionPaths);

    return nextTimer;
  }

  /**
   * Returns a safe time for outputting TinyActionPath. Handles the case if impressions happen
   * slightly out of order with the actions.
   */
  private long getMaxTime(TinyActionPath actionPath) {
    long time = actionPath.getAction().getCommon().getEventApiTimestamp();
    for (TinyTouchpoint touchpoint : actionPath.getTouchpointsList()) {
      time =
          Math.max(
              time,
              touchpoint.getJoinedImpression().getImpression().getCommon().getEventApiTimestamp());
    }
    return time;
  }

  private static Tuple2<String, Long> getKey(TinyJoinedImpression impression) {
    String durableRequestId = impression.getInsertion().getPagingId();
    if (isPagingIdEmpty(durableRequestId)) {
      durableRequestId = TinyFlatUtil.getViewId(impression);
    }
    return Tuple2.of(durableRequestId, impression.getInsertion().getCore().getPosition());
  }

  /**
   * Returns a copy of ActionPath that replaces/removes redundant Impressions. If nothing is
   * modified, the original input is returned.
   */
  private TinyActionPath reduceRedundantImpressions(TinyActionPath actionPath) throws Exception {
    // Small optimization to avoid rebuilding.
    boolean modifiedAnyTouchpoints = false;
    ImmutableList.Builder<TinyTouchpoint> touchpoints =
        ImmutableList.builderWithExpectedSize(actionPath.getTouchpointsList().size());
    Set<Tuple2<String, Long>> seenKeys =
        Sets.newHashSetWithExpectedSize(actionPath.getTouchpointsList().size());

    for (TinyTouchpoint touchpoint : actionPath.getTouchpointsList()) {
      Tuple2<String, Long> key = getKey(touchpoint.getJoinedImpression());
      if (seenKeys.contains(key)) {
        // Skip keys that we've seen before.
        modifiedAnyTouchpoints = true;
      } else {
        seenKeys.add(key);
        Tuple2<TinyJoinedImpression, Long> reducedState = keyToReducedImpression.get(key);
        if (reducedState != null) {
          TinyJoinedImpression reducedImpression = reducedState.f0;
          if (reducedImpression
              .getImpression()
              .getImpressionId()
              .equals(touchpoint.getJoinedImpression().getImpression().getImpressionId())) {
            touchpoints.add(touchpoint);
          } else {
            // The touchpoint needs to be replaced.
            touchpoints.add(
                TinyTouchpoint.newBuilder().setJoinedImpression(reducedImpression).build());
            modifiedAnyTouchpoints = true;
          }
        } else {
          LOGGER.warn(
              "Encountered TinyActionPath without a registered TinyJoinedImpression. "
                  + "This most likely means a bug in the Join Job.  actionPath={}; key={}",
              actionPath,
              key);
        }
      }
    }

    if (!modifiedAnyTouchpoints) {
      // An optimized case.
      return actionPath;
    } else {
      return actionPath.toBuilder()
          .clearTouchpoints()
          .addAllTouchpoints(touchpoints.build())
          .build();
    }
  }

  /**
   * Cleans up older data.
   *
   * @return the next clean-up timer.
   */
  private long clearOldData(long timerTimestamp) throws Exception {
    // delayOutputImpressions and delayOutputActionPaths get cleared out while processing.
    // Just clear out keyToReducedImpression here.
    Iterator<Map.Entry<Tuple2<String, Long>, Tuple2<TinyJoinedImpression, Long>>> entriesIterator =
        keyToReducedImpression.entries().iterator();

    long lowestNonCleanedUpEventTime = Long.MAX_VALUE;
    // These lists are usually pretty short.
    while (entriesIterator.hasNext()) {
      Tuple2<TinyJoinedImpression, Long> state = entriesIterator.next().getValue();
      long eventCleanupTime = state.f1;
      // Easier to understand why we use `<=` logic using examples.
      // E.g. eventTime=5ms and ttl=1ms.  inclusiveTtlMillis is 0ms.  timer will be at 5ms.
      // Clean-up happens at the end of the 5ms timer.
      if (eventCleanupTime <= timerTimestamp) {
        entriesIterator.remove();
      } else {
        lowestNonCleanedUpEventTime = Math.min(lowestNonCleanedUpEventTime, eventCleanupTime);
      }
    }
    return lowestNonCleanedUpEventTime;
  }
}
