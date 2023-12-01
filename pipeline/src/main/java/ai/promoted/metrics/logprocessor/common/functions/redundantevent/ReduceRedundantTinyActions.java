package ai.promoted.metrics.logprocessor.common.functions.redundantevent;

import ai.promoted.metrics.common.RedundantAction;
import ai.promoted.metrics.logprocessor.common.util.DebugIds;
import ai.promoted.proto.event.TinyAction;
import ai.promoted.proto.event.TinyActionPath;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// TODO - we should eventually take in an orderId that we can use for deduplicating purchases.
// If someone purchases the same item multiple times, we will remove purchases that we shouldn't.

/**
 * Used to filter out redundant actions.
 *
 * <p>The inputs need to be keyed by the grouping dimensions for reduction (see RedundantEventKey).
 *
 * <p>The output of this function won't execute consistently if the input events come out of order.
 * We can improe this after we support retractions.
 *
 * @param <K> The grouping dimension for reduction IN = TinyEvent for both flat impressions and
 *     actions
 */
public class ReduceRedundantTinyActions<K>
    extends KeyedProcessFunction<K, TinyActionPath, TinyActionPath> {
  private static final Logger LOGGER = LogManager.getLogger(ReduceRedundantTinyActions.class);
  // TODO - implement better type for TinyEvent table.
  /** OutputTag for redundant actions so we can track the issues over time. */
  public static final OutputTag<RedundantAction> REDUNDANT_ACTION =
      new OutputTag<RedundantAction>("redundant-action") {};

  // For testing, there's a higher chance of encountering multiple actions with the same
  // event_api_timestamp.  When this happens, fallback to the IDs.
  private static final Comparator<TinyActionPath> COMPARATOR =
      Comparator.<TinyActionPath>comparingLong(
              actionPath -> actionPath.getAction().getCommon().getEventApiTimestamp())
          .thenComparing(Comparator.comparing(actionPath -> actionPath.getAction().getActionId()))
          .thenComparing(Comparator.comparing(actionPath -> actionPath.getAction().getContentId()));

  // TODO - change the implementation to be similar to ReduceRedundantTinyimpressions.
  // This version can produce incosnistent outputs on re-runs.  We'd need to output in onTimer
  // instead.

  // TTL for state.  Reduces redundant events based on first encountered action timestamp + ttl.
  private final long inclusiveTtlMillis;
  private final DebugIds debugIds;

  // Only used before the `alreadyOutputted` is specified.  Meant to handle
  @VisibleForTesting transient ListState<TinyActionPath> initialDelayOutputActionPaths;

  // Even though we use TinyAction, we only set a small subset of fields that are used to
  // identify redundant events.
  @VisibleForTesting transient ValueState<TinyAction> alreadyOutputted;

  @VisibleForTesting transient ValueState<Long> nextTimer;

  @VisibleForTesting transient ValueState<Long> cleanUpTime;

  public ReduceRedundantTinyActions(Duration ttl, DebugIds debugIds) {
    Preconditions.checkArgument(
        ttl.toMillis() > 0, "ReduceRedundantTinyImpressions TTL needs to be a positive number");
    this.inclusiveTtlMillis = ttl.toMillis() - 1;
    this.debugIds = debugIds;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters); // Doesn't do anything.
    ValueStateDescriptor<TinyAction> alreadyOutputtedTypesDescriptor =
        new ValueStateDescriptor<>("already-outputted-types", TypeInformation.of(TinyAction.class));
    // TTL is handled in our own onTimer code.
    alreadyOutputted = getRuntimeContext().getState(alreadyOutputtedTypesDescriptor);

    initialDelayOutputActionPaths =
        getRuntimeContext()
            .getListState(
                new ListStateDescriptor<>(
                    "initial-delay-output-actions", TypeInformation.of(TinyActionPath.class)));

    nextTimer = getRuntimeContext().getState(new ValueStateDescriptor<>("next-timer", Types.LONG));
    cleanUpTime =
        getRuntimeContext().getState(new ValueStateDescriptor<>("clean-up-time", Types.LONG));
  }

  @Override
  public void processElement(
      TinyActionPath actionPath, Context ctx, Collector<TinyActionPath> collector)
      throws Exception {
    TinyAction alreadyOutputtedAction = alreadyOutputted.value();
    long initialOutputTimer;
    if (alreadyOutputtedAction == null) {
      initialDelayOutputActionPaths.add(actionPath);
      initialOutputTimer = actionPath.getAction().getCommon().getEventApiTimestamp();
    } else {
      outputRedundantAction(actionPath, alreadyOutputtedAction, ctx, "processElement");
      initialOutputTimer = Long.MAX_VALUE;
    }
    long nextCleanUpTime =
        updateCleanUpTimeState(actionPath.getAction().getCommon().getEventApiTimestamp());
    updateNextTimer(ctx, Math.min(initialOutputTimer, nextCleanUpTime));
  }

  private void updateNextTimer(KeyedProcessFunction.Context context, long eventTime)
      throws IOException {
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

  /** Returns the next cleanup time. */
  private long updateCleanUpTimeState(long eventTime) throws IOException {
    Long cleanUpTimeValue = cleanUpTime.value();
    long eventCleanUpTime = eventTime + inclusiveTtlMillis;
    if (cleanUpTimeValue == null) {
      cleanUpTimeValue = eventCleanUpTime;
    } else {
      cleanUpTimeValue = Math.max(cleanUpTimeValue, eventCleanUpTime);
    }
    cleanUpTime.update(cleanUpTimeValue);
    return cleanUpTimeValue;
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<TinyActionPath> out)
      throws Exception {
    super.onTimer(timestamp, ctx, out);
    long nextInitialDelayTimer = processInitialDelayedOutputActionPaths(ctx, out);
    long nextCleanUpTimer = processCleanUp(timestamp);
    long nextTimerValue = Math.min(nextInitialDelayTimer, nextCleanUpTimer);

    if (Long.MAX_VALUE == nextTimerValue) {
      Preconditions.checkArgument(
          !initialDelayOutputActionPaths.get().iterator().hasNext(),
          "initialDelayOutputActionPaths should be empty if there's no nextTimer");
      Preconditions.checkArgument(
          alreadyOutputted.value() == null,
          "alreadyOutputted should be null if there's no nextTimer");
      Preconditions.checkArgument(
          cleanUpTime.value() == null, "cleanUpTime should be null if there's no nextTimer");
      nextTimer.clear();
    } else {
      nextTimer.update(nextTimerValue);
      ctx.timerService().registerEventTimeTimer(nextTimerValue);
    }
  }

  private long processInitialDelayedOutputActionPaths(
      OnTimerContext ctx, Collector<TinyActionPath> out) throws Exception {
    Iterable<TinyActionPath> initialDelayOutputActionPathsIterable =
        initialDelayOutputActionPaths.get();
    if (!initialDelayOutputActionPathsIterable.iterator().hasNext()) {
      // We've already outputted the initially delayed ActionPaths.  The next timer is none.
      return Long.MAX_VALUE;
    }

    List<TinyActionPath> initialDelayOutputActionPathsList =
        ImmutableList.copyOf(initialDelayOutputActionPathsIterable);
    TinyActionPath earliestActionPath =
        Collections.min(initialDelayOutputActionPathsList, COMPARATOR);
    setAndOutputEarliestActionPath(earliestActionPath, out);

    for (TinyActionPath actionPath : initialDelayOutputActionPathsList) {
      if (COMPARATOR.compare(earliestActionPath, actionPath) != 0) {
        outputRedundantAction(actionPath, earliestActionPath.getAction(), ctx, "onTimer");
      }
    }
    // After we've processed this initial delayed list once, we don't need to process again.
    initialDelayOutputActionPaths.clear();
    return Long.MAX_VALUE;
  }

  private long processCleanUp(long timestamp) throws IOException {
    long nextCleanUpTimer = cleanUpTime.value();
    if (nextCleanUpTimer == timestamp) {
      alreadyOutputted.clear();
      cleanUpTime.clear();
      nextCleanUpTimer = Long.MAX_VALUE;
    }
    return nextCleanUpTimer;
  }

  private void setAndOutputEarliestActionPath(
      TinyActionPath actionPath, Collector<TinyActionPath> out) throws IOException {
    TinyAction action = actionPath.getAction();
    boolean shouldDebugLog = debugIds.matches(action);
    if (shouldDebugLog)
      LOGGER.info(
          "{} onTimer: outputting; input={}, is not redundant",
          ReduceRedundantTinyActions.class.getSimpleName(),
          actionPath);
    alreadyOutputted.update(
        TinyAction.newBuilder()
            .setActionId(action.getActionId())
            .setActionType(action.getActionType())
            .setCustomActionType(action.getCustomActionType())
            .build());
    out.collect(actionPath);
  }

  private void outputRedundantAction(
      TinyActionPath redundantActionPath,
      TinyAction alreadyOutputtedAction,
      Context ctx,
      String methodForTextLog) {
    TinyAction redundantAction = redundantActionPath.getAction();
    boolean shouldDebugLog = debugIds.matches(redundantAction);
    if (shouldDebugLog)
      LOGGER.info(
          "{} {}: not outputting; input={}, is redundant",
          ReduceRedundantTinyActions.class.getSimpleName(),
          methodForTextLog,
          redundantAction);
    ctx.output(
        REDUNDANT_ACTION,
        RedundantAction.newBuilder()
            .setPlatformId(redundantAction.getCommon().getPlatformId())
            .setEventApiTimestamp(redundantAction.getCommon().getEventApiTimestamp())
            .setRedundantActionId(redundantAction.getActionId())
            .setRedundantActionType(redundantAction.getActionType().toString())
            .setRedundantCustomActionType(redundantAction.getCustomActionType())
            .setActionId(alreadyOutputtedAction.getActionId())
            .setActionType(alreadyOutputtedAction.getActionType().toString())
            .setCustomActionType(alreadyOutputtedAction.getCustomActionType())
            .build());
  }
}
