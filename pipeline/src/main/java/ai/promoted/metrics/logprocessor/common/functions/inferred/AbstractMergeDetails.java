package ai.promoted.metrics.logprocessor.common.functions.inferred;

import ai.promoted.metrics.error.MismatchError;
import ai.promoted.metrics.logprocessor.common.error.MismatchErrorTag;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableFunction;
import ai.promoted.metrics.logprocessor.common.util.DebugIds;
import ai.promoted.metrics.logprocessor.common.util.LogUtil;
import ai.promoted.proto.event.UnionEvent;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Base class for MergeDetails classes. Allows joining full details back onto TinyEvent streams.
 *
 * @param <T> Tiny event
 * @param <J> Joined event
 * @param <JB> Joined event builder
 */
public abstract class AbstractMergeDetails<T, J, JB>
    extends KeyedCoProcessFunction<Tuple2<Long, String>, UnionEvent, T, J> {
  private static final Logger LOGGER = LogManager.getLogger(AbstractMergeDetails.class);
  // We'll do Flink state cleanup at this interval.
  private final Duration batchCleanupPeriod;
  private final Duration missingEntityDelay;
  // This flag can be used during cleanup duration migration to force batch cleanups regularly.
  // Changing cleanup intervals can cause state to be leaked.  Defaults to 0.
  private final long batchCleanupAllTimersBeforeTimestamp;
  private final DebugIds debugIds;
  private final Class<T> tinyEventClazz;
  // For now, if we have IDs but are missing the entities, wait the full missingEntityDelay and then
  // output.
  // This algorithm simplifies this class for now.  We'd need a few more MapStates to output
  // earlier.
  @VisibleForTesting MapState<Long, List<T>> timeToIncompleteEvents;

  public AbstractMergeDetails(
      Duration batchCleanupPeriod,
      Duration missingEntityDelay,
      long batchCleanupAllTimersBeforeTimestamp,
      DebugIds debugIds,
      Class<T> tinyEventClazz) {
    this.batchCleanupPeriod = batchCleanupPeriod;
    this.missingEntityDelay = missingEntityDelay;
    this.batchCleanupAllTimersBeforeTimestamp = batchCleanupAllTimersBeforeTimestamp;
    this.debugIds = debugIds;
    this.tinyEventClazz = tinyEventClazz;
  }

  @Override
  public void open(Configuration config) throws Exception {
    super.open(config);
    timeToIncompleteEvents =
        getRuntimeContext()
            .getMapState(
                // TODO(PRO-1683) - add caches back in.
                new MapStateDescriptor<>(
                    "time-to-incomplete-event",
                    Types.LONG,
                    Types.LIST(TypeInformation.of(tinyEventClazz))));
  }

  @Override
  public void processElement2(T rhs, Context ctx, Collector<J> out) throws Exception {
    JB builder = newBuilder();
    EnumSet<MissingEvent> missing =
        fillInFull(builder, rhs, error -> ctx.output(MismatchErrorTag.TAG, error));
    boolean matchesDebugId = matchesDebugIds(rhs);

    // Prefer outputting if we have the required events in order to save Flink state.
    // This really only hurts a small edge case with Optional joins (none currently exist).
    if (hasRequiredEvents(missing)) {
      J event = build(builder);
      if (matchesDebugId) {
        String rhsString = LogUtil.truncate(rhs.toString());
        String eventString = LogUtil.truncate(event.toString());
        LOGGER.info(
            "{} processElement2 complete output; rhs: {}\nevent: {}, key: {}, ts:{}, watermark: {}",
            getClass().getSimpleName(),
            rhsString,
            eventString,
            ctx.getCurrentKey(),
            ctx.timestamp(),
            ctx.timerService().currentWatermark());
      }
      out.collect(event);
      // We do not need to run a batch cleanup timer here since state should not have changed.
    } else {
      long timerTimestamp = ctx.timestamp() + missingEntityDelay.toMillis();
      if (matchesDebugId) {
        String rhsString = LogUtil.truncate(rhs.toString());
        String missingString = LogUtil.truncate(missing.toString());
        LOGGER.info(
            "{} processElement2 incomplete output; rhs: {}, missing: {}, key: {}, timerTimestamp: {}, ts:{}, watermark: {}",
            getClass().getSimpleName(),
            rhsString,
            missingString,
            ctx.getCurrentKey(),
            timerTimestamp,
            ctx.timestamp(),
            ctx.timerService().currentWatermark());
      }
      List<T> incompleteEvents = timeToIncompleteEvents.get(timerTimestamp);
      if (incompleteEvents == null) {
        incompleteEvents = new ArrayList<>();
      } else {
        // Copy the list.
        incompleteEvents = new ArrayList<>(incompleteEvents);
      }
      incompleteEvents.add(rhs);
      timeToIncompleteEvents.put(timerTimestamp, incompleteEvents);
      addIncompleteTimers(rhs, ctx, missing, timerTimestamp);
      // For fast output.
      ctx.timerService().registerEventTimeTimer(timerTimestamp);
      ctx.timerService().registerEventTimeTimer(toCleanupTimestamp(timerTimestamp));
    }
  }

  /**
   * Add timers for {@code missing} events. These timers are usually used to speed of out-of-order
   * joins.
   */
  protected abstract void addIncompleteTimers(
      T rhs, Context ctx, EnumSet<MissingEvent> missing, long timerTimestamp) throws Exception;

  /** Returns the next, closest cleanup timestamp. */
  private long toCleanupTimestamp(long timestamp) {
    return timestamp - (timestamp % batchCleanupPeriod.toMillis()) + batchCleanupPeriod.toMillis();
  }

  protected void addIncompleteTimers(
      MapState<String, List<Long>> idToIncompleteEventTimers, String id, Long timer)
      throws Exception {
    List<Long> timers = idToIncompleteEventTimers.get(id);
    if (timers == null) {
      timers = new ArrayList<>();
    }
    // TODO - evaluate switching to sort and binary search.
    if (!timers.contains(timer)) {
      timers.add(timer);
      idToIncompleteEventTimers.put(id, timers);
    }
  }

  /**
   * Quick v1 cleaning solution. Scan through state and clear out values. Infrequently do bigger
   * cleanup.
   */
  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<J> out) throws Exception {
    super.onTimer(timestamp, ctx, out);
    List<T> incompleteEvents = timeToIncompleteEvents.get(timestamp);
    if (incompleteEvents != null) {
      for (T incompleteEvent : incompleteEvents) {
        JB builder = newBuilder();
        EnumSet<MissingEvent> missing =
            fillInFull(builder, incompleteEvent, error -> ctx.output(MismatchErrorTag.TAG, error));

        J event = build(builder);
        if (matchesDebugIds(incompleteEvent)) {
          LOGGER.info(
              "{} onTimer incomplete output; incompleteEvent: {}, missing: {}, event: {}, key: {}, state: {}",
              getClass().getSimpleName(),
              incompleteEvent,
              missing,
              event,
              ctx.getCurrentKey(),
              getAllRelatedStateString(incompleteEvent));
        }
        if (hasRequiredEvents(missing)) {
          out.collect(event);
        } else {
          outputDropped(ctx, incompleteEvent, event);
        }
      }
    }
    timeToIncompleteEvents.remove(timestamp);
    cleanupMergers(timestamp);
  }

  // TODO - we migh be able to remove errorLogger.
  /** Returns the set of entity types which are missing. */
  protected abstract EnumSet<MissingEvent> fillInFull(
      JB builder, T rhs, Consumer<MismatchError> errorLogger) throws Exception;

  protected abstract JB newBuilder();

  protected abstract J build(JB builder);

  protected abstract boolean matchesDebugIds(T rhs);

  protected abstract void outputDropped(
      KeyedCoProcessFunction.Context ctx, T tinyEvent, J joinedEvent);

  /** Returns a debug string with relevant state. */
  protected abstract String getAllRelatedStateString(T event) throws Exception;

  /** Clean up Merger state. */
  protected abstract void cleanupMergers(long timestamp) throws Exception;

  /** Returns false if any required event type is in {@code missing}. */
  protected abstract boolean hasRequiredEvents(EnumSet<MissingEvent> missing);

  protected boolean isBatchCleanup(long timestamp) {
    return timestamp < batchCleanupAllTimersBeforeTimestamp
        || timestamp % batchCleanupPeriod.toMillis() == 0;
  }

  protected void cleanupOldTimers(
      MapState<String, List<Long>> idToIncompleteEventTimers, long timestamp, String rowType)
      throws Exception {
    List<String> deletes = new ArrayList<>();
    Map<String, List<Long>> updates = new HashMap<>();
    for (Map.Entry<String, List<Long>> entry : idToIncompleteEventTimers.entries()) {
      List<Long> newTimers =
          entry.getValue().stream().filter(timer -> timer > timestamp).collect(Collectors.toList());
      if (newTimers.isEmpty()) {
        deletes.add(entry.getKey());
      } else {
        updates.put(entry.getKey(), newTimers);
      }
      if (debugIds.matches(entry.getKey())) {
        LOGGER.info(
            "{} batch cleanupOldTimers {} {} entries; key: {}",
            getClass().getSimpleName(),
            rowType,
            newTimers.isEmpty() ? "removed all" : "kept some",
            entry.getKey());
      }
    }
    for (String delete : deletes) {
      idToIncompleteEventTimers.remove(delete);
    }
    for (Map.Entry<String, List<Long>> entry : updates.entrySet()) {
      idToIncompleteEventTimers.put(entry.getKey(), entry.getValue());
    }
  }

  protected <R, K> void cleanupDetailsState(
      MapState<K, R> idToRow,
      long timestamp,
      Function<R, Long> getRowTimestamp,
      Duration cleanupWindow,
      String rowType,
      Function<K, Boolean> matchesDebugIdFn)
      throws Exception {
    List<K> removeRowKeys = new ArrayList<>();
    for (Map.Entry<K, R> entry : idToRow.entries()) {
      K key = entry.getKey();
      boolean matchesDebugId = matchesDebugIdFn.apply(key);
      long rowTimestamp = getRowTimestamp.apply(entry.getValue());
      if (rowTimestamp <= 0) {
        throw new IllegalArgumentException(
            rowType + " does not have eventApiTimestamp, row=" + entry.getValue());
      }
      boolean remove = rowTimestamp < timestamp - cleanupWindow.toMillis();
      if (remove) {
        removeRowKeys.add(entry.getKey());
      }
      if (matchesDebugId) {
        LOGGER.info(
            "{} cleanupMapState {} entry; rowType: {}, key: {}, timestamp: {}",
            getClass().getSimpleName(),
            remove ? "removed" : "kept",
            rowType,
            key,
            timestamp);
      }
    }
    for (K removeRowKey : removeRowKeys) {
      idToRow.remove(removeRowKey);
    }
  }

  @VisibleForTesting
  enum MissingEvent {
    DELIERY_LOG,
    IMPRESSION,
    JOINED_IMPRESSION,
    ACTION,
  }

  /**
   * ABC for Mergers. Done to help template processing of each entity type.
   *
   * @param <D> dimension (full event details that we're joining in)
   */
  abstract class BaseMerger<D> implements Serializable {
    private static final long serialVersionUID = 2L;

    protected final SerializableFunction<UnionEvent, D> getEvent;
    protected final Duration window;
    protected final SerializableFunction<D, Boolean> matchesDebugIdFn;

    // TODO - construct that contains getEvent and cleanupDuration.
    protected BaseMerger(
        SerializableFunction<UnionEvent, D> getEvent,
        Duration window,
        SerializableFunction<D, Boolean> matchesDebugIdFn) {
      this.getEvent = getEvent;
      this.window = window;
      this.matchesDebugIdFn = matchesDebugIdFn;
    }

    void processElement1(UnionEvent input, Context ctx, Collector<J> out) throws Exception {
      D dimension = getEvent.apply(input);
      boolean matchesDebugId = matchesDebugIdFn.apply(dimension);
      // TODO - switch to log timestamps on the records.
      long cleanupTimestamp = toCleanupTimestamp(ctx.timestamp() + window.toMillis());
      if (matchesDebugId) {
        String inputString = LogUtil.truncate(input.toString());
        LOGGER.info(
            "AbstractMergeDetails.{} processElement1; eventCase={} input={}, ts:{}, watermark: {}, cleanupTs: {}, key: {}, state: {}",
            this.getClass().getSimpleName(),
            input.getEventCase(),
            inputString,
            ctx.timestamp(),
            ctx.timerService().currentWatermark(),
            cleanupTimestamp,
            ctx.getCurrentKey(),
            getDebugStateString(dimension));
      }
      addFull(dimension);
      if (matchesDebugId) {
        String inputString = LogUtil.truncate(input.toString());
        LOGGER.info(
            "AbstractMergeDetails.{} processElement1 after addFull; eventCase={}, input={}, key: {}, state: {}",
            this.getClass().getSimpleName(),
            input.getEventCase(),
            inputString,
            ctx.getCurrentKey(),
            getDebugStateString(dimension));
      }
      tryProcessIncompleteEvents(
          dimension,
          matchesDebugId,
          error -> ctx.output(MismatchErrorTag.TAG, error),
          out,
          ctx.timestamp(),
          ctx.timerService().currentWatermark());
      ctx.timerService().registerEventTimeTimer(cleanupTimestamp);
    }

    /** Adds the full event into the merger's state. */
    protected abstract void addFull(D dimension) throws Exception;

    /**
     * Tries to process incomplete events. Takes {@code out} in case has to output the now completed
     * events.
     */
    protected abstract void tryProcessIncompleteEvents(
        D dimension,
        boolean matchesDebugId,
        Consumer<MismatchError> errorLogger,
        Collector<J> out,
        long timestamp,
        long watermark)
        throws Exception;

    /**
     * Returns a string for debug logging that represents matching state for the entity ID. Only
     * matches with the ID fields set on `dimension`. This might have a bunch of missing fields.
     */
    protected abstract String getDebugStateString(D dimension) throws Exception;

    /**
     * Go through idToIncompleteEventTimers for the key and sees if we should immediately output any
     * completed JoinedEvents.
     */
    protected void tryProcessIncompleteEvents(
        String key,
        boolean matchesDebugId,
        MapState<String, List<Long>> idToIncompleteEventTimers,
        Consumer<MismatchError> errorLogger,
        Collector<J> out,
        long timestamp,
        long watermark)
        throws Exception {
      List<Long> incompleteTimers = idToIncompleteEventTimers.get(key);
      if (incompleteTimers != null) {
        ImmutableList.Builder<Long> incompleteTimersBuilder = ImmutableList.builder();
        for (Long incompleteTimer : incompleteTimers) {
          List<T> incompleteEvents = timeToIncompleteEvents.get(incompleteTimer);
          if (incompleteEvents != null) {
            ImmutableList.Builder<T> stillIncompleteEventsBuilder = ImmutableList.builder();
            for (T incompleteEvent : incompleteEvents) {
              JB builder = newBuilder();
              EnumSet<MissingEvent> missingEvents =
                  fillInFull(builder, incompleteEvent, errorLogger);
              // Prefer outputting if we have the required events in order to save Flink state.
              // This really only hurts a small edge case with optional joins (none currently
              // exist).
              if (hasRequiredEvents(missingEvents)) {
                J joinedEvent = build(builder);
                if (matchesDebugId) {
                  LOGGER.info(
                      "AbstractMergeDetails.{} tryProcessIncompleteEvents output complete; key={}, joinedEvent={}, ts={}, watermark={}",
                      this.getClass().getSimpleName(),
                      key,
                      joinedEvent,
                      timestamp,
                      watermark);
                }
                out.collect(joinedEvent);
              } else {
                stillIncompleteEventsBuilder.add(incompleteEvent);
              }
            }
            List<T> stillIncompleteEvents = stillIncompleteEventsBuilder.build();
            if (stillIncompleteEvents.isEmpty()) {
              timeToIncompleteEvents.remove(incompleteTimer);
              if (matchesDebugId) {
                LOGGER.info(
                    "AbstractMergeDetails.{} tryProcessIncompleteEvents finished entry in timeToIncompleteEvents; key={}, incompleteTimer={}, ts={}, watermark={}",
                    this.getClass().getSimpleName(),
                    key,
                    incompleteTimer,
                    timestamp,
                    watermark);
              }
            } else {
              timeToIncompleteEvents.put(incompleteTimer, stillIncompleteEvents);
              incompleteTimersBuilder.add(incompleteTimer);
              if (matchesDebugId) {
                LOGGER.info(
                    "AbstractMergeDetails.{} tryProcessIncompleteEvents still has entry in timeToIncompleteEvents; key={}, incompleteTimer={}, stillIncompleteEvents{}, ts={}, watermark={}",
                    this.getClass().getSimpleName(),
                    key,
                    incompleteTimer,
                    stillIncompleteEvents,
                    timestamp,
                    watermark);
              }
            }
          }
        }
        List<Long> newIncompleteTimers = incompleteTimersBuilder.build();
        if (newIncompleteTimers.isEmpty()) {
          idToIncompleteEventTimers.remove(key);
          if (matchesDebugId) {
            LOGGER.info(
                "AbstractMergeDetails.{} tryProcessIncompleteEvents finished entry idToIncompleteEventTimers; key={}, ts={}, watermark={}",
                this.getClass().getSimpleName(),
                key,
                timestamp,
                watermark);
          }
        } else {
          idToIncompleteEventTimers.put(key, newIncompleteTimers);
          if (matchesDebugId) {
            LOGGER.info(
                "AbstractMergeDetails.{} tryProcessIncompleteEvents still has entry in idToIncompleteEventTimers; key={}, newIncompleteTimers={}, ts={}, watermark={}",
                this.getClass().getSimpleName(),
                key,
                newIncompleteTimers,
                timestamp,
                watermark);
          }
        }
      }
    }
  }
}
