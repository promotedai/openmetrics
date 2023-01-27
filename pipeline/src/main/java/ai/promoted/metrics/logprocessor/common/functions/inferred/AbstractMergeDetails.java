package ai.promoted.metrics.logprocessor.common.functions.inferred;

import ai.promoted.metrics.error.MismatchError;
import ai.promoted.metrics.logprocessor.common.functions.SerializableFunction;
import ai.promoted.metrics.logprocessor.common.util.DebugIds;
import ai.promoted.proto.event.DroppedMergeDetailsEvent;
import ai.promoted.proto.event.JoinedEvent;
import ai.promoted.proto.event.TinyEvent;
import ai.promoted.proto.event.UnionEvent;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Base class for MergeDetails classes.  Allows joining full details back onto TinyEvent streams.
 */
public abstract class AbstractMergeDetails extends KeyedCoProcessFunction<Tuple2<Long, String>, UnionEvent, TinyEvent, JoinedEvent> {
    private static final Logger LOGGER = LogManager.getLogger(AbstractMergeDetails.class);

    @VisibleForTesting
    enum MissingEvent {
        VIEW,
        DELIERY_LOG,
        IMPRESSION,
        JOINED_IMPRESSION,
        ACTION,
    }

    public static final OutputTag<DroppedMergeDetailsEvent> DROPPED_TAG = new OutputTag<DroppedMergeDetailsEvent>("dropped"){};

    // We'll do Flink state cleanup at this interval.
    private final Duration batchCleanupPeriod;
    private final Duration missingEntityDelay;
    // This flag can be used during cleanup duration migration to force batch cleanups regularly.
    // Changing cleanup intervals can cause state to be leaked.  Defaults to 0.
    private final long batchCleanupAllTimersBeforeTimestamp;
    private final DebugIds debugIds;

    // For now, if we have IDs but are missing the entities, wait the full missingEntityDelay and then output.
    // This algorithm simplifies this class for now.  We'd need a few more MapStates to output earlier.
    @VisibleForTesting MapState<Long, List<TinyEvent>> timeToIncompleteEvents;

    public AbstractMergeDetails(
            Duration batchCleanupPeriod,
            Duration missingEntityDelay,
            long batchCleanupAllTimersBeforeTimestamp,
            DebugIds debugIds) {
        this.batchCleanupPeriod = batchCleanupPeriod;
        this.missingEntityDelay = missingEntityDelay;
        this.batchCleanupAllTimersBeforeTimestamp = batchCleanupAllTimersBeforeTimestamp;
        this.debugIds = debugIds;
    }

    @Override
    public void open(Configuration config) throws Exception {
        super.open(config);
        timeToIncompleteEvents = getRuntimeContext().getMapState(
                // TODO(PRO-1683) - add caches back in.
                new MapStateDescriptor<>("time-to-incomplete-event", Types.LONG, Types.LIST(TypeInformation.of(TinyEvent.class))));
    }

    @Override
    public void processElement2(TinyEvent rhs, Context ctx, Collector<JoinedEvent> out) throws Exception {
        JoinedEvent.Builder builder = JoinedEvent.newBuilder();
        EnumSet<MissingEvent> missing = fillInFull(builder, rhs, ctx::output);
        boolean matchesDebugId = debugIds.matches(rhs);

        // Prefer outputting if we have the required events in order to save Flink state.
        // This really only hurts a small edge case with View joins.
        if (hasRequiredEvents(missing)) {
            JoinedEvent event = builder.build();
            if (matchesDebugId) {
                LOGGER.info("{} processElement2 complete output; rhs: {}\nevent: {}\nts:{}\nwatermark: {}",
                        getClass().getSimpleName(), rhs, event, ctx.timestamp(), ctx.timerService().currentWatermark());
            }
            if (event.getIds().getImpressionId().isEmpty()) {
                // To help catch PRO-1758 - bad flat_impression output.  Some of the records are completely empty.  Some do not
                // have an impressionId.  Log more info for now to help track it down.
                String error = String.format("Outputting an incomplete JoinedEvent from %s.processElement2.  event=%s, rhs=%s",
                        getClass().getSimpleName(), event, rhs);
                LOGGER.error(error);
            }
            out.collect(event);
            // We do not need to run a batch cleanup timer here since state should not have changed.
        } else {
            long timerTimestamp = ctx.timestamp() + missingEntityDelay.toMillis();
            if (matchesDebugId) {
                LOGGER.info("{} processElement2 incomplete output; rhs: {}\nmissing: {}\ntimerTimestamp: {}\nevent: {}\nts:{}\nwatermark: {}",
                        getClass().getSimpleName(), rhs, missing, timerTimestamp, ctx.timestamp(), ctx.timerService().currentWatermark());
            }
            List<TinyEvent> incompleteEvents = timeToIncompleteEvents.get(timerTimestamp);
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
     * Add timers for {@code missing} events.  These timers are usually used to speed of out-of-order joins.
     */
    protected abstract void addIncompleteTimers(TinyEvent rhs, Context ctx, EnumSet<MissingEvent> missing, long timerTimestamp) throws Exception;

    /**
     * Returns the next, closest cleanup timestamp.
     */
    private long toCleanupTimestamp(long timestamp) {
        return timestamp - (timestamp % batchCleanupPeriod.toMillis()) + batchCleanupPeriod.toMillis();
    }

    protected void addIncompleteTimers(MapState<String, List<Long>> idToIncompleteEventTimers, String id, Long timer) throws Exception {
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

    /** Returns the set of entity types which are missing. */
    protected abstract EnumSet<MissingEvent> fillInFull(JoinedEvent.Builder builder, TinyEvent rhs, BiConsumer<OutputTag<MismatchError>, MismatchError> errorLogger) throws Exception;

    /** Quick v1 cleaning solution.  Scan through state and clear out values.  Infrequently do bigger cleanup. */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<JoinedEvent> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        List<TinyEvent> incompleteEvents = timeToIncompleteEvents.get(timestamp);
        if (incompleteEvents != null) {
            for (TinyEvent incompleteEvent : incompleteEvents) {
                JoinedEvent.Builder builder = JoinedEvent.newBuilder();
                EnumSet<MissingEvent> missing = fillInFull(builder, incompleteEvent, ctx::output);

                JoinedEvent event = builder.build();
                if (debugIds.matches(incompleteEvent)) {
                    LOGGER.info("{} onTimer incomplete output; incompleteEvent: {}\nmissing: {}\nevent: {}, state: {}",
                            getClass().getSimpleName(), incompleteEvent, missing, event,
                            getAllRelatedStateString(incompleteEvent.getViewId(), incompleteEvent.getRequestId(), incompleteEvent.getImpressionId(), incompleteEvent.getActionId()));
                }
                if (event.getIds().getImpressionId().isEmpty()) {
                    // To help catch PRO-1758 - bad flat_impression output.  Some of the records are completely empty.  Some do not
                    // have an impressionId.  Log more info for now to help track it down.
                    String error = String.format("Outputting an incomplete JoinedEvent from %s.onTimer.  event=%s, incompleteEvent=%s, missing=%s",
                            getClass().getSimpleName(), event, incompleteEvent, missing);
                    LOGGER.error(error);
                }
                if (hasRequiredEvents(missing)) {
                    out.collect(event);
                } else {
                    ctx.output(DROPPED_TAG, DroppedMergeDetailsEvent.newBuilder()
                            .setTinyEvent(incompleteEvent)
                            .setJoinedEvent(event)
                            .build());
                }
            }
        }
        timeToIncompleteEvents.remove(timestamp);
        cleanupMergers(timestamp);
    }

    /**
     * Returns a debug string with relevant state.
     */
    protected abstract String getAllRelatedStateString(String viewId, String requestId, String impressionId, String actionId) throws Exception;

    /**
     * Clean up Merger state.
     */
    protected abstract void cleanupMergers(long timestamp) throws Exception;

    /**
     * Returns false if any required event type is in {@code missing}.
     */
    protected abstract boolean hasRequiredEvents(EnumSet<MissingEvent> missing);

    protected boolean isBatchCleanup(long timestamp) {
        return timestamp < batchCleanupAllTimersBeforeTimestamp || timestamp % batchCleanupPeriod.toMillis() == 0;
    }

    protected void cleanupOldTimers(MapState<String, List<Long>> idToIncompleteEventTimers, long timestamp, String rowType) throws Exception {
        List<String> deletes = new ArrayList<>();
        Map<String, List<Long>> updates = new HashMap<>();
        for (Map.Entry<String, List<Long>> entry : idToIncompleteEventTimers.entries()) {
            List<Long> newTimers = entry.getValue().stream().filter(timer -> timer > timestamp).collect(Collectors.toList());
            if (newTimers.isEmpty()) {
                deletes.add(entry.getKey());
            } else {
                updates.put(entry.getKey(), newTimers);
            }
            if (debugIds.matches(entry.getKey())) {
                LOGGER.info("{} batch cleanupOldTimers {} {} entries; key: {}",
                        getClass().getSimpleName(), rowType, newTimers.isEmpty() ? "removed all" : "kept some", entry.getKey());
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
            Function<K, Boolean> matchesDebugIdFn
    ) throws Exception {
        List<K> removeRowKeys = new ArrayList<>();
        for (Map.Entry<K, R> entry : idToRow.entries()) {
            K key = entry.getKey();
            boolean matchesDebugId = matchesDebugIdFn.apply(key);
            long rowTimestamp = getRowTimestamp.apply(entry.getValue());
            if (rowTimestamp <= 0) {
                throw new IllegalArgumentException(rowType + " does not have logTimestamp, row=" + entry.getValue());
            }
            boolean remove = rowTimestamp < timestamp - cleanupWindow.toMillis();
            if (remove) {
                removeRowKeys.add(entry.getKey());
            }
            if (matchesDebugId) {
                LOGGER.info("{} cleanupMapState {} entry; rowType: {}\nkey: {}\ntimestamp: {}",
                        getClass().getSimpleName(), remove ? "removed" : "kept", rowType, key, timestamp);
            }
        }
        for (K removeRowKey : removeRowKeys) {
            idToRow.remove(removeRowKey);
        }
    }

    /**
     * ABC for Mergers.  Done to help template processing of each entity type.
     *
     * @param <T> The main type for the Merger.
     */
    abstract class BaseMerger<T> implements Serializable {
        private static final long serialVersionUID = 2L;

        protected final SerializableFunction<UnionEvent, T> getEvent;
        protected final Duration window;
        protected final SerializableFunction<T, Boolean> matchesDebugIdFn;

        // TODO - construct that contains getEvent and cleanupDuration.
        protected BaseMerger(SerializableFunction<UnionEvent, T> getEvent, Duration window, SerializableFunction<T, Boolean> matchesDebugIdFn) {
            this.getEvent = getEvent;
            this.window = window;
            this.matchesDebugIdFn = matchesDebugIdFn;
        }

        void processElement1(UnionEvent input, Context ctx, Collector<JoinedEvent> out) throws Exception {
            T event = getEvent.apply(input);
            boolean matchesDebugId = matchesDebugIdFn.apply(event);
            // TODO - switch to log timestamps on the records.
            long cleanupTimestamp = toCleanupTimestamp(ctx.timestamp() + window.toMillis());
            if (matchesDebugId) {
                LOGGER.info("AbstractMergeDetails.{} processElement1; eventCase={} input={}\n ts:{}\nwatermark: {}\ncleanupTs: {}, state: {}",
                        this.getClass().getSimpleName(),
                        input.getEventCase(),
                        input,
                        ctx.timestamp(),
                        ctx.timerService().currentWatermark(),
                        cleanupTimestamp,
                        getDebugStateString(event));
            }
            addFull(event);
            tryProcessIncompleteEvents(event, matchesDebugId, ctx::output, out);
            ctx.timerService().registerEventTimeTimer(cleanupTimestamp);
        }

        /**
         * Adds the full event into the merger's state.
         */
        protected abstract void addFull(T event) throws Exception;

        /**
         * Tries to process incomplete events.  Takes {@code out} in case has to output the now completed events.
         */
        protected abstract void tryProcessIncompleteEvents(T event, boolean matchesDebugId, BiConsumer<OutputTag<MismatchError>, MismatchError> errorLogger, Collector<JoinedEvent> out) throws Exception;

        /**
         * Returns a string for debug logging that represents matching state for the entity ID.
         * Only matches with the ID fields set on `event`.  This might have a bunch of missing fields.
         */
        protected abstract String getDebugStateString(T event) throws Exception;

        /**
         * Go through idToIncompleteEventTimers for the key and sees if we should immediately output any completed JoinedEvents.
         */
        protected void tryProcessIncompleteEvents(String key, boolean matchesDebugId, MapState<String, List<Long>> idToIncompleteEventTimers, BiConsumer<OutputTag<MismatchError>, MismatchError> errorLogger, Collector<JoinedEvent> out) throws Exception {
            List<Long> incompleteTimers = idToIncompleteEventTimers.get(key);
            if (incompleteTimers != null) {
                ImmutableList.Builder<Long> incompleteTimersBuilder = ImmutableList.builder();
                for (Long incompleteTimer : incompleteTimers) {
                    List<TinyEvent> incompleteEvents = timeToIncompleteEvents.get(incompleteTimer);
                    if (incompleteEvents != null) {
                        ImmutableList.Builder<TinyEvent> stillIncompleteEventsBuilder = ImmutableList.builder();
                        for (TinyEvent incompleteEvent : incompleteEvents) {
                            JoinedEvent.Builder builder = JoinedEvent.newBuilder();
                            EnumSet<MissingEvent> missingEvents = fillInFull(builder, incompleteEvent, errorLogger);
                            // Prefer outputting if we have the required events in order to save Flink state.
                            // This really only hurts a small edge case with View joins.
                            if (hasRequiredEvents(missingEvents)) {
                                JoinedEvent joinedEvent = builder.build();
                                if (matchesDebugId) {
                                    LOGGER.info("AbstractMergeDetails.{} tryProcessIncompleteEvents output complete; key={}\njoinedEvent={}",
                                            this.getClass().getSimpleName(), key, joinedEvent);
                                }
                                out.collect(joinedEvent);
                            } else {
                                stillIncompleteEventsBuilder.add(incompleteEvent);
                            }
                        }
                        List<TinyEvent> stillIncompleteEvents = stillIncompleteEventsBuilder.build();
                        if (stillIncompleteEvents.isEmpty()) {
                            timeToIncompleteEvents.remove(incompleteTimer);
                            if (matchesDebugId) {
                                LOGGER.info("AbstractMergeDetails.{} tryProcessIncompleteEvents finished entry in timeToIncompleteEvents; key={}, incompleteTimer={}",
                                        this.getClass().getSimpleName(), key, incompleteTimer);
                            }
                        } else {
                            timeToIncompleteEvents.put(incompleteTimer, stillIncompleteEvents);
                            incompleteTimersBuilder.add(incompleteTimer);
                            if (matchesDebugId) {
                                LOGGER.info("AbstractMergeDetails.{} tryProcessIncompleteEvents still has entry in timeToIncompleteEvents; key={}\nincompleteTimer={}\nstillIncompleteEvents{}",
                                        this.getClass().getSimpleName(), key, incompleteTimer, stillIncompleteEvents);
                            }
                        }
                    }
                }
                List<Long> newIncompleteTimers = incompleteTimersBuilder.build();
                if (newIncompleteTimers.isEmpty()) {
                    idToIncompleteEventTimers.remove(key);
                    if (matchesDebugId) {
                        LOGGER.info("AbstractMergeDetails.{} tryProcessIncompleteEvents finished entry idToIncompleteEventTimers; key={}",
                                this.getClass().getSimpleName(), key);
                    }
                } else {
                    idToIncompleteEventTimers.put(key, newIncompleteTimers);
                    if (matchesDebugId) {
                        LOGGER.info("AbstractMergeDetails.{} tryProcessIncompleteEvents still has entry in idToIncompleteEventTimers; key={}\nnewIncompleteTimers={}",
                                this.getClass().getSimpleName(), key, newIncompleteTimers);
                    }
                }
            }
        }
    }
}
