package ai.promoted.metrics.logprocessor.common.functions.inferred;

import ai.promoted.metrics.error.MismatchError;
import ai.promoted.metrics.logprocessor.common.functions.SerializableFunction;
import ai.promoted.metrics.logprocessor.common.functions.SerializablePredicate;
import ai.promoted.metrics.logprocessor.common.functions.SerializableToLongFunction;
import ai.promoted.metrics.logprocessor.common.functions.SerializableTriFunction;
import ai.promoted.metrics.logprocessor.common.util.StringUtil;
import ai.promoted.proto.event.TinyEvent;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
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

import java.lang.StringBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * This abstract base class handles explicit joins and sets up ancestor based inferred joins.
 *
 * This base implementation handles both explicit and temporally-closest implicit joins.  Explict
 * joins immediately output when join keys are matched, and inferred joins need to wait until after
 * accumulating enough context to make a reasonable guess (maxOutOfOrder duration).
 *
 * The output flat event's flink timestamp should be interpreted as the RHS's timestamp since the
 * RHS is the actual parent for the downstream join.  In-order explicit joins are fine since the
 * emitted events' flink timestamp should already be the arrived RHS event time.  Out-of-order
 * explicit joins can be worst-case maxOutOfOrder duration delayed, and as mentioned, inferred joins
 * always waits maxOutOfOrder duration before emitting any event.  Therefore, we need to adjust the
 * flink operator's watermark to be delayed by maxOutOfOrder duration
 * {@see ai.promoted.metrics.logprocessor.common.flink.operator.InferenceOperator}.
 *
 * Child classes meant to provide additional joining hints should simply implement ersatz join keys.
 */
public abstract class BaseInferred<RHS extends GeneratedMessageV3> extends KeyedCoProcessFunction<Tuple2<Long, String>, TinyEvent, RHS, TinyEvent> {
    private static final Logger LOGGER = LogManager.getLogger(BaseInferred.class);
    private static final Comparator<TinyEvent> TINY_FLAT_EVENT_LOG_TIMESTAMP_COMPARATOR =
        (a, b) -> Long.valueOf(a.getLogTimestamp()).compareTo(b.getLogTimestamp());

    /**
     * Log debug info when a state list size matches one of the values in this array or
     * is a multiple of the last value.
     * Uses primitive int[] to avoid boxing performance.  This will be called a lot.
     * Warning - this array must have at least one value or MAX_LIST_LONG_THRESHOLD needs updating.
     * Warning - this array must be sorted.
     */
    private static final int[] LIST_LOG_THRESHOLDS = sort(new int[]{500, 1000, 5000, 10000});
    private static final int MAX_LIST_LONG_THRESHOLD = LIST_LOG_THRESHOLDS[LIST_LOG_THRESHOLDS.length - 1];

    private static final int[] sort(int values[]) {
        return Arrays.stream(values).sorted().toArray();
    }

    /** OutputTag to get late events in a side channel. */
    public static final OutputTag<TinyEvent> LATE_EVENTS_TAG = new OutputTag<TinyEvent>("late-events"){};

    /** OutputTag to get duplicate LHS events in a side channel. */
    public static final OutputTag<TinyEvent> DUPLICATE_EVENTS_TAG = new OutputTag<TinyEvent>("duplicate-events"){};

    protected final String name;
    private final SerializablePredicate<TinyEvent> hasLeft;
    private final SerializableFunction<RHS, String> rightPKeyGetter;
    private final SerializableFunction<RHS, String> rightJoinIdGetter;
    private final SerializableToLongFunction<RHS> rightLogTimeGetter;
    @VisibleForTesting final SerializableTriFunction<TinyEvent.Builder, RHS, BiConsumer<OutputTag<MismatchError>, MismatchError>, List<TinyEvent.Builder>> join;
    private final boolean includeKeyAsScope;
    protected final Options options;

    // OutputTag to get dropped RHS events in a side channel.
    private OutputTag<RHS> droppedEventsTag;

    // Map used for LHS events keyed by primary key.
    @VisibleForTesting MapState<String, TinyEvent> idToLeft;
    // Map used for RHS events seen out of order keyed by LHS primary key.
    @VisibleForTesting MapState<String, List<RHS>> ooIdJoin;
    /* Map used to store LHS events by ancestor scopes.
     *
     * The key (String) space of the map are all UUID primary keys of different event types.  The
     * values are a tuple of LHS's log timestamp and LHS primary key in log timestamp order.
     *
     * TODO: more ideas to improve state management cost:
     * - use int128 uuids
     * - use bespoke int-map/tree structured on heirarchy
     * - use timestamp-based table (scope id, timer ts, list&lt;lhs&gt;)
     * - use index based map (need more details from dan)
     */
    @VisibleForTesting MapState<String, List<Tuple2<Long, String>>> inferenceScopes;
    /* Retain RHS elements that weren't able to join by primary key.
     * The value tuple is the timer timestamp and the RHS event.
     */
    @VisibleForTesting MapState<String, Tuple2<Long, RHS>> inferenceCandidates;

    /**
     * @param name Name of the operator, primarily used for flink state naming.
     * @param hasLeft Predicate to check if the LHS is populated in the TinyEvent (e.g. TinyEvent::hasUser).
     * @param rightPKeyGetter Function to extract the primary key (Foo.foo_id) from a RHS event.
     * @param rightJoinIdGetter Function to extract the immediate parent (LHS) id from a RHS event.
     * @param rightLogTimeGetter Function to extract the log timestamp from a RHS event.
     * @param join Function to produce joined (TinyEvent.Builder, RHS, MismatchIdsError) outputs.
     *             Can be configured to output multiple TinyEvent.Builders per join.
     * @param includeKeyAsScope Whether to include the key (usually ~log_user_id) in possible scopes.
     *                          For certain joins, this is an expensive scope and provides very little value.
     *                          This field lets us disable it.
     * @param options Runtime options.
     */
    protected BaseInferred(
            String name,
            SerializablePredicate<TinyEvent> hasLeft,
            SerializableFunction<RHS, String> rightPKeyGetter,
            SerializableFunction<RHS, String> rightJoinIdGetter,
            SerializableToLongFunction<RHS> rightLogTimeGetter,
            SerializableTriFunction<TinyEvent.Builder, RHS, BiConsumer<OutputTag<MismatchError>, MismatchError>, List<TinyEvent.Builder>> join,
            boolean includeKeyAsScope,
            Options options) {
        this.name = name;
        this.hasLeft = hasLeft;
        this.rightJoinIdGetter = rightJoinIdGetter;
        this.rightPKeyGetter = rightPKeyGetter;
        this.rightLogTimeGetter = rightLogTimeGetter;
        this.join = join;
        this.includeKeyAsScope = includeKeyAsScope;
        this.options = options;
    }

    /** Similar to the other constructor but supports singular output for rightSetter. */
    protected BaseInferred(
            String name,
            SerializablePredicate<TinyEvent> hasLeft,
            SerializableFunction<RHS, String> rightPKeyGetter,
            SerializableFunction<RHS, String> rightJoinIdGetter,
            SerializableTriFunction<TinyEvent.Builder, RHS, BiConsumer<OutputTag<MismatchError>, MismatchError>, TinyEvent.Builder> join,
            SerializableToLongFunction<RHS> rightLogTimeGetter,
            boolean includeKeyAsScope,
            Options options) {
        this(name,
                hasLeft,
                rightPKeyGetter,
                rightJoinIdGetter,
                rightLogTimeGetter,
                new ToSerializeTriListFunction(join),
                includeKeyAsScope,
                options);
    }

    /**
     * Returns join ids ordered: self, immediate parent, and then all ancestors until the view.
     *
     * Ordering is important because we rely on from most-to-least specific scopes to attempt to
     * infer a join.
     *
     * @see #getRightJoinIds(RHS)
     **/
    abstract ImmutableList<String> getLeftJoinIds(TinyEvent flat);

    /**
     * Returns join ids ordered: immediate parent, and then all ancestors until the view.
     *
     * Hint: it should be the identical list of ids returned by getJoinIds(TinyEvent).
     * @see #getLeftJoinIds(TinyEvent)
     */
    abstract ImmutableList<String> getRightJoinIds(RHS rhs);

    /**
     * Sets the RHS timing.log_timestamp to the given timestamp.
     * WARNING: If your RHS source is NOT from kafka, you will need to manage this log timestamp
     * accordingly on your own since it may mess up downstream inferred joins.
     */
    abstract RHS setLogTimestamp(RHS rhs, long timestamp);

    abstract TypeInformation<RHS> getRightTypeInfo();

    abstract boolean debugIdsMatch(RHS rhs);

    /** Returns OutputTag to get dropped RHS events in a side channel. */
    public final OutputTag<RHS> getDroppedEventsTag() {
        if (droppedEventsTag == null)
            droppedEventsTag = new OutputTag<RHS>("dropped-events", getRightTypeInfo()){};
        return droppedEventsTag;
    }

    public final SerializableToLongFunction<RHS> getRightLogTimeGetter() {
        return rightLogTimeGetter;
    }

    /** Returns the watermark delay necessary for inference. */
    public final long getWatermarkDelay() {
        return options.maxOutOfOrderMillis();
    }

    public final String getName() {
        return name;
    }

    public String dumpState() throws Exception {
        Joiner csv = Joiner.on(", ");
        StringBuilder sb = new StringBuilder();
        sb.append("idToLeft.keys:\n\t").append(csv.join(idToLeft.keys()));
        sb.append("\nooIdJoin.entries:\n");
        ooIdJoin.entries().forEach(entry -> sb.append("\t[").append(entry.getKey()).append("]->")
                .append(entry.getValue().stream().map(r -> rightPKeyGetter.apply(r)).collect(Collectors.joining(", "))));
        sb.append("\ninferenceScopes.entries:\n");
        inferenceScopes.entries().forEach(entry -> sb.append("\t[").append(entry.getKey()).append("]->").append(entry.getValue()));
        sb.append("\ninferenceCandidates.entries:\n");
        inferenceCandidates.entries().forEach(entry -> sb.append("\t[").append(entry.getKey()).append("]->").append(entry.getValue().f0));
        return sb.toString();
    }

    @Override
    public void open(Configuration config) throws Exception {
        super.open(config);
        idToLeft = getRuntimeContext().getMapState(
                // TODO(PRO-1683) - add caches back in.
                new MapStateDescriptor<>(name + " id", String.class, TinyEvent.class));
        ooIdJoin = getRuntimeContext().getMapState(
                // TODO(PRO-1683) - add caches back in.
                new MapStateDescriptor<>(name + " id out of order", Types.STRING, Types.LIST(getRightTypeInfo())));
        inferenceScopes = getRuntimeContext().getMapState(
                // TODO(PRO-1683) - add caches back in.
                new MapStateDescriptor<>(name + " inference scopes", Types.STRING, Types.LIST(Types.TUPLE(Types.LONG, Types.STRING))));
        inferenceCandidates = getRuntimeContext().getMapState(
                // TODO(PRO-1683) - add caches back in.
                new MapStateDescriptor<>(name + " inference candidates", Types.STRING, Types.TUPLE(Types.LONG, getRightTypeInfo())));
    }

    @Override
    public void processElement1(TinyEvent flat, Context ctx, Collector<TinyEvent> out) throws Exception {
        if (options.skipJoin()) return;
        if (options.debugIds().matches(flat))
            LOGGER.info("{} processElement1: {}\nwatermark: {}\nin: {}\nstate: {}",
                    name, ctx.timestamp(), ctx.timerService().currentWatermark(), flat, dumpState());

        if (!hasLeft.test(flat)) return;
        long timestamp = ctx.timestamp();
        long watermark = ctx.timerService().currentWatermark();
        if (isLate(timestamp, watermark)) {
            LOGGER.trace("{} processElement1.isLate: ts: {} watermark: {}\n{}", name, timestamp, watermark, flat);
            ctx.output(LATE_EVENTS_TAG, flat);
            return;
        }

        TinyEvent.Builder builder = flat.toBuilder();

        // The LHS timestamp should already be set in initial TinyEvent conversion and
        // in InferenceOperator for downstream joins, but this check and assignment is mainly to handle any that we miss.
        Preconditions.checkArgument(
            builder.getLogTimestamp() != 0,
                "%s LHS row is missing logTimestamp, flat=%s",
                name, flat);

        Iterator<String> joinIdItr = getScopeKeys(getLeftJoinIds(flat), ctx).iterator();

        // FIFO dedupe by primary join key and handle its joins.
        String primaryKey = joinIdItr.next();
        if (!StringUtil.isBlank(primaryKey)) {
            TinyEvent existing = idToLeft.get(primaryKey);
            if (existing != null) {
                ctx.output(DUPLICATE_EVENTS_TAG, flat);
                return;
            }
            idToLeft.put(primaryKey, builder.clone().build());

            List<RHS> outOfOrder = ooIdJoin.get(primaryKey);
            if (outOfOrder != null) {
                outOfOrder.forEach(rhs -> joinRight(builder.clone(), rhs, ctx).forEach(out::collect));
                ooIdJoin.remove(primaryKey);
            }
        }

        // Other ancestor keys define inference scopes.
        while (joinIdItr.hasNext()) {
            String scopeKey = joinIdItr.next();
            if (StringUtil.isBlank(scopeKey)) continue;

            // We need to ensure ordering by the log timestamp for later binary searches.
            Tuple2<Long, String> toAdd = Tuple2.of(builder.getLogTimestamp(), primaryKey);
            List<Tuple2<Long, String>> scope = inferenceScopes.get(scopeKey);
            if (scope == null) {
                scope = new ArrayList<>(1);
                scope.add(toAdd);
            } else {
                int i = Collections.binarySearch(scope, toAdd, (a, b) -> a.f0.compareTo(b.f0));
                if (i < 0) i = ~i;
                scope.add(i, toAdd);
            }
            if (matchesLongListThreshold(scope.size())) {
                LOGGER.warn("Long list in inferred {} inferenceScopes, size={}, scopeKey={}",
                    name, scope.size(), scopeKey);
            }
            inferenceScopes.put(scopeKey, scope);
        }

        // Trigger onTimer inference/cleanup for in order events (maxTime) in the future.
        // TODO - remove try-catch block after finding bad key issue.
        try {
            ctx.timerService().registerEventTimeTimer(ctx.timestamp() + options.maxTimeMillis());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    String.format("%s issue with registerEventTimeTimer for key=%s, flat=%s",
                            name, ctx.getCurrentKey(), flat),
                    e);
        }
    }

    @Override
    public void processElement2(RHS rhs, Context ctx, Collector<TinyEvent> out) throws Exception {
        if (debugIdsMatch(rhs))
            LOGGER.info("{} processElement2: {}\nwatermark: {}\nin: {}\nstate: {}",
                    name, ctx.timestamp(), ctx.timerService().currentWatermark(), rhs, dumpState());

        long timestamp = ctx.timestamp();
        long watermark = ctx.timerService().currentWatermark();
        if (isLate(timestamp, watermark)) {
            List<TinyEvent.Builder> builders = join.apply(TinyEvent.newBuilder(), rhs, ctx::output);
            builders.forEach(builder -> {
                TinyEvent event = builder.build();
                LOGGER.trace("{} processElement2.isLate: ts: {} watermark: {}\n{}", name, timestamp, watermark, event);
                ctx.output(LATE_EVENTS_TAG, event);
            });
            return;
        }

        // RHS records should have a logTimestamp set that matches the Kafka time.
        Preconditions.checkArgument(
                getRightLogTimeGetter().applyAsLong(rhs) != 0,
                "%s RHS row is missing logTimestamp, rhs=%s",
                name, rhs);

        if (options.skipJoin()) {
            joinRight(TinyEvent.newBuilder(), rhs, ctx).forEach(out::collect);
            return;
        }

        String foreignKey = rightJoinIdGetter.apply(rhs);
        TinyEvent flat = idToLeft.get(foreignKey);

        if (flat != null) {
            joinRight(flat.toBuilder(), rhs, ctx).forEach(out::collect);
        } else {
            // Trigger onTimer inference/cleanup for out of order events (maxOutOfOrder) in the "past".
            long timer = ctx.timestamp() + options.maxOutOfOrderMillis();

            // Out of order right side.
            if (!StringUtil.isBlank(foreignKey)) {
                // Would there be RHS w/ parent keys but no primary keys on LHS?  assuming no.
                List<RHS> lookup = ooIdJoin.get(foreignKey);
                if (lookup == null) lookup = new ArrayList<>();
                lookup.add(rhs);
                if (matchesLongListThreshold(lookup.size())) {
                    LOGGER.warn("Long list in inferred {} ooIdJoin, size={}, foreignKey={}",
                        name, lookup.size(), foreignKey);
                }
                ooIdJoin.put(foreignKey, lookup);
            } else {
                String rightKey = rightPKeyGetter.apply(rhs);
                // Only store rhs with primary key ids.
                if (!StringUtil.isBlank(rightKey)) {
                    // FIFO dedupe.  Not as critical here as later processing of this rhs event
                    // dedupes by primary key across the (typically) longer max in-order duration.
                    Tuple2<Long, RHS> lookup = inferenceCandidates.get(rightKey);
                    if (lookup == null) {
                        inferenceCandidates.put(rightKey, Tuple2.of(timer, rhs));
                    }
                }
            }

            // TODO - remove try-catch block after finding bad key issue.
            try {
                ctx.timerService().registerEventTimeTimer(timer);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        String.format("%s issue with registerEventTimeTimer for key=%s, rhs=%s",
                                name, ctx.getCurrentKey(), rhs),
                        e);
            }
        }
    }

    /**
     * Periodic emission of inferred joins and state cleanup.
     *
     * We're here because one of the relative timers fired after getting a watermark.  The
     * general idea is to visit all of our inferenceCandidates for this luid and see if any are
     * outside of the current window.  If they are any, do our best to infer the join for those
     * entities using the appropriate ancestor scope.  Then, remove them from our join state to stop
     * processing them.
     *
     * Because of the lateness checks in each processElement method, state should always be bound
     * within watermarks.
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<TinyEvent> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        processInferenceCandidates(timestamp, ctx, out);
        // PR - if idJoinDurationMultipler is not 1.0, I think this will leak state.
        cleanIdToLeft(timestamp - options.maxTimeMillis() * options.idJoinDurationMultiplier());
        cleanOoIdJoin(timestamp - options.maxOutOfOrderMillis() * options.idJoinDurationMultiplier(), ctx, out);
    }

    private void processInferenceCandidates(long timestamp, OnTimerContext ctx, Collector<TinyEvent> out) throws Exception {
        // Visit inference candidates and process them given current scopes.
        Iterator<Entry<String, Tuple2<Long, RHS>>> itr = inferenceCandidates.iterator();
        PriorityQueue<TinyEvent> toOutput = new PriorityQueue<>(TINY_FLAT_EVENT_LOG_TIMESTAMP_COMPARATOR);
        while (itr.hasNext()) {
            Entry<String, Tuple2<Long, RHS>> entry = itr.next();
            RHS rhs = entry.getValue().f1;
            boolean debugLogging = debugIdsMatch(rhs);
            if (debugLogging) {
                LOGGER.info("{} processInferenceCandidates started: {}\nrhs: {}\nstate: {}",
                        name, timestamp, rhs, dumpState());
            }

            if (entry.getValue().f0 <= timestamp) {
                List<TinyEvent> joined = ImmutableList.of();
                for (String scopeKey : getScopeKeys(getRightJoinIds(rhs), ctx)) {
                    joined = inferJoin(scopeKey, rhs, ctx);
                    if (!joined.isEmpty()) {
                        if (debugLogging) {
                            LOGGER.info("{} processInferenceCandidates: {}\njoined: {}\nrhs: {}\nscopeKey: {}\nscope: {}",
                                    name, timestamp, joined, rhs, scopeKey, getTinyEventsForScope(scopeKey));
                        }
                        break;
                    } else if (debugLogging) {
                        LOGGER.info("{} processInferenceCandidates not joined at scope: {}\nrhs: {}\nscopeKey: {}",
                                name, timestamp, scopeKey);
                    }
                }
                if (options.rightOuterJoin()) {
                    if (joined.isEmpty()) joined = joinRight(TinyEvent.newBuilder(), rhs, ctx);
                    toOutput.addAll(joined);
                } else {
                    if (joined.isEmpty()) ctx.output(getDroppedEventsTag(), rhs);
                    else toOutput.addAll(joined);
                }
                itr.remove();
            }
        }
        toOutput.forEach(out::collect);
    }

    /**
     * Joins using an ersatz key instead of the primary key.
     *
     * The general convention is to override this method in subclasses to filter the scope's
     * eligible LHS candidates using the ersatz key(s) and then finding the nearest valid LHS
     * event.
     *
     * Returns list of output events.  Empty if no joined events.
     */
    protected List<TinyEvent> inferJoin(String scopeKey, RHS rhs, Context ctx) throws Exception {
        return inferJoin(getTinyEventsForScope(scopeKey), rhs, ctx);
    }

    /**
     * Gets the sorted list of TinyEvents for a given inference scope (ancestor uuid).
     * Sorting is enforced on updates to inferenceScopes.
     */
    protected List<TinyEvent> getTinyEventsForScope(String scopeKey) throws Exception{
        // Perf optimization - don't do an inferenceScopes lookup for empty string.
        if (scopeKey.isEmpty()) return null;
        List<Tuple2<Long, String>> scopeIds = inferenceScopes.get(scopeKey);
        if (scopeIds == null) return null;
        ArrayList<TinyEvent> scope = new ArrayList<>(scopeIds.size());
        for (Tuple2<Long, String> tuple : scopeIds) {
            TinyEvent lhs = idToLeft.get(tuple.f1);
            if (lhs != null) scope.add(lhs);
            else {
                LOGGER.info("{} null LHS scope event key: {}", name, tuple);
            }
        }
        return scope;
    }

    private Iterable<String> getScopeKeys(Iterable<String> iterable, Context ctx) {
        if (includeKeyAsScope) {
            iterable = FluentIterable.from(iterable).append(ctx.getCurrentKey().f1);
        }
        return iterable;
    }

    /** Joins using the nearest LHS element in event time that respects the max time bounds.
     * Returns a list of TinyEvents.  Empty means no inferred TinyEvents.  Uses a List so
     * subclasses can return multiple TinyEvents per join.
     */
    protected List<TinyEvent> inferJoin(List<TinyEvent> scope, RHS rhs, Context ctx) {
        if (scope == null || scope.isEmpty()) return ImmutableList.of();
        boolean doLogging = options.debugIds().matches(rightPKeyGetter.apply(rhs));

        long rightTime = rightLogTimeGetter.applyAsLong(rhs);
        int index = Collections.binarySearch(
                scope,
                TinyEvent.newBuilder().setLogTimestamp(rightTime).build(),
                TINY_FLAT_EVENT_LOG_TIMESTAMP_COMPARATOR);
        if (doLogging)
            LOGGER.info("{} inferJoin binarySearch:\nrightTime: {}\nindex: {}", name, rightTime, index);

        // Find nearest, valid LHS.
        if (index < 0) {
            index = (-index) - 1; // index is now the first later lhs event.
            if (index > scope.size() - 1) {
                // index is the end, so point to the last element.
                index = scope.size() - 1;
            } else if (index > 0) {
                long earlierDiff = rightTime - scope.get(index - 1).getLogTimestamp();
                long laterDiff = scope.get(index).getLogTimestamp() - rightTime;
                boolean earlierValid = earlierDiff <= options.maxTimeMillis();
                boolean laterValid = laterDiff <= options.maxOutOfOrderMillis();
                // Set index to the temporally-closest, "valid" (within join window) element.
                if (!earlierValid && !laterValid) {
                    return ImmutableList.of();
                } else if (earlierValid && (!laterValid || earlierDiff < laterDiff)) {
                    index = index - 1;
                }
            }
        }

        TinyEvent nearest = scope.get(index);
        long leftTime = nearest.getLogTimestamp();
        if (doLogging)
            LOGGER.info("{} inferJoin nearest:\nindex: {}\nleftTime: {}\nnearest: {}",
                    name, index, leftTime, nearest);
        if (rightTime - options.maxTimeMillis() > leftTime || leftTime > rightTime + options.maxOutOfOrderMillis())
            return ImmutableList.of();
        return joinRight(nearest.toBuilder(), rhs, ctx);
    }

    /**
     * Always use this helper for out.collect() output to ensure the RHS's log timestamp is the log
     * timestamp for the downstream join which expects to join around the this operator's RHS.
     */
    protected ImmutableList<TinyEvent> joinRight(TinyEvent.Builder leftBuilder, RHS rhs, Context ctx) {
        List<TinyEvent.Builder> builders = join.apply(leftBuilder, rhs, ctx::output);
        return builders.stream().map(builder -> builder.build()).collect(ImmutableList.toImmutableList());
    }

    /** Generally doesn't affect us as long as our timestamps in flink are from kafka. */
    private boolean isLate(long timestamp, long watermark) {
        if (!options.checkLateness()) return false;
        return watermark != Long.MIN_VALUE && timestamp < watermark;
    }

    private void cleanIdToLeft(long keepMillis) throws Exception {
        Iterator<Entry<String, TinyEvent>> itr = idToLeft.iterator();
        HashSet<String> removed = new HashSet<>();
        while (itr.hasNext()) {
            Entry<String, TinyEvent> entry = itr.next();
            if (entry.getValue().getLogTimestamp() <= keepMillis) {
                removed.add(entry.getKey());
                itr.remove();
            }
        }
        cleanInferenceScopes(removed);
    }

    private void cleanInferenceScopes(HashSet<String> removed) throws Exception {
        Iterator<Entry<String, List<Tuple2<Long, String>>>> itr = inferenceScopes.iterator();
        while (itr.hasNext()) {
            Entry<String, List<Tuple2<Long, String>>> entry = itr.next();
            List<Tuple2<Long, String>> values = entry.getValue();
            values.removeIf(tuple -> removed.contains(tuple.f1));
            if (values.isEmpty()) itr.remove();
            else inferenceScopes.put(entry.getKey(), values);
        }
    }

    private void cleanOoIdJoin(long keepMillis, Context ctx, Collector<TinyEvent> out) throws Exception {
        Iterator<Entry<String, List<RHS>>> itr = ooIdJoin.iterator();
        while (itr.hasNext()) {
            Entry<String, List<RHS>> entry = itr.next();
            List<RHS> values = entry.getValue();
            List<RHS> newValues = new ArrayList<>();
            for (RHS rhs : values) {
                if (rightLogTimeGetter.applyAsLong(rhs) <= keepMillis) {
                    if (options.rightOuterJoin()) {
                        joinRight(TinyEvent.newBuilder(), rhs, ctx).forEach(out::collect);
                    } else {
                        ctx.output(getDroppedEventsTag(), rhs);
                    }
                } else {
                    newValues.add(rhs);
                }
            }
            if (newValues.isEmpty()) itr.remove();
            else ooIdJoin.put(entry.getKey(), newValues);
        }
    }

    @VisibleForTesting
    static boolean matchesLongListThreshold(int listSize) {
        return contains(LIST_LOG_THRESHOLDS, listSize)
            || (listSize % MAX_LIST_LONG_THRESHOLD == 0);
    }
    
    private static boolean contains(int[] matches, int value) {
        for (int match : matches) {
            if (value == match) {
                return true;
            } if (value < match) {
                // A short-cut.  Assumes matches is in order.
                return false;
            }
        }
        return false;
    }
}
