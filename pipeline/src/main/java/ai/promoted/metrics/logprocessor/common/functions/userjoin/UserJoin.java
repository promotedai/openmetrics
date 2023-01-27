package ai.promoted.metrics.logprocessor.common.functions.userjoin;

import ai.promoted.metrics.error.MismatchError;
import ai.promoted.metrics.logprocessor.common.functions.SerializablePredicate;
import ai.promoted.metrics.logprocessor.common.functions.SerializableToLongFunction;
import ai.promoted.metrics.logprocessor.common.functions.SerializableTriFunction;
import ai.promoted.metrics.logprocessor.common.util.DebugIds;
import ai.promoted.proto.event.User;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Joins a stream of User updates with a stream of events.  Acts like a time lookup but is looser to so the join
 * can output earlier.  The streams should be keyed by Tuple2.of(platformId, logUserId).
 *
 * <p>Design decisions:</p>
 * <ul>
 *     <li>Make this specific in v1 to users and events.</li>
 *     <li>Support multiple user updates.</li>
 *     <li>
 *          If we have user records, do not delay events much. This lets us output the events at
 *          fastUpperBound (defaults zero seconds).  We'll still wait for the onTimer and watermark.
 *          We don't need to wait very long for out-of-order.  This does not handle out-of-order issues well.
 *          We'll wait for retractions to support this.
 *     </li>
 *     <li>
 *         Out-of-order events will be rare since we delay events throughout the system.  This v1 solution keeps the
 *         out-of-order event implementation simple by waiting until the full of out of order time duration to output.
 *     </li>
 *     <li>
 *         As long as there are User updates, we'll keep the merged User state around.  Example scenario: if we have
 *         full User log record from 1 year ago but we get some incremental update once per month, we'll keep a full
 *         merged User record around.
 *     </li>
 *     <li>
 *         Wait to hook up User DB for a larger project.
 *     </li>
 *     <li>
 *         Don't worry about reusing core Flink time lookup logic.  Might make more sense when using a CDC stream from
 *         User DB.
 *     </li>
 * </ul>
 *
 * <p>This started as a fork of Flink's internal IntervalJoinOperator but is now very different.</p>
 */
public class UserJoin<E extends GeneratedMessageV3> extends KeyedCoProcessFunction<Tuple2<Long, String>, User, E, E> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LogManager.getLogger(UserJoin.class);

    // A multiplier on the clean-up delay for User Update records.  This is used to avoid edge boundary conditions.
    private static final int CLEAN_UP_MULTIPLIER = 2;

    // Usually a large negative range.  E.g. -30D.  If we have not received any User record in this time period, we'll
    // clear the User record.
    private final Duration maxTime;
    // This is a timer day when we have any existing user record. Usually 0S but can be overriden via a flag.
    private final Duration fastMaxOutOfOrder;
    // This is the max out of order time.
    private final Duration maxOutOfOrder;
    private final Class<E> eventClass;
    // Used to merge the event and User messages together.
    private final SerializableTriFunction<E, User, BiConsumer<OutputTag<MismatchError>, MismatchError>, E> copyAndSetUser;
    private final SerializableToLongFunction<E> getLogTimestamp;
    private final DebugIds debugIds;
    private final SerializablePredicate<E> matchesDebugId;
    private final boolean runAsserts;

    // Unordered list of user operations.  onTimer will sort it by logTimestamp.
    @VisibleForTesting
    ListState<User> users;
    // Events are first put into this map.  When the fastUpperBound is hit, if there are no effective user records,
    // we wait until the hardUpperBound is hit.
    @VisibleForTesting
    MapState<Long, List<E>> fastEvents;
    // This will most likely be empty.
    @VisibleForTesting
    MapState<Long, List<E>> untilEndOfWindowEvents;

    public UserJoin(
            Duration maxTime,
            Duration fastMaxOutOfOrder,
            Duration maxOutOfOrder,
            Class<E> eventClass,
            SerializableTriFunction<E, User, BiConsumer<OutputTag<MismatchError>, MismatchError>, E> copyAndSetUser,
            SerializableToLongFunction<E> getLogTimestamp,
            DebugIds debugIds,
            SerializablePredicate<E> matchesDebugId,
            boolean runAsserts) {
        Preconditions.checkArgument(maxTime.isNegative(), "maxTime should be negative");
        Preconditions.checkArgument(!maxOutOfOrder.isNegative(), "maxOutOfOrder should not be negative");
        Preconditions.checkArgument(
                maxTime.compareTo(fastMaxOutOfOrder) <= 0, "maxTime should be <= fastUpperBound.");
        Preconditions.checkArgument(
                fastMaxOutOfOrder.compareTo(maxOutOfOrder) <= 0, "maxTime <= hardUpperBound must be fulfilled");
        this.maxTime = maxTime;
        this.fastMaxOutOfOrder = fastMaxOutOfOrder;
        this.maxOutOfOrder = maxOutOfOrder;
        this.copyAndSetUser = copyAndSetUser;
        this.eventClass = eventClass;
        this.getLogTimestamp = getLogTimestamp;
        this.debugIds = debugIds;
        this.matchesDebugId = matchesDebugId;
        this.runAsserts = runAsserts;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // TTL is handled in our own onTimer code.  If we try to use Flink's ListState's TTL, we'll hit an issue
        // with a RocksDB thread not having access to our classloader.
        // https://stackoverflow.com/questions/60745711/flink-kryo-serializer-because-chill-serializer-couldnt-be-found
        users = getRuntimeContext().getListState(new ListStateDescriptor<>("users", User.class));
        fastEvents = getRuntimeContext().getMapState(new MapStateDescriptor<>(
                "fast-events", Types.LONG, Types.LIST(TypeInformation.of(eventClass))));
        untilEndOfWindowEvents = getRuntimeContext().getMapState(new MapStateDescriptor<>(
                "until-end-of-window-events", Types.LONG, Types.LIST(TypeInformation.of(eventClass))));
    }

    @Override
    public void processElement1(User user, Context ctx, Collector<E> out) throws Exception {
        if (debugIds.matches(user)) {
            LOGGER.info("debug IDs processElement1, event={}", user);
        }
        // Treat user fields as field updates.  Append the state and deal with it on timer.
        // TODO - there might be a way to optimize the state here.
        users.add(user);
        // Register a clean-up timers.
        // The first one is a quicker timer that ca be used to consolidate state in case we get a fast stream only User
        // updates.  This shouldn't really happen.
        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + maxOutOfOrder.toMillis());
        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + CLEAN_UP_MULTIPLIER * maxOutOfOrder.toMillis());
        // Since maxTime is negative, subtract it.
        ctx.timerService().registerEventTimeTimer(ctx.timestamp() - maxTime.toMillis());
    }

    @Override
    public void processElement2(E event, Context ctx, Collector<E> out) throws Exception {
        if (matchesDebugId.test(event)) {
            LOGGER.info("debug IDs processElement2, event={}", event);
        }
        long timerTime = ctx.timestamp() + fastMaxOutOfOrder.toMillis();
        addToBuffer(fastEvents, event, timerTime);
        ctx.timerService().registerEventTimeTimer(timerTime);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<E> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        List<User> userUpdates = getSortedUsers();
        boolean debugLogging = userUpdates.stream().anyMatch(debugIds::matches);
        processFastEvents(userUpdates, timestamp, ctx, out, debugLogging);
        processUntilEndOfWindowEvents(userUpdates, timestamp, ctx, out, debugLogging);
        processRemainingUsers(userUpdates, timestamp, debugLogging);
    }

    private void processFastEvents(List<User> userUpdates, long timestamp, OnTimerContext ctx, Collector<E> out, boolean debugLogging) throws Exception {
        List<E> events = fastEvents.get(timestamp);
        if (events == null) {
            return;
        }
        events = getSortedCopy(events);
        UserUpdateMerger userMerger = new UserUpdateMerger(userUpdates, runAsserts);
        Set<Long> newTimers = new HashSet<>();
        for (E event : events) {
            userMerger.advanceTo(getLogTimestamp.applyAsLong(event));
            User user = userMerger.getEffectiveUser();

            if (user != null) {
                E joinedEvent = copyAndSetUser.apply(event, user, ctx::output);
                if (debugLogging || matchesDebugId.test(joinedEvent)) {
                    LOGGER.info("debug IDs processFastEvents matched, joinedEvent={}, userUpdates={}", joinedEvent, userUpdates);
                }
                out.collect(joinedEvent);
            } else {
                if (debugLogging || matchesDebugId.test(event)) {
                    LOGGER.info("debug IDs processFastEvents did not match, event={}, userUpdates={}", event, userUpdates);
                }
                long timerTime = timestamp + maxOutOfOrder.toMillis();
                addToBuffer(untilEndOfWindowEvents, event, timerTime);
                newTimers.add(timerTime);
            }
        }
        newTimers.forEach(newTimer -> ctx.timerService().registerEventTimeTimer(newTimer));
        fastEvents.remove(timestamp);
    }

    private void processUntilEndOfWindowEvents(List<User> userUpdates, long timestamp, OnTimerContext ctx, Collector<E> out, boolean debugLogging) throws Exception {
        List<E> events = untilEndOfWindowEvents.get(timestamp);
        if (events == null) {
            return;
        }
        events = getSortedCopy(events);
        UserUpdateMerger userMerger = new UserUpdateMerger(userUpdates, runAsserts);
        for (E event : events) {
            // For the hard output, we are okay advancing beyond the current event.
            // This is a case where we might have out of order logging.
            userMerger.advanceTo(getLogTimestamp.applyAsLong(event) + maxOutOfOrder.toMillis());
            User user = userMerger.getEffectiveUser();
            if (user != null) {
                E joinedEvent = copyAndSetUser.apply(event, user, ctx::output);
                if (debugLogging || matchesDebugId.test(joinedEvent)) {
                    LOGGER.info("debug IDs processUntilEndOfWindowEvents matched, event={}, userUpdates={}", joinedEvent, userUpdates);
                }
                out.collect(joinedEvent);
            } else {
                if (debugLogging || matchesDebugId.test(event)) {
                    LOGGER.info("debug IDs processUntilEndOfWindowEvents did not match, event={}, userUpdates={}", event, userUpdates);
                }
                // Drop the record.
            }
        }
        untilEndOfWindowEvents.remove(timestamp);
    }

    private void processRemainingUsers(List<User> userUpdates, long timerTimestamp, boolean debugLogging) throws Exception {
        UserUpdateMerger userMerger = new UserUpdateMerger(userUpdates, runAsserts);

        // Advance to the lower end of the range.  Reminder that maxTime is negative.
        userMerger.advanceTo(Instant.ofEpochMilli(timerTimestamp).plus(maxTime).toEpochMilli());

        if (userMerger.getRemainingUserUpdatesCount() == 0) {
            // We don't want to keep the User state around if there has not been any User records in maxTime.
            this.users.clear();
            if (debugLogging) {
                LOGGER.info("debug IDs processRemainingUsers clear users, userUpdates={}", userUpdates);
            }
            return;
        }

        // We can consolidate older User records together.
        // processUntilEndOfWindowEvents calls UserUpdateMerger with an advanceTo time that is maxOutOfOrder ahead.
        // We need to keep at least that duration of records.  To avoid edge boundary issues with infrequent User updates,
        // we'll multiply the clean-up duration by the multiplier.
        userMerger.advanceTo(Instant.ofEpochMilli(timerTimestamp).minus(maxOutOfOrder.multipliedBy(CLEAN_UP_MULTIPLIER)).toEpochMilli());
        List<User> remainingUsers = userMerger.getRemainingUserUpdates();
        List<User> result;
        if (userMerger.getEffectiveUser() != null) {
            result = ImmutableList.<User>builderWithExpectedSize(1 + remainingUsers.size())
                    .add(userMerger.getEffectiveUser())
                    .addAll(remainingUsers)
                    .build();
        } else {
            result = remainingUsers;
        }
        Preconditions.checkState(!result.isEmpty(), "UserBuilder should have a User update that we should keep.");
        if (userUpdates.size() != result.size()) {
            // Small optimization - since user is not updated in onTimer, if the userUpdates and result sizes are the
            // same, then the user state does not need updating.
            if (debugLogging) {
                LOGGER.info("debug IDs processRemainingUsers update users state, oldUserUpdates={}, newUserUpdates={}", userUpdates, result);
            }
            this.users.update(result);
        } else if (debugLogging) {
            LOGGER.info("debug IDs processRemainingUsers did not update users state, userUpdates={}", userUpdates);
        }
    }

    /** Gets sorted list by logTimestamp. */
    private List<User> getSortedUsers() throws Exception {
        Iterable<User> userUpdatesIterable = users.get();
        if (userUpdatesIterable == null) {
            return ImmutableList.of();
        }
        return ImmutableList.copyOf(users.get())
                .stream()
                .sorted(Comparator.comparingLong(e -> e.getTiming().getLogTimestamp()))
                .collect(Collectors.toList());
    }

    private List<E> getSortedCopy(List<E> events) {
        return events.stream()
                .sorted(Comparator.comparingLong(getLogTimestamp))
                .collect(Collectors.toList());
    }

    private static <T> void addToBuffer(
            final MapState<Long, List<T>> buffer,
            final T entry,
            final long timerTime)
            throws Exception {
        List<T> elemsInBucket = buffer.get(timerTime);
        if (elemsInBucket == null) {
            elemsInBucket = new ArrayList<>();
        }
        elemsInBucket.add(entry);
        buffer.put(timerTime, elemsInBucket);
    }
}
