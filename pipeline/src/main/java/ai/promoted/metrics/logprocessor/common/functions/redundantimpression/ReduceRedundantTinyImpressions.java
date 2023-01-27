package ai.promoted.metrics.logprocessor.common.functions.redundantimpression;

import ai.promoted.proto.event.TinyEvent;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.Duration;

/**
 * Used to filter out redundant impressions.  Changes actions that reference redundant impressions
 * to an impression that this Function outputs.
 *
 * <p>The inputs need to be keyed by the grouping dimensions for reduction.
 *
 * <p>The {@code reducedImpression} is the earliest impression until an impression is encountered after
 * {@code replaceReducedImpressionAfter}.  The {@code reducedImpression} is kept for {@code 2 * ttl} in case a
 * redundant impression and action are encountered that would push the action outside of the {@code ttl}.
 * This is not optimal but keeps the logic simple.
 *
 * @param <K> The grouping dimension for reduction
 * IN = TinyEvent for both flat impressions and actions
 * OUT = TinyEvent for both flat impressions and actions
 */
public class ReduceRedundantTinyImpressions<K> extends KeyedProcessFunction<K, TinyEvent, TinyEvent> {
    private static final Logger LOGGER = LogManager.getLogger(ReduceRedundantTinyImpressions.class);
    // This is a temporary multiplier for the TTL.
    // TODO - delete this flag when we turn off process-time TTL.
    // Turning on/off enableTimeToLive is a breaking change.  Instead of turning it off now,
    // We'll set the TTL to be very large.
    private static final long TMP_TTL_MULTIPLIER = 1000;

    /** OutputTag for redundant impressions so we can track the issues over time. */
    public static final OutputTag<TinyEvent> REDUNDANT_IMPRESSION = new OutputTag<TinyEvent>("redundant-impression"){};

    @VisibleForTesting
    transient ValueState<TinyEvent> reducedImpression;

    /**
     * {@code reducedImpression} should be replaced after this timestamp (or if this is null).
     */
    @VisibleForTesting
    transient ValueState<Long> replaceReducedImpressionAfter;

    /**
     * Keeps track of out of order actions for the first impression.
     * Just keep in a {@code ListState}.  This should be small because we require an inner join between impressions and
     * actions.  If it grows large, we probably have a bug in the pipeline.
     */
    @VisibleForTesting
    transient ListState<TinyEvent> outOfOrderActions;

    /**
     * The latest cleanup timer.  When this timer is hit, clear out all state.
     */
    @VisibleForTesting
    transient ValueState<Long> latestCleanupTime;

    private final Duration ttl;

    public ReduceRedundantTinyImpressions(Duration ttl) {
        this.ttl = ttl;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters); // Doesn't do anything.

        // TODO - can I use the TTL for the keys?
        ValueStateDescriptor<TinyEvent> reducedImpressionDescriptor = new ValueStateDescriptor<>(
                "root-impression", TypeInformation.of(TinyEvent.class));
        // TODO - delete the enableTimeToLive code.
        // Dan thinks enabling/disabling enableTimeToLive is not a safe state change.
        // For now, just put a very large TTL to ignore it.
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.milliseconds(TMP_TTL_MULTIPLIER * ttl.toMillis()))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        reducedImpressionDescriptor.enableTimeToLive(ttlConfig);
        reducedImpression = getRuntimeContext().getState(reducedImpressionDescriptor);

        ValueStateDescriptor<Long> replaceReducedImpressionAfterDescriptor = new ValueStateDescriptor<>(
                "replace-reduced-impression-after", Types.LONG);
        replaceReducedImpressionAfterDescriptor.enableTimeToLive(ttlConfig);
        replaceReducedImpressionAfter = getRuntimeContext().getState(replaceReducedImpressionAfterDescriptor);

        ValueStateDescriptor<Long> latestCleanupTimeDescriptor = new ValueStateDescriptor<>(
                "latest-cleanup-time", Types.LONG);
        latestCleanupTime = getRuntimeContext().getState(latestCleanupTimeDescriptor);

        ListStateDescriptor<TinyEvent> outOfOrderActionsDescriptor = new ListStateDescriptor<>(
                "out-of-order-actions", TypeInformation.of(TinyEvent.class));
        // TTL is handled in our own onTimer code.  If we try to use Flink's ListState's TTL, we'll hit an issue
        // with a RocksDB thread not having access to our classloader.
        // https://stackoverflow.com/questions/60745711/flink-kryo-serializer-because-chill-serializer-couldnt-be-found
        outOfOrderActions = getRuntimeContext().getListState(outOfOrderActionsDescriptor);
    }

    // Impressions or Actions.
    @Override
    public void processElement(TinyEvent input, Context context, Collector<TinyEvent> collector) throws Exception {
        TinyEvent reducedImpression = this.reducedImpression.value();
        if (input.getActionId().isEmpty()) {
            // Impression.

            // First impression for this key.
            if (reducedImpression == null || isAfterReplacementTime(context.timestamp())) {
                this.reducedImpression.update(input);
                this.replaceReducedImpressionAfter.update(context.timestamp() + ttl.toMillis());
                collector.collect(input);
                processAnyOutOfOrderActions(input, collector);
                registerCleanupTimer(context, input.getLogTimestamp());
            } else {
                // Encountered redundant impression.
                // TODO - add a wrapper around the TinyEvent that contains the ID of reducedImpression.impressionId.
                context.output(REDUNDANT_IMPRESSION, input);
            }
            // Otherwise, we don't output the reduced Impression.
        } else {
            // Action.
            if (reducedImpression == null) {
                outOfOrderActions.add(input);
                registerCleanupTimer(context, input.getLogTimestamp());
            } else {
                collector.collect(withRootImpressionIds(input, reducedImpression));
            }
        }
    }

    /**
     * Register clean-up timer.  Might also update {@code latestCleanupTime} which is used to clean out the last state.
     */
    private void registerCleanupTimer(Context context, long timestamp) throws IOException {
        long cleanupTime = toCleanupTime(timestamp);
        Long latestCleanupTime =  this.latestCleanupTime.value();
        if (latestCleanupTime == null || cleanupTime > latestCleanupTime) {
            this.latestCleanupTime.update(cleanupTime);
        }
        context.timerService().registerEventTimeTimer(cleanupTime);
    }

    private void processAnyOutOfOrderActions(TinyEvent input, Collector<TinyEvent> collector) throws Exception {
        // We don't need to check outOfOrderActions if rootImpression is already set.
        Iterable<TinyEvent> oufOfOrderActionIterable = outOfOrderActions.get();
        if (oufOfOrderActionIterable.iterator().hasNext()) {
            // Delete timers to reduce timer load in Flink.
            oufOfOrderActionIterable.forEach(action -> {
                collector.collect(withRootImpressionIds(action, input));
            });
            outOfOrderActions.clear();
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<TinyEvent> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        // Process out-of-order actions.  This should not happen unless we have a joined-action without a joined-impression
        // (so a bug, edge condition or new feature development that misses this code).  This code outputs these
        // impression-less actions.
        //
        // Other considered short-term options:
        // 1. Kill the job.  Long-term, we do not want to blow up production.  It might make sense to have these explode in dev environments.
        //    TODO(PRO-1627) - introduce a flag setup that lets us log errors in production but explode in canary and dev.
        // 2. Drop the action.  This is similar to outputting since full details are not kept around very long.
        //
        // Longer-term, this logic will change:
        // A. If we use retractions.
        // B. If we change from inner joins to outer joins.
        ImmutableList.Builder<TinyEvent> newListBuilder = ImmutableList.builder();
        for (TinyEvent action : outOfOrderActions.get()) {
            if (timestamp >= toCleanupTime(action.getLogTimestamp())) {
                LOGGER.error("Encountered unexpected out-of-order tiny action that did not have a corresponding tiny impression, action={}", action);
                out.collect(action);
            } else {
                newListBuilder.add(action);
            }
        }
        ImmutableList<TinyEvent> newList = newListBuilder.build();
        outOfOrderActions.update(newList);

        // If onTimer hits the latestCleanupTime, then clear out the rest of the keyed state.
        if (timestamp == latestCleanupTime.value()) {
            // TODO - log if there are lists.
            if (!newList.isEmpty()) {
                LOGGER.error("This should never happen.  During final clean-up, we encountered existing outOfOrderActions without a clean-up timer.");
            }
            clearAllState();
        }
    }

    private void clearAllState() {
        reducedImpression.clear();
        replaceReducedImpressionAfter.clear();
        outOfOrderActions.clear();
        latestCleanupTime.clear();
    }

    private boolean isAfterReplacementTime(long timestamp) throws IOException {
        Long replacementTimestamp = replaceReducedImpressionAfter.value();
        return replacementTimestamp == null || timestamp > replacementTimestamp;
    }

    /**
     * This converts a logTimestamp to a clean-up that works ~okay in Flink event time.
     *
     * <p>This usually will usually create bugs (since that time might not match the event's Flink time).  In this case,
     * it's okay since the cleanup code is to handle a corner case which should not happen.
     * If it happens, we'll have bigger bugs.
     */
    private long toCleanupTime(long logTimestamp) {
        long ts = (logTimestamp + 2 * ttl.toMillis());
        return ts - ts % ttl.toMillis();
    }

    private TinyEvent withRootImpressionIds(TinyEvent action, TinyEvent impression) {
        if (action.getImpressionId().equals(impression.getImpressionId())) {
            // The good case where the action is already assigned to the root impression.
            return action;
        } else {
            return impression.toBuilder()
                    .setLogTimestamp(action.getLogTimestamp())
                    .setActionId(action.getActionId())
                    .build();
        }
    }
}
