package ai.promoted.metrics.logprocessor.common.functions;

import ai.promoted.metrics.logprocessor.common.constant.Constants;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.JoinedEvent;
import ai.promoted.proto.event.JoinedIdentifiers;
import ai.promoted.proto.event.LatestImpression;
import ai.promoted.proto.event.LatestImpressions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * A function that coprocesses {@code FlatActions} and {@code FlatImpressions} to join a list of the
 * latest impressions onto the flat actions. This list of impressions is used by AWS Personalize as
 * a negative signal. This function filters out contentIds with positive behavior (action events).
 *
 * <p>The logic is pretty complex: 1. buffers actions and impressions temporarily. 2. uses timers to
 * delay output the action events with the {@code LatestImpressions} 3. removes contentId that (a)
 * has actions on it and (b) duplicated.
 *
 * <p>K = log_user_id. IN1 = *JoinedEvent for joined-action IN2 = *JoinedEvent for joined-impression
 * OUT = *JoinedEvent for joined-action with recent impressions
 */
public class AddLatestImpressions
    extends KeyedCoProcessFunction<Tuple2<Long, String>, JoinedEvent, JoinedEvent, JoinedEvent> {
  private static final long MILLIS_PER_MINUTE = TimeUnit.MINUTES.toMillis(1);
  private static final long MILLIS_PER_HOUR = TimeUnit.HOURS.toMillis(1);
  // Timer bucket for pruning impressions so we can keep the state small.
  private static final long PRUNE_IMPRESSIONS_CLEANUP_MINUTES = 1;
  private static final long SESSION_DURATION_MILLIS =
      Constants.SESSION_DURATION_HOURS * MILLIS_PER_HOUR;
  // Timer bucket for doing larger session cleanup.
  // We pad SESSION_DURATION_HOURS to handle for floor logic with bucketing.
  private static final long SESSION_CLEANUP_HOURS = Constants.SESSION_DURATION_HOURS + 1;

  // TODO - see if I can remove this.

  // Accessor cache
  // 18 = action, 1 = common, 5 = event_api_timestamp.
  private static Comparator<LatestImpression> LATEST_IMPRESSION_TIMESTAMP_COMPARATOR =
      Comparator.comparingLong(AddLatestImpressions::getLatestImpressionTimestamp);

  // Stores Actions temporarily so we can handle late events.
  @VisibleForTesting transient ListState<JoinedEvent> actions;
  // Stores Impressions so we can join them into Actions.
  @VisibleForTesting transient ListState<LatestImpression> impressions;
  // Stores a smaller version of Actions so we can exclude positive content events from the joined
  // output.
  // Overloads LatestImpression.  Timestamp is the action timestamp.
  @VisibleForTesting transient ListState<LatestImpression> actionedImpressions;

  // This is an extra maxOutOfOrderness in case we want to experiment with catching more data.
  // This should normally be zero.
  // This sets the event timer to (event timer + lateDurationMillis).  This can be used if
  // we want to be paranoid about out of order events.
  private final Duration maxOutOfOrderness;
  // Max number of Impressions to output on FlatActions.
  // In production, we output 25 impressions (the max for AWS Personalize).
  // The main use for the function is to join in a list of impressions for AWS Personalize.
  private final int maxOutputImpressions;
  // Max number of Impressions to keep in our state.  We want extra Impressions so we have backups
  // in case
  // the user actions on a bunch of recent impressions.  We default to +5 impressions.
  private final int maxStateImpressions;
  private final boolean runAsserts;

  public AddLatestImpressions(Duration maxOutOfOrderness, boolean runAsserts) {
    // This still needs to be optimized.
    // maxOutputImpressions = max AWS Personalize impression count.
    // maxStateImpressions = factoring in buffer in case the user actions a lot of items they've
    // seen.
    this(maxOutOfOrderness, 25, 30, runAsserts);
  }

  public AddLatestImpressions(
      Duration maxOutOfOrderness,
      int maxOutputImpressions,
      int maxStateImpressions,
      boolean runAsserts) {
    this.maxOutOfOrderness = maxOutOfOrderness;
    this.maxOutputImpressions = maxOutputImpressions;
    this.maxStateImpressions = maxStateImpressions;
    this.runAsserts = runAsserts;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters); // Doesn't do anything.

    ListStateDescriptor<JoinedEvent> actionsDescriptor =
        new ListStateDescriptor<JoinedEvent>("actions", TypeInformation.of(JoinedEvent.class));
    actions = getRuntimeContext().getListState(actionsDescriptor);

    ListStateDescriptor<LatestImpression> impressionsDescriptor =
        new ListStateDescriptor<>("impressions", TypeInformation.of(LatestImpression.class));
    impressions = getRuntimeContext().getListState(impressionsDescriptor);

    ListStateDescriptor<LatestImpression> actionedImpressionsDescriptor =
        new ListStateDescriptor<>(
            "actionedImpressions", TypeInformation.of(LatestImpression.class));
    actionedImpressions = getRuntimeContext().getListState(actionedImpressionsDescriptor);
  }

  private static long nullToZero(Long value) {
    return value != null ? value : 0;
  }

  /**
   * Gets a timestamp to use for the action.
   *
   * <p>We slightly prefer clientLogTimestamp over eventApiTimestamp for the timestamp. Both have
   * issues: 1. clientLogTimestamp - relative ordering is probably more accurate. Harder to trust
   * and it won't always be set (which is why I fall back to event api ts). 2. eventApiTimestamp -
   * we can trust more but delays on the client side can cause weird smaller order issues.
   *
   * <p>If we eventually want both, we could add another field to LatestImpression to have a
   * different timestamp field. We'd still want to pick one and order by it.
   */
  @VisibleForTesting
  static long getActionTimestamp(JoinedEvent joinedAction) {
    Action action = joinedAction.getAction();
    long timestamp = action.getTiming().getClientLogTimestamp();
    if (timestamp == 0) {
      // 18 = action, 1 = impression, 5 = event_api_timestamp.
      timestamp = action.getTiming().getEventApiTimestamp();
    }
    // TODO - log bad timestamp when we have a rate-limit text logger.
    return timestamp;
  }

  /** See {@code getActionTimestamp for explanation of the logic}. */
  @VisibleForTesting
  static long getImpressionTimestamp(JoinedEvent joinedImpression) {
    Impression impression = joinedImpression.getImpression();
    long timestamp = impression.getTiming().getClientLogTimestamp();
    if (timestamp == 0) {
      timestamp = impression.getTiming().getEventApiTimestamp();
    }
    // TODO - log bad timestamp when we have a rate-limit text logger.
    return timestamp;
  }

  /** See {@code getActionTimestamp for explanation of the logic}. */
  @VisibleForTesting
  static long getLatestImpressionTimestamp(LatestImpression latestImpression) {
    long timestamp = latestImpression.getClientLogTimestamp();
    if (timestamp == 0) {
      timestamp = latestImpression.getEventApiTimestamp();
    }
    // TODO - log bad timestamp when we have a rate-limit text logger.
    return timestamp;
  }

  // Actions.
  @Override
  public void processElement1(
      JoinedEvent joinedAction, Context context, Collector<JoinedEvent> collector)
      throws Exception {
    // Add actions to the state.  Most of the work is done in the timer after waiting for more
    // events.
    actions.add(joinedAction);

    // Track the latest impressions that are actioned on.  We'll use this list to exclude
    // impressions later.
    long timestamp = context.timestamp();
    Action action = joinedAction.getAction();
    JoinedIdentifiers ids = joinedAction.getIds();
    actionedImpressions.add(
        LatestImpression.newBuilder()
            .setImpressionId(ids.getImpressionId())
            .setContentId(joinedAction.getResponseInsertion().getContentId())
            .setClientLogTimestamp(action.getTiming().getClientLogTimestamp())
            .setEventApiTimestamp(action.getTiming().getEventApiTimestamp())
            .setEventTimestamp(timestamp)
            .build());

    // The timer logic uses the event timer to make sure we satisfy the watermark threshold.

    // The action stream for a user is usually small.  We'll just run a timer for it.
    long outputTimerTimestamp = timestamp + maxOutOfOrderness.toMillis();
    context.timerService().registerEventTimeTimer(outputTimerTimestamp);

    // Timer for ~session pruning.  Bucket to reduce number of timers.
    long sessionCleanupTimestamp =
        ((timestamp / MILLIS_PER_HOUR) + SESSION_CLEANUP_HOURS) * MILLIS_PER_HOUR;
    context.timerService().registerEventTimeTimer(sessionCleanupTimestamp);
  }

  // Impressions.
  @Override
  public void processElement2(
      JoinedEvent joinedImpression, Context context, Collector<JoinedEvent> collector)
      throws Exception {
    // Convert these to LatestImpression (smaller version).
    long timestamp = context.timestamp();
    Impression impression = joinedImpression.getImpression();
    JoinedIdentifiers ids = joinedImpression.getIds();
    impressions.add(
        LatestImpression.newBuilder()
            .setImpressionId(ids.getImpressionId())
            .setContentId(joinedImpression.getResponseInsertion().getContentId())
            .setClientLogTimestamp(impression.getTiming().getClientLogTimestamp())
            .setEventApiTimestamp(impression.getTiming().getEventApiTimestamp())
            .setEventTimestamp(timestamp)
            .build());

    // The timer logic uses the event timer to make sure we satisfy the watermark threshold.

    // Timer for smaller cleanup.  Bucket to reduce number of timers.
    long smallCleanupTimestamp =
        ((timestamp / MILLIS_PER_MINUTE) + PRUNE_IMPRESSIONS_CLEANUP_MINUTES) * MILLIS_PER_MINUTE;
    context.timerService().registerEventTimeTimer(smallCleanupTimestamp);

    // Timer for ~session pruning.  Bucket to reduce number of timers.
    long sessionCleanupTimestamp =
        ((timestamp / MILLIS_PER_HOUR) + SESSION_CLEANUP_HOURS) * MILLIS_PER_HOUR;
    context.timerService().registerEventTimeTimer(sessionCleanupTimestamp);
  }

  private List<LatestImpression> prune(List<LatestImpression> sortedImpressions, long timestamp) {
    final long beforeSessionTimestamp = timestamp - SESSION_DURATION_MILLIS;
    final long startLateWatermarkTimestamp = timestamp - maxOutOfOrderness.toMillis();
    Set<String> latestContentIds = Sets.newHashSetWithExpectedSize(maxStateImpressions);
    return copyAndFilterInReverseOrder(
        sortedImpressions,
        (impression) -> {
          if (getLatestImpressionTimestamp(impression) < beforeSessionTimestamp) {
            return FilterResult.REMOVE;
          }
          if (startLateWatermarkTimestamp < impression.getEventTimestamp()) {
            return FilterResult.KEEP_DO_NOT_COUNT;
          }
          if (latestContentIds.add(impression.getContentId())) {
            return FilterResult.KEEP_AND_COUNT;
          } else {
            return FilterResult.REMOVE;
          }
        },
        maxStateImpressions);
  }

  /** Used to support filtering cases where we have a max count on a subset of items. */
  enum FilterResult {
    REMOVE,
    KEEP_DO_NOT_COUNT, // Include in filter but don't include in the count against max limits.
    KEEP_AND_COUNT,
  };

  private static boolean isSorted(List<LatestImpression> sortedImpressions, int index) {
    if (index < 2) {
      return true;
    } else if (LATEST_IMPRESSION_TIMESTAMP_COMPARATOR.compare(
            sortedImpressions.get(index - 2), sortedImpressions.get(index - 1))
        > 0) {
      return false;
    } else {
      return isSorted(sortedImpressions, index - 1);
    }
  }

  /**
   * Copies {@param sortedImpressions} and filters in reverse order. {@code FilterResult} is used to
   * support including items that do not count towards {@code maxKeepSize}.
   */
  private List<LatestImpression> copyAndFilterInReverseOrder(
      List<LatestImpression> sortedImpressions,
      Function<LatestImpression, FilterResult> filter,
      int maxKeepSize) {
    if (runAsserts) {
      if (!isSorted(sortedImpressions, sortedImpressions.size())) {
        throw new IllegalArgumentException("sortedImpression is not actually sorted.");
      }
    }

    // We want to remove older content.  It's easier to do this by iterating over the list the
    // opposite way and
    // then flip it.
    List<LatestImpression> reversedCopy = Lists.newArrayListWithCapacity(sortedImpressions.size());
    int keepSize = 0;
    for (int i = sortedImpressions.size() - 1; i >= 0; i--) {
      LatestImpression impression = sortedImpressions.get(i);
      FilterResult filterResult = filter.apply(impression);
      if (filterResult == FilterResult.KEEP_DO_NOT_COUNT) {
        reversedCopy.add(impression);
      } else if (filterResult == FilterResult.KEEP_AND_COUNT && keepSize < maxKeepSize) {
        reversedCopy.add(impression);
        keepSize++;
      }
    }
    Collections.reverse(reversedCopy);
    return reversedCopy;
  }

  // Small helper method.
  private static <T> Stream<T> toStream(ListState<T> listState) throws Exception {
    return StreamSupport.stream(listState.get().spliterator(), false);
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<JoinedEvent> actionOut)
      throws Exception {
    super.onTimer(timestamp, ctx, actionOut);

    // For now, we'll just sort the impression list each timer.  This uses TimSort so it should be
    // pretty
    // efficient.
    List<LatestImpression> sortedImpressions =
        toStream(impressions)
            .sorted(LATEST_IMPRESSION_TIMESTAMP_COMPARATOR)
            .collect(Collectors.toList());

    List<JoinedEvent> keepActions = new ArrayList<>();
    for (JoinedEvent joinedAction : actions.get()) {

      // TODO - this should use the server-side event timer (not Event API Timestamp).
      // We're not using the server-side time because (1) it's not available on the Action proto and
      // (2)
      // I don't want to wrap the state in another message.
      // There is a small difference between the two (usually a small 10ms of delay).
      // Setting maxOutOfOrderness high enough reduces the chance of this being an issue.
      final long actionFlinkTimestamp = joinedAction.getAction().getTiming().getEventApiTimestamp();

      if (actionFlinkTimestamp <= timestamp - maxOutOfOrderness.toMillis()) {

        // Output action.
        final long actionTimestamp = getActionTimestamp(joinedAction);

        // Seed seenContentIds so we can exclude items that have been actioned on.
        Set<String> seenContentIds =
            toStream(actionedImpressions)
                .filter(
                    actionImpression ->
                        getLatestImpressionTimestamp(actionImpression) <= actionTimestamp)
                .map(LatestImpression::getContentId)
                .collect(Collectors.toSet());

        List<LatestImpression> actionImpressions =
            copyAndFilterInReverseOrder(
                sortedImpressions,
                (impression) -> {
                  // We don't need a lateDurationMillis check here since we already checked that the
                  // action is
                  // safely out of the late time period.
                  if (getLatestImpressionTimestamp(impression) <= actionTimestamp
                      && seenContentIds.add(impression.getContentId())) {
                    return FilterResult.KEEP_AND_COUNT;
                  }
                  return FilterResult.REMOVE;
                },
                maxOutputImpressions);

        // We'll output the results in order.  AWS Personalize does not care.
        LatestImpressions.Builder latestImpressionBuilder = LatestImpressions.newBuilder();
        latestImpressionBuilder.addAllImpression(actionImpressions);
        actionOut.collect(
            joinedAction.toBuilder().setLatestImpressions(latestImpressionBuilder).build());
      } else {
        keepActions.add(joinedAction);
      }
    }
    actions.update(keepActions);
    impressions.update(prune(sortedImpressions, timestamp));
    actionedImpressions.update(prune(ImmutableList.copyOf(actionedImpressions.get()), timestamp));
  }
}
