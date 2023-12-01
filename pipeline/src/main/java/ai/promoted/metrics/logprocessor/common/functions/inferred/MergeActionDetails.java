package ai.promoted.metrics.logprocessor.common.functions.inferred;

import ai.promoted.metrics.error.MismatchError;
import ai.promoted.metrics.logprocessor.common.util.DebugIds;
import ai.promoted.metrics.logprocessor.common.util.TrackingUtil;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.AttributedAction;
import ai.promoted.proto.event.Cart;
import ai.promoted.proto.event.CartContent;
import ai.promoted.proto.event.DroppedMergeActionDetails;
import ai.promoted.proto.event.JoinedImpression;
import ai.promoted.proto.event.TinyAttributedAction;
import ai.promoted.proto.event.TinyInsertion;
import ai.promoted.proto.event.Touchpoint;
import ai.promoted.proto.event.UnionEvent;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

// TODO - move it it's own package in a different PR.

/**
 * Takes tiny Actions (from Inferred References) and fills in details using the stream of
 * UnionEvent.
 *
 * <p>TODO - maybe switch to regular interval join. Research data lake first. MergeImpressionDetails
 * is still useful because we can consolidate DeliveryLog details into less state.
 */
public class MergeActionDetails
    extends AbstractMergeDetails<TinyAttributedAction, AttributedAction, AttributedAction.Builder> {
  public static final OutputTag<DroppedMergeActionDetails> DROPPED_TAG =
      new OutputTag<>("dropped") {};

  @VisibleForTesting final JoinedImpressionMerger joinedImpressionMerger;
  @VisibleForTesting final ActionMerger actionMerger;
  private final DebugIds debugIds;

  public MergeActionDetails(
      Duration joinedImpressionWindow,
      Duration actionWindow,
      Duration batchCleanupWindow,
      Duration missingEntityDelay,
      Long allTimersBeforeCauseBatchCleanup,
      DebugIds debugIds) {
    super(
        batchCleanupWindow,
        missingEntityDelay,
        allTimersBeforeCauseBatchCleanup,
        debugIds,
        TinyAttributedAction.class);
    this.joinedImpressionMerger = new JoinedImpressionMerger(joinedImpressionWindow, debugIds);
    this.actionMerger = new ActionMerger(actionWindow, debugIds);
    this.debugIds = debugIds;
  }

  @Override
  public void open(Configuration config) throws Exception {
    super.open(config);
    joinedImpressionMerger.open(getRuntimeContext());
    actionMerger.open(getRuntimeContext());
  }

  @Override
  public void processElement1(UnionEvent input, Context ctx, Collector<AttributedAction> out)
      throws Exception {
    // TODO - optimize the full records being stored in maps.  We can strip out fields we'll
    // eventually clear.
    switch (input.getEventCase()) {
      case JOINED_IMPRESSION:
        joinedImpressionMerger.processElement1(input, ctx, out);
        break;
      case ACTION:
        actionMerger.processElement1(input, ctx, out);
        break;
      default:
        throw new UnsupportedOperationException("Unsupported UnionEvent=" + input);
    }
  }

  @Override
  protected void addIncompleteTimers(
      TinyAttributedAction rhs, Context ctx, EnumSet<MissingEvent> missing, long timerTimestamp)
      throws Exception {
    for (MissingEvent missingEvent : missing) {
      switch (missingEvent) {
        case JOINED_IMPRESSION:
          addIncompleteTimers(
              joinedImpressionMerger.impressionIdToIncompleteEventTimers,
              rhs.getTouchpoint().getJoinedImpression().getImpression().getImpressionId(),
              timerTimestamp);
          break;
        case ACTION:
          addIncompleteTimers(
              actionMerger.actionIdToIncompleteEventTimers,
              rhs.getAction().getActionId(),
              timerTimestamp);
          break;
        default:
          throw new UnsupportedOperationException("Unsupported MissingEntity=" + missingEvent);
      }
    }
  }

  @Override
  protected AttributedAction.Builder newBuilder() {
    return AttributedAction.newBuilder();
  }

  @Override
  protected AttributedAction build(AttributedAction.Builder builder) {
    builder
        .getActionBuilder()
        .getTimingBuilder()
        .setProcessingTimestamp(TrackingUtil.getProcessingTime());
    return builder.build();
  }

  @Override
  protected boolean matchesDebugIds(TinyAttributedAction rhs) {
    return debugIds.matches(rhs);
  }

  @Override
  protected void outputDropped(
      KeyedCoProcessFunction.Context ctx,
      TinyAttributedAction tinyEvent,
      AttributedAction incompleteAttributedAction) {
    ctx.output(
        DROPPED_TAG,
        DroppedMergeActionDetails.newBuilder()
            .setAction(tinyEvent)
            .setAttributedAction(incompleteAttributedAction)
            .build());
  }

  @Override
  protected String getAllRelatedStateString(TinyAttributedAction attributedAction)
      throws Exception {
    return getAllRelatedStateString(
        attributedAction.getTouchpoint().getJoinedImpression().getImpression().getImpressionId(),
        attributedAction.getAction().getActionId());
  }

  protected String getAllRelatedStateString(String impressionId, String actionId) throws Exception {
    return String.join(
        ",",
        ImmutableList.of(
            joinedImpressionMerger.getDebugStateString(impressionId),
            actionMerger.getDebugStateString(actionId)));
  }

  /** Returns the set of entity types which are missing. */
  @Override
  protected EnumSet<MissingEvent> fillInFull(
      AttributedAction.Builder builder,
      TinyAttributedAction rhs,
      Consumer<MismatchError> errorLogger)
      throws Exception {
    EnumSet<MissingEvent> missing = EnumSet.noneOf(MissingEvent.class);
    if (!joinedImpressionMerger.fillInFull(builder, rhs, errorLogger)) {
      missing.add(MissingEvent.JOINED_IMPRESSION);
    }
    if (!actionMerger.fillInFull(builder, rhs, errorLogger)) {
      missing.add(MissingEvent.ACTION);
    }
    return missing;
  }

  @Override
  protected void cleanupMergers(long timestamp) throws Exception {
    if (isBatchCleanup(timestamp)) {
      joinedImpressionMerger.cleanup(timestamp);
      actionMerger.cleanup(timestamp);
    }
  }

  @Override
  protected boolean hasRequiredEvents(EnumSet<MissingEvent> missing) {
    return !missing.contains(MissingEvent.JOINED_IMPRESSION)
        && !missing.contains(MissingEvent.ACTION);
  }

  // TODO - change this to be TouchpointMerger.
  final class JoinedImpressionMerger extends BaseMerger<JoinedImpression> {
    private static final long serialVersionUID = 2L;

    @VisibleForTesting MapState<String, JoinedImpression> idToJoinedImpression;

    // This map is to speed up out-of-order joins.  This map can be stale.
    // Some clean-up happens incrementally in processElement but it will miss some cases.
    // We do not expect this to happen in production.  It happens in tests.
    @VisibleForTesting MapState<String, List<Long>> impressionIdToIncompleteEventTimers;

    public JoinedImpressionMerger(Duration window, DebugIds debugIds) {
      super(UnionEvent::getJoinedImpression, window, debugIds::matches);
    }

    public void open(RuntimeContext runtimeContext) throws Exception {
      idToJoinedImpression =
          runtimeContext.getMapState(
              new MapStateDescriptor<>(
                  "id-to-joined-impression", String.class, JoinedImpression.class));
      impressionIdToIncompleteEventTimers =
          runtimeContext.getMapState(
              // TODO(PRO-1683) - add caches back in.
              new MapStateDescriptor<>(
                  "impression-id-to-incomplete-event-timers",
                  Types.STRING,
                  Types.LIST(Types.LONG)));
    }

    @Override
    public void addFull(JoinedImpression impression) throws Exception {
      idToJoinedImpression.put(impression.getIds().getImpressionId(), impression);
    }

    @Override
    protected void tryProcessIncompleteEvents(
        JoinedImpression impression,
        boolean matchesDebugId,
        Consumer<MismatchError> errorLogger,
        Collector<AttributedAction> out,
        long timestamp,
        long watermark)
        throws Exception {
      tryProcessIncompleteEvents(
          impression.getIds().getImpressionId(),
          matchesDebugId,
          impressionIdToIncompleteEventTimers,
          errorLogger,
          out,
          timestamp,
          watermark);
    }

    @Override
    protected String getDebugStateString(JoinedImpression impression) throws Exception {
      return getAllRelatedStateString(impression.getIds().getImpressionId(), "");
    }

    protected String getDebugStateString(String id) throws Exception {
      if (id.isEmpty()) {
        return "JoinedImpressionLogMerger{no key checked}";
      }
      JoinedImpression impression = idToJoinedImpression.get(id);
      List<Long> incompleteEventTimers = impressionIdToIncompleteEventTimers.get(id);
      return String.format(
          "JoinedImpressionMerger{idToJoinedImpression[%s]=%s, impressionIdToIncompleteEventTimers[%s]=%s}",
          id, impression, id, incompleteEventTimers);
    }

    /** Returns true iff all of the requested fields are included. */
    public boolean fillInFull(
        AttributedAction.Builder builder,
        TinyAttributedAction rhs,
        Consumer<MismatchError> errorLogger)
        throws Exception {
      String impressionId =
          rhs.getTouchpoint().getJoinedImpression().getImpression().getImpressionId();
      if (!impressionId.isEmpty()) {
        JoinedImpression joinedImpression = idToJoinedImpression.get(impressionId);
        if (joinedImpression != null) {
          // Merge the whole message since it's the first record to be merged.
          builder.setTouchpoint(Touchpoint.newBuilder().setJoinedImpression(joinedImpression));
        } else {
          return false;
        }
      }
      return true;
    }

    public void cleanup(long timestamp) throws Exception {
      cleanupDetailsState(
          idToJoinedImpression,
          timestamp,
          impression -> impression.getTiming().getEventApiTimestamp(),
          window,
          "JoinedImpression",
          debugIds::matchesImpressionId);
      cleanupOldTimers(impressionIdToIncompleteEventTimers, timestamp, "JoinedImpression");
    }
  }

  final class ActionMerger extends BaseMerger<Action> {
    private static final long serialVersionUID = 2L;

    @VisibleForTesting MapState<String, Action> idToAction;

    // This map is to speed up out-of-order joins.  This map can be stale.
    // Some clean-up happens incrementally in processElement but it will miss some cases.
    // We do not expect this to happen in production.  It happens in tests.
    @VisibleForTesting MapState<String, List<Long>> actionIdToIncompleteEventTimers;

    public ActionMerger(Duration window, DebugIds debugIds) {
      super(UnionEvent::getAction, window, debugIds::matches);
    }

    public void open(RuntimeContext runtimeContext) throws Exception {
      idToAction =
          runtimeContext.getMapState(
              new MapStateDescriptor<>("id-to-action", String.class, Action.class));
      actionIdToIncompleteEventTimers =
          runtimeContext.getMapState(
              // TODO(PRO-1683) - add caches back in.
              new MapStateDescriptor<>(
                  "action-id-to-incomplete-event-timers", Types.STRING, Types.LIST(Types.LONG)));
    }

    @Override
    public void addFull(Action action) throws Exception {
      idToAction.put(action.getActionId(), action);
    }

    @Override
    protected void tryProcessIncompleteEvents(
        Action action,
        boolean matchesDebugId,
        Consumer<MismatchError> errorLogger,
        Collector<AttributedAction> out,
        long timestamp,
        long watermark)
        throws Exception {
      tryProcessIncompleteEvents(
          action.getActionId(),
          matchesDebugId,
          actionIdToIncompleteEventTimers,
          errorLogger,
          out,
          timestamp,
          watermark);
    }

    @Override
    protected String getDebugStateString(Action action) throws Exception {
      return getAllRelatedStateString(action.getImpressionId(), action.getActionId());
    }

    protected String getDebugStateString(String id) throws Exception {
      if (id.isEmpty()) {
        return "ActionMerger{no key checked}";
      }
      Action action = idToAction.get(id);
      List<Long> incompleteEventTimers = actionIdToIncompleteEventTimers.get(id);
      return String.format(
          "ActionMerger{idToAction[%s]=%s, actionIdToIncompleteEventTimers[%s]=%s}",
          id, action, id, incompleteEventTimers);
    }

    /** Returns true iff all of the requested fields are included. */
    public boolean fillInFull(
        AttributedAction.Builder builder,
        TinyAttributedAction rhs,
        Consumer<MismatchError> errorLogger)
        throws Exception {
      String actionId = rhs.getAction().getActionId();
      if (!actionId.isEmpty()) {
        Action action = idToAction.get(actionId);
        if (action != null) {
          builder.setAction(action).setAttribution(rhs.getAttribution());
          if (action.hasCart()) {
            // Populate `Action.singleCartItem` using the action content details from TinyActions.
            // This is a little weird to have inside MergeActionDetails.
            // The code tries to pick the most relevant CartItem.  If there are similar CartItems,
            // the first one is picked.
            Optional<CartContent> optionalCartContent =
                getMatchingCartContent(builder.getAction().getCart(), rhs);
            if (optionalCartContent.isPresent()) {
              CartContent cartItem = optionalCartContent.get();
              if (cartItem.getQuantity() == 0) {
                cartItem = cartItem.toBuilder().setQuantity(1).build();
              }
              // TODO - consider moving this onto AttributedAction instead.
              builder.getActionBuilder().setSingleCartContent(cartItem);
            }
          }
        } else {
          return false;
        }
      }
      return true;
    }

    // Prefer Action content_ids.  Then fallback to Insertion content_ids.
    private Optional<CartContent> getMatchingCartContent(Cart cart, TinyAttributedAction rhs) {
      if (!rhs.getAction().getContentId().isEmpty()) {
        Optional<CartContent> cartContent =
            getMatchingCartContent(cart, rhs.getAction().getContentId());
        if (cartContent.isPresent()) {
          return cartContent;
        }
      }
      for (String contentId : rhs.getAction().getOtherContentIdsMap().values()) {
        Optional<CartContent> cartContent = getMatchingCartContent(cart, contentId);
        if (cartContent.isPresent()) {
          return cartContent;
        }
      }

      TinyInsertion insertion = rhs.getTouchpoint().getJoinedImpression().getInsertion();
      String insertionContentId = insertion.getCore().getContentId();
      if (!insertionContentId.isEmpty()) {
        Optional<CartContent> cartContent = getMatchingCartContent(cart, insertionContentId);
        if (cartContent.isPresent()) {
          return cartContent;
        }
      }
      for (String contentId : insertion.getCore().getOtherContentIdsMap().values()) {
        Optional<CartContent> cartContent = getMatchingCartContent(cart, contentId);
        if (cartContent.isPresent()) {
          return cartContent;
        }
      }

      return Optional.empty();
    }

    private Optional<CartContent> getMatchingCartContent(Cart cart, String contentId) {
      for (CartContent cartContent : cart.getContentsList()) {
        if (cartContent.getContentId().equals(contentId)) {
          return Optional.of(cartContent);
        }
      }
      return Optional.empty();
    }

    public void cleanup(long timestamp) throws Exception {
      cleanupDetailsState(
          idToAction,
          timestamp,
          (Action action) -> action.getTiming().getEventApiTimestamp(),
          window,
          "Action",
          debugIds::matchesActionId);
      cleanupOldTimers(actionIdToIncompleteEventTimers, timestamp, "Action");
    }
  }
}
