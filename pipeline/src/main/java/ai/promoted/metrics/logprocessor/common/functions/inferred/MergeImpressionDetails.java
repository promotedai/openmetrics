package ai.promoted.metrics.logprocessor.common.functions.inferred;

import static ai.promoted.metrics.logprocessor.common.util.DeliveryLogUtil.getRequest;

import ai.promoted.metrics.error.MismatchError;
import ai.promoted.metrics.logprocessor.common.util.DebugIds;
import ai.promoted.metrics.logprocessor.common.util.DeliveryLogUtil;
import ai.promoted.metrics.logprocessor.common.util.FlatUtil;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.event.CombinedDeliveryLog;
import ai.promoted.proto.event.HiddenApiRequest;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.InsertionBundle;
import ai.promoted.proto.event.JoinedEvent;
import ai.promoted.proto.event.TinyEvent;
import ai.promoted.proto.event.UnionEvent;
import ai.promoted.proto.event.View;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// TODO - move it it's own package in a different PR.
/**
 * Takes tiny impressions (from Inferred References) and fills in details using the stream of
 * UnionEvent.
 */
public class MergeImpressionDetails extends AbstractMergeDetails {
  private static final Logger LOGGER = LogManager.getLogger(MergeImpressionDetails.class);

  @VisibleForTesting final ViewMerger viewMerger;
  @VisibleForTesting final DeliveryLogMerger deliveryLogMerger;
  @VisibleForTesting final ImpressionMerger impressionMerger;
  private final boolean skipViewJoin;
  private final DebugIds debugIds;

  public MergeImpressionDetails(
      Duration viewWindow,
      Duration deliveryLogWindow,
      Duration impressionWindow,
      Duration batchCleanupWindow,
      Duration missingEntityDelay,
      boolean skipViewJoin,
      Long allTimersBeforeCauseBatchCleanup,
      DebugIds debugIds) {
    super(batchCleanupWindow, missingEntityDelay, allTimersBeforeCauseBatchCleanup, debugIds);
    this.viewMerger = new ViewMerger(viewWindow, debugIds);
    this.deliveryLogMerger = new DeliveryLogMerger(deliveryLogWindow, debugIds);
    this.impressionMerger = new ImpressionMerger(impressionWindow, debugIds);
    this.skipViewJoin = skipViewJoin;
    this.debugIds = debugIds;
  }

  @Override
  public void open(Configuration config) throws Exception {
    super.open(config);
    viewMerger.open(getRuntimeContext());
    deliveryLogMerger.open(getRuntimeContext());
    impressionMerger.open(getRuntimeContext());
  }

  @Override
  public void processElement1(UnionEvent input, Context ctx, Collector<JoinedEvent> out)
      throws Exception {
    // TODO - optimize the full records being stored in maps.  We can strip out fields we'll
    // eventually clear.
    switch (input.getEventCase()) {
      case VIEW:
        viewMerger.processElement1(input, ctx, out);
        break;
      case COMBINED_DELIVERY_LOG:
        deliveryLogMerger.processElement1(input, ctx, out);
        break;
      case IMPRESSION:
        impressionMerger.processElement1(input, ctx, out);
        break;
      default:
        throw new UnsupportedOperationException("Unsupported UnionEvent=" + input);
    }
  }

  @Override
  protected void addIncompleteTimers(
      TinyEvent rhs,
      Context ctx,
      EnumSet<AbstractMergeDetails.MissingEvent> missing,
      long timerTimestamp)
      throws Exception {
    for (MissingEvent missingEvent : missing) {
      switch (missingEvent) {
        case VIEW:
          addIncompleteTimers(
              viewMerger.viewIdToIncompleteEventTimers, rhs.getViewId(), timerTimestamp);
          break;
        case DELIERY_LOG:
          addIncompleteTimers(
              deliveryLogMerger.requestIdToIncompleteEventTimers,
              rhs.getRequestId(),
              timerTimestamp);
          break;
        case IMPRESSION:
          addIncompleteTimers(
              impressionMerger.impressionIdToIncompleteEventTimers,
              rhs.getImpressionId(),
              timerTimestamp);
          break;
        default:
          throw new UnsupportedOperationException("Unsupported MissingEntity=" + missingEvent);
      }
    }
  }

  @Override
  protected String getAllRelatedStateString(
      String viewId, String requestId, String impressionId, String actionId) throws Exception {
    return String.join(
        ",",
        ImmutableList.of(
            viewMerger.getDebugStateString(viewId),
            deliveryLogMerger.getDebugStateString(requestId),
            impressionMerger.getDebugStateString(impressionId)));
  }

  /** Returns the set of entity types which are missing. */
  @Override
  protected EnumSet<MissingEvent> fillInFull(
      JoinedEvent.Builder builder,
      TinyEvent rhs,
      BiConsumer<OutputTag<MismatchError>, MismatchError> errorLogger)
      throws Exception {
    EnumSet<MissingEvent> missing = EnumSet.noneOf(MissingEvent.class);
    if (!skipViewJoin && !viewMerger.fillInFull(builder, rhs, errorLogger)) {
      missing.add(MissingEvent.VIEW);
    }
    if (!deliveryLogMerger.fillInFull(builder, rhs, errorLogger)) {
      missing.add(MissingEvent.DELIERY_LOG);
    }
    if (!impressionMerger.fillInFull(builder, rhs, errorLogger)) {
      missing.add(MissingEvent.IMPRESSION);
    }
    return missing;
  }

  @Override
  protected void cleanupMergers(long timestamp) throws Exception {
    if (isBatchCleanup(timestamp)) {
      viewMerger.cleanup(timestamp);
      deliveryLogMerger.cleanup(timestamp);
      impressionMerger.cleanup(timestamp);
    }
  }

  @Override
  protected boolean hasRequiredEvents(EnumSet<MissingEvent> missing) {
    return !missing.contains(MissingEvent.DELIERY_LOG)
        && !missing.contains(MissingEvent.IMPRESSION);
  }

  final class ViewMerger extends BaseMerger<View> {
    private static final long serialVersionUID = 2L;

    @VisibleForTesting MapState<String, View> idToView;

    // This map is to speed up out-of-order joins.  This map can be stale.
    // Some clean-up happens incrementally in processElement but it will miss some cases.
    // We do not expect this to happen in production.  It happens in tests.
    @VisibleForTesting MapState<String, List<Long>> viewIdToIncompleteEventTimers;

    public ViewMerger(Duration window, DebugIds debugIds) {
      super(UnionEvent::getView, window, debugIds::matches);
    }

    public void open(RuntimeContext runtimeContext) throws Exception {
      idToView =
          runtimeContext.getMapState(
              new MapStateDescriptor<>("id-to-view", String.class, View.class));
      viewIdToIncompleteEventTimers =
          runtimeContext.getMapState(
              // TODO(PRO-1683) - add caches back in.
              new MapStateDescriptor<>(
                  "view-id-to-incomplete-event-timers", Types.STRING, Types.LIST(Types.LONG)));
    }

    @Override
    protected void addFull(View view) throws Exception {
      idToView.put(view.getViewId(), view);
    }

    @Override
    protected void tryProcessIncompleteEvents(
        View view,
        boolean matchesDebugId,
        BiConsumer<OutputTag<MismatchError>, MismatchError> errorLogger,
        Collector<JoinedEvent> out)
        throws Exception {
      tryProcessIncompleteEvents(
          view.getViewId(), matchesDebugId, viewIdToIncompleteEventTimers, errorLogger, out);
    }

    @Override
    protected String getDebugStateString(View view) throws Exception {
      return getAllRelatedStateString(view.getViewId(), "", "", "");
    }

    protected String getDebugStateString(String id) throws Exception {
      if (id.isEmpty()) {
        return "ViewMerger{no key checked}";
      }
      View view = idToView.get(id);
      List<Long> incompleteEventTimers = viewIdToIncompleteEventTimers.get(id);
      return String.format(
          "ViewMerger{idToView[%s]=%s, viewIdToIncompleteEventTimers[%s]=%s}",
          id, view, id, incompleteEventTimers);
    }

    /** Returns true iff all of the requested fields are included. */
    public boolean fillInFull(
        JoinedEvent.Builder builder,
        TinyEvent rhs,
        BiConsumer<OutputTag<MismatchError>, MismatchError> errorLogger)
        throws Exception {
      if (!rhs.getViewId().isEmpty()) {
        View view = idToView.get(rhs.getViewId());
        if (view != null) {
          FlatUtil.setFlatView(builder, view, errorLogger);
        } else {
          return false;
        }
      }
      return true;
    }

    public void cleanup(long timestamp) throws Exception {
      cleanupDetailsState(
          idToView,
          timestamp,
          (View view) -> view.getTiming().getLogTimestamp(),
          window,
          "View",
          debugIds::matchesViewId);
      cleanupOldTimers(viewIdToIncompleteEventTimers, timestamp, "View");
    }
  }

  final class DeliveryLogMerger extends BaseMerger<CombinedDeliveryLog> {
    private static final long serialVersionUID = 2L;

    @VisibleForTesting MapState<String, CombinedDeliveryLog> idToCombinedDeliveryLog;
    @VisibleForTesting MapState<String, InsertionBundle> idToInsertion;

    // This map is to speed up out-of-order joins.  This map can be stale.
    // Some clean-up happens incrementally in processElement but it will miss some cases.
    // We do not expect this to happen in production.  It happens in tests.
    @VisibleForTesting MapState<String, List<Long>> requestIdToIncompleteEventTimers;

    public DeliveryLogMerger(Duration window, DebugIds debugIds) {
      super(UnionEvent::getCombinedDeliveryLog, window, debugIds::matches);
    }

    public void open(RuntimeContext runtimeContext) throws Exception {
      idToCombinedDeliveryLog =
          runtimeContext.getMapState(
              new MapStateDescriptor<>(
                  "id-to-combineddeliverylog", String.class, CombinedDeliveryLog.class));
      idToInsertion =
          runtimeContext.getMapState(
              new MapStateDescriptor<>("id-to-insertion", String.class, InsertionBundle.class));
      requestIdToIncompleteEventTimers =
          runtimeContext.getMapState(
              // TODO(PRO-1683) - add caches back in.
              new MapStateDescriptor<>(
                  "request-id-to-incomplete-event-timers", Types.STRING, Types.LIST(Types.LONG)));
    }

    @Override
    protected void addFull(CombinedDeliveryLog input) throws Exception {
      // We send DeliveryLog as a single unit.  We split the state once in this operator.
      CombinedDeliveryLog combinedDeliveryLog = stripInsertionAndUnusedFields(input).build();
      String requestId = DeliveryLogUtil.getRequestId(input);
      idToCombinedDeliveryLog.put(requestId, combinedDeliveryLog);
      List<InsertionIdAndBundle> insertions = toInsertionBundles(input);
      for (InsertionIdAndBundle idAndBundle : insertions) {
        String responseInsertionId = idAndBundle.insertionId();
        Preconditions.checkArgument(
            !responseInsertionId.isEmpty(),
            "Response Insertion should have insertionId, combinedDeliveryLog=%s",
            input);
        idToInsertion.put(responseInsertionId, idAndBundle.bundle());
      }
    }

    @Override
    protected void tryProcessIncompleteEvents(
        CombinedDeliveryLog deliveryLog,
        boolean matchesDebugId,
        BiConsumer<OutputTag<MismatchError>, MismatchError> errorLogger,
        Collector<JoinedEvent> out)
        throws Exception {
      // A little redundant work.
      String requestId = DeliveryLogUtil.getRequestId(deliveryLog);
      tryProcessIncompleteEvents(
          requestId, matchesDebugId, requestIdToIncompleteEventTimers, errorLogger, out);
    }

    @Override
    protected String getDebugStateString(CombinedDeliveryLog deliveryLog) throws Exception {
      Request request = DeliveryLogUtil.getRequest(deliveryLog);
      return getAllRelatedStateString(request.getViewId(), request.getRequestId(), "", "");
    }

    protected String getDebugStateString(String id) throws Exception {
      if (id.isEmpty()) {
        return "DeliveryLogMerger{no key checked}";
      }
      CombinedDeliveryLog combinedDelivery = idToCombinedDeliveryLog.get(id);
      InsertionBundle insertionBundle = idToInsertion.get(id);
      List<Long> incompleteEventTimers = requestIdToIncompleteEventTimers.get(id);
      return String.format(
          "DeliveryLogMerger{idToCombinedDeliveryLog[%s]=%s, idToInsertion[%s]=%s, viewIdToIncompleteEventTimers[%s]=%s}",
          id, combinedDelivery, id, insertionBundle, id, incompleteEventTimers);
    }

    /** Returns true iff all of the requested fields are included. */
    public boolean fillInFull(
        JoinedEvent.Builder builder,
        TinyEvent rhs,
        BiConsumer<OutputTag<MismatchError>, MismatchError> errorLogger)
        throws Exception {
      boolean allMatched = true;
      if (!rhs.getRequestId().isEmpty()) {
        CombinedDeliveryLog combinedDeliveryLog = idToCombinedDeliveryLog.get(rhs.getRequestId());
        if (combinedDeliveryLog != null) {
          FlatUtil.setFlatRequest(builder, getRequest(combinedDeliveryLog), errorLogger);
          if (combinedDeliveryLog.hasApi() && combinedDeliveryLog.hasSdk()) {
            // If the JoinedEvent has both SDK and API Requests, the SDK Response was used.
            // Most of the Request fields are the same.  We'll keep fields that we expect to be
            // different in HiddenApiRequest.
            builder.setHiddenApiRequest(toHiddenApiRequest(combinedDeliveryLog.getApi()));
          }
          FlatUtil.setFlatResponse(
              builder, DeliveryLogUtil.getDeliveryLog(combinedDeliveryLog).getResponse());
          if (combinedDeliveryLog.hasApi() && combinedDeliveryLog.getApi().hasExecution()) {
            FlatUtil.setFlatApiExecution(builder, combinedDeliveryLog.getApi().getExecution());
          }
          if (combinedDeliveryLog.hasSdk() && combinedDeliveryLog.getSdk().hasExecution()) {
            FlatUtil.setFlatSdkExecution(builder, combinedDeliveryLog.getSdk().getExecution());
          }
        } else {
          allMatched = false;
        }
      }
      InsertionBundle insertion = null;
      if (!rhs.getInsertionId().isEmpty()) {
        insertion = idToInsertion.get(rhs.getInsertionId());
        // Fill this in since it's not on the InsertionBundle that is saved to Flink state.
        builder.getIdsBuilder().setInsertionId(rhs.getInsertionId());
      }

      if (insertion != null) {
        if (insertion.hasRequestInsertion()) {
          FlatUtil.setFlatRequestInsertion(
              builder, insertion.getRequestInsertion(), rhs.getLogTimestamp(), errorLogger);
        }
        if (insertion.hasResponseInsertion()) {
          FlatUtil.setFlatResponseInsertion(
              builder, insertion.getResponseInsertion(), rhs.getLogTimestamp(), errorLogger);
        }
        if (insertion.hasApiExecutionInsertion()) {
          FlatUtil.setFlatApiExecutionInsertion(
              builder, insertion.getApiExecutionInsertion(), rhs.getLogTimestamp(), errorLogger);
        }
      } else {
        allMatched = false;
      }
      return allMatched;
    }

    public void cleanup(long timestamp) throws Exception {
      cleanupDetailsState(
          idToCombinedDeliveryLog,
          timestamp,
          (CombinedDeliveryLog deliveryLog) ->
              getRequest(deliveryLog).getTiming().getLogTimestamp(),
          window,
          "CombinedDeliveryLog",
          debugIds::matchesRequestId);
      cleanupDetailsState(
          idToInsertion,
          timestamp,
          InsertionBundle::getLogTimestamp,
          window,
          "Insertion",
          debugIds::matchesInsertionId);
      cleanupOldTimers(requestIdToIncompleteEventTimers, timestamp, "Request");
    }

    private CombinedDeliveryLog.Builder stripInsertionAndUnusedFields(
        CombinedDeliveryLog combinedDeliveryLog) {
      CombinedDeliveryLog.Builder builder = combinedDeliveryLog.toBuilder();
      if (builder.hasSdk() && builder.hasApi()) {
        // If the JoinedEvent has both SDK and API Requests, the SDK Response was used.
        // Most of the Request fields are the same.  We'll keep fields that we expect to be
        // different in HiddenApiRequest.
        builder
            .getApiBuilder()
            .clearResponse()
            .setRequest(stripToHiddenApiRequestFields(builder.getApi().getRequest()));
      }
      if (builder.hasSdk()) {
        stripInsertionFields(builder.getSdkBuilder());
      }
      if (builder.hasApi()) {
        stripInsertionFields(builder.getApiBuilder());
      }
      return builder;
    }

    private DeliveryLog.Builder stripInsertionFields(DeliveryLog.Builder builder) {
      if (builder.hasRequest()) {
        builder.getRequestBuilder().clearInsertion();
      }
      if (builder.hasResponse()) {
        builder.getResponseBuilder().clearInsertion();
      }
      if (builder.hasExecution()) {
        builder.getExecutionBuilder().clearExecutionInsertion();
      }
      return builder;
    }

    private Request stripToHiddenApiRequestFields(Request request) {
      return Request.newBuilder()
          .setRequestId(request.getRequestId())
          .setTiming(request.getTiming())
          .setClientInfo(request.getClientInfo())
          .build();
    }

    private HiddenApiRequest toHiddenApiRequest(DeliveryLog deliveryLog) {
      Request request = deliveryLog.getRequest();
      return HiddenApiRequest.newBuilder()
          .setRequestId(request.getRequestId())
          .setTiming(request.getTiming())
          .setClientInfo(request.getClientInfo())
          .build();
    }

    private List<InsertionIdAndBundle> toInsertionBundles(CombinedDeliveryLog combinedDeliveryLog) {
      DeliveryLog deliveryLog = DeliveryLogUtil.getDeliveryLog(combinedDeliveryLog);
      Map<String, Insertion> contentIdToRequestInsertion =
          deliveryLog.getRequest().getInsertionList().stream()
              .collect(
                  Collectors.toMap(
                      Insertion::getContentId,
                      Function.identity(),
                      (insertion1, insertion2) -> {
                        // If we see duplicate contentIds, we need to verify how duplicate
                        // contentIds work.
                        LOGGER.error(
                            "Multiple request insertions with the same contentId, {}, found on DeliveryLog={}",
                            insertion1.getContentId(),
                            deliveryLog);
                        // Just take the first insertion for now.
                        return insertion1;
                      }));
      Map<String, Insertion> insertionIdToApiExecutionInsertion =
          combinedDeliveryLog.getApi().getExecution().getExecutionInsertionList().stream()
              // This can happen for some older DeliveryLogs.
              // TODO - remove this condition.
              .filter(insertion -> !insertion.getInsertionId().isEmpty())
              .collect(
                  Collectors.toMap(
                      Insertion::getInsertionId,
                      Function.identity(),
                      (insertion1, insertion2) -> {
                        LOGGER.error(
                            "Multiple execution insertions with the same insertionId, {}, found on combinedDeliveryLog.getApi()={}",
                            insertion1.getInsertionId(),
                            combinedDeliveryLog.getApi());
                        // Just take the first insertion for now.
                        return insertion1;
                      }));
      Map<String, Insertion> contentIdToApiExecutionInsertion =
          combinedDeliveryLog.getApi().getExecution().getExecutionInsertionList().stream()
              .collect(
                  Collectors.toMap(
                      Insertion::getContentId,
                      Function.identity(),
                      (insertion1, insertion2) -> {
                        // Just take the first insertion for now.  It's more likely to get an
                        // impression.
                        // This logic will have to change
                        // TODO(PRO-2012) - Simplify Metrics support for missing insertionIds.
                        return insertion1;
                      }));
      long logTimestamp = deliveryLog.getRequest().getTiming().getLogTimestamp();

      return deliveryLog.getResponse().getInsertionList().stream()
          .map(
              responseInsertion -> {
                InsertionBundle.Builder builder =
                    InsertionBundle.newBuilder().setLogTimestamp(logTimestamp);
                builder.setResponseInsertion(
                    FlatUtil.clearRedundantFlatInsertionFields(responseInsertion.toBuilder()));
                String contentId = responseInsertion.getContentId();
                Insertion requestInsertion = contentIdToRequestInsertion.get(contentId);
                if (requestInsertion != null) {
                  builder.setRequestInsertion(
                      FlatUtil.clearRedundantFlatInsertionFields(requestInsertion.toBuilder()));
                }
                String insertionId = responseInsertion.getInsertionId();
                Insertion apiExecutionInsertion =
                    insertionIdToApiExecutionInsertion.get(insertionId);
                if (apiExecutionInsertion == null) {
                  apiExecutionInsertion = contentIdToApiExecutionInsertion.get(contentId);
                }
                if (apiExecutionInsertion != null) {
                  builder.setApiExecutionInsertion(
                      FlatUtil.clearRedundantFlatInsertionFields(
                          apiExecutionInsertion.toBuilder()));
                }
                stripInsertionBundle(builder);
                return InsertionIdAndBundle.create(insertionId, builder.build());
              })
          .collect(ImmutableList.toImmutableList());
    }

    private void stripInsertionBundle(InsertionBundle.Builder builder) {
      stripRequestFields(builder);

      // Strip redundant fields.  insertion_id is saved as the key.
      if (builder.hasRequestInsertion()) {
        builder.getRequestInsertionBuilder().clearInsertionId().clearContentId();
      }
      if (builder.hasApiExecutionInsertion()) {
        builder.getApiExecutionInsertionBuilder().clearInsertionId().clearContentId();
      }
      if (builder.hasResponseInsertion()) {
        builder.getResponseInsertionBuilder().clearInsertionId();
      }
    }

    // Strip out fields that we should be on Request.
    private void stripRequestFields(InsertionBundle.Builder builder) {
      if (builder.hasApiExecutionInsertion()) {
        stripRequestFields(builder.getApiExecutionInsertionBuilder());
      }
      if (builder.hasRequestInsertion()) {
        stripRequestFields(builder.getRequestInsertionBuilder());
      }
      if (builder.hasResponseInsertion()) {
        stripRequestFields(builder.getResponseInsertionBuilder());
      }
    }

    private void stripRequestFields(Insertion.Builder builder) {
      builder
          .clearPlatformId()
          .clearTiming()
          .clearUserInfo()
          .clearClientInfo()
          .clearSessionId()
          .clearAutoViewId()
          .clearViewId()
          .clearRequestId();
    }
  }

  final class ImpressionMerger extends BaseMerger<Impression> {
    private static final long serialVersionUID = 2L;

    @VisibleForTesting MapState<String, Impression> idToImpression;

    // This map is to speed up out-of-order joins.  This map can be stale.
    // Some clean-up happens incrementally in processElement but it will miss some cases.
    // We do not expect this to happen in production.  It happens in tests.
    @VisibleForTesting MapState<String, List<Long>> impressionIdToIncompleteEventTimers;

    public ImpressionMerger(Duration window, DebugIds debugIds) {
      super(UnionEvent::getImpression, window, debugIds::matches);
    }

    public void open(RuntimeContext runtimeContext) throws Exception {
      idToImpression =
          runtimeContext.getMapState(
              new MapStateDescriptor<>("id-to-impression", String.class, Impression.class));
      impressionIdToIncompleteEventTimers =
          runtimeContext.getMapState(
              // TODO(PRO-1683) - add caches back in.
              new MapStateDescriptor<>(
                  "impression-id-to-incomplete-event-timers",
                  Types.STRING,
                  Types.LIST(Types.LONG)));
    }

    @Override
    public void addFull(Impression impression) throws Exception {
      idToImpression.put(impression.getImpressionId(), impression);
    }

    @Override
    protected void tryProcessIncompleteEvents(
        Impression impression,
        boolean matchesDebugId,
        BiConsumer<OutputTag<MismatchError>, MismatchError> errorLogger,
        Collector<JoinedEvent> out)
        throws Exception {
      tryProcessIncompleteEvents(
          impression.getImpressionId(),
          matchesDebugId,
          impressionIdToIncompleteEventTimers,
          errorLogger,
          out);
    }

    @Override
    protected String getDebugStateString(Impression impression) throws Exception {
      return getAllRelatedStateString(
          impression.getViewId(), impression.getRequestId(), impression.getImpressionId(), "");
    }

    protected String getDebugStateString(String id) throws Exception {
      if (id.isEmpty()) {
        return "ImpressionLogMerger{no key checked}";
      }
      Impression impression = idToImpression.get(id);
      List<Long> incompleteEventTimers = impressionIdToIncompleteEventTimers.get(id);
      return String.format(
          "ImpressionMerger{idToImpression[%s]=%s, impressionIdToIncompleteEventTimers[%s]=%s}",
          id, impression, id, incompleteEventTimers);
    }

    /** Returns true iff all of the requested fields are included. */
    public boolean fillInFull(
        JoinedEvent.Builder builder,
        TinyEvent rhs,
        BiConsumer<OutputTag<MismatchError>, MismatchError> errorLogger)
        throws Exception {
      if (!rhs.getImpressionId().isEmpty()) {
        Impression impression = idToImpression.get(rhs.getImpressionId());
        if (impression != null) {
          FlatUtil.setFlatImpression(builder, impression, errorLogger);
          builder.setTiming(impression.getTiming());
        } else {
          return false;
        }
      }
      return true;
    }

    public void cleanup(long timestamp) throws Exception {
      cleanupDetailsState(
          idToImpression,
          timestamp,
          (Impression impression) -> impression.getTiming().getLogTimestamp(),
          window,
          "Impression",
          debugIds::matchesImpressionId);
      cleanupOldTimers(impressionIdToIncompleteEventTimers, timestamp, "Impression");
    }
  }
}
