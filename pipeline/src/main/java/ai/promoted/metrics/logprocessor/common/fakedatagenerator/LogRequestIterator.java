package ai.promoted.metrics.logprocessor.common.fakedatagenerator;

import static com.google.common.collect.ImmutableList.copyOf;

import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.Content;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.ContentDB;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.ContentDBFactory;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.ContentDBFilter;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.ContentType;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.FakeInsertionSurface;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.ImpressedContent;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.InsertedContent;
import ai.promoted.proto.common.ClientInfo;
import ai.promoted.proto.common.CurrencyCode;
import ai.promoted.proto.common.Money;
import ai.promoted.proto.common.Properties;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.delivery.DeliveryExecution;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.ExecutionServer;
import ai.promoted.proto.delivery.FeatureStage;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.delivery.Response;
import ai.promoted.proto.delivery.internal.features.Features;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.AutoView;
import ai.promoted.proto.event.Cart;
import ai.promoted.proto.event.CartContent;
import ai.promoted.proto.event.CohortArm;
import ai.promoted.proto.event.CohortMembership;
import ai.promoted.proto.event.Diagnostics;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.LogRequest;
import ai.promoted.proto.event.MobileDiagnostics;
import ai.promoted.proto.event.Session;
import ai.promoted.proto.event.SessionProfile;
import ai.promoted.proto.event.User;
import ai.promoted.proto.event.View;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;

/**
 * This iterator produces fake data {@code LogRequests} with delays to mimic real traffic. This is
 * not thread safe.
 *
 * <ul>
 *   <li>A heap is used to manage future {@code LogRequests}. The first item might be in {@code
 *       nextState} variable. This allows us to delay child {@code LogRequests}.
 *   <li>The heap values contains an external {@code LogRequest} (returned to clients) and an
 *       optional {@code Runnable}. The {@code Runnable} is executed when we want to add more state
 *       to the heap. The {@code Runnable} can get executed while calling {@code hasNext()} or
 *       {@code peekTime}.
 * </ul>
 *
 * <p>Consumers should call {@link #hasNext()} and {@link #peekTime()} to control this
 * LogRequestIterator. When the time is passed, then {@link #next()} should be called.
 *
 * <p>Design notes for {@code addAbcToHeap} methods
 *
 * <ul>
 *   <li>The methods are designed to be simple. Instead of having an explicit state machine, the
 *       methods are responsible for figuring out the next state. They can look at options and other
 *       parameters to influence the next states for the generator. A single method can create
 *       multiple future outputs for this iterator.
 *   <li>When adding or modifying, assume the methods can be executed when peeking.
 *   <li>Group the {@code addAbcToHeap} methods together.
 *   <li>Timing logic is broken. The caller waits using {@code peekTime()}. The actual timing on the
 *       records will look to see what {@code now()} is. These might not be the same. The timing can
 *       now start executing earlier since those methods get called while peeking.
 *       <p>TODO - fix the time logic. E.g. change the {@code addAbcToHeap} methods to pass through
 *       the times. E.g. change the background runnables to have a slightly later timestamp (+1ms).
 *   <li>This {@code Runnable} design would make it hard to distribute state of the {@code
 *       LogRequestIterator}. That's not important right now. The {@code Runnable} design makes it
 *       easier to delay execution of derived state.
 * </ul>
 */
public final class LogRequestIterator implements Iterator<LogRequest> {
  public static final long PLATFORM_ID = 1L;
  private final LogRequestIteratorOptions options;
  private final ContentDB contentDB;
  /* Future events are ordered using a heap.  State might contain Runnables that add more to stateHeap.
     It's ordered by time and then by a index to make a more consistent sort.
  */
  private final MinMaxPriorityQueue<State> stateHeap =
      MinMaxPriorityQueue.<State>orderedBy(
              Ordering.compound(
                  ImmutableList.of(
                      Ordering.natural().onResultOf(state -> state.time()),
                      Ordering.natural().onResultOf(state -> state.index()))))
          .create();
  // The Cart state is not in sync with stateHeap.
  // TODO - synchronize it.  It'd probably mean keeping track of updates with timestamps.  The
  // complexity isn't
  // worth it right now.
  // This is currently keyed by a forgettable user ID (anonUserId or logUserId).
  private final Map<String, Cart> anyUserIdToCart = new HashMap<>();
  /* nextState contains the next state to process `next()`.  If null, check stateHeap. */
  @Nullable private State nextState;
  private int nextStateIndex;

  public LogRequestIterator(LogRequestIteratorOptions.Builder optionsBuilder) {
    this(optionsBuilder.build());
  }

  public LogRequestIterator(LogRequestIteratorOptions options) {
    this(options, ContentDBFactory.create(PLATFORM_ID, options.contentDBFactoryOptions()));
  }

  public LogRequestIterator(LogRequestIteratorOptions options, ContentDB contentDB) {
    this.options = options;
    this.contentDB = contentDB;
    addUsersToHeap();
  }

  private static boolean isMissingEvent(float missingRate, String eventId, String additionalHash) {
    return matchesRateOption(missingRate, eventId, additionalHash);
  }

  public static boolean matchesRateOption(float rate, String id, String additionalHash) {
    return new Random(hash(id + additionalHash)).nextFloat() < rate;
  }

  private static int hash(String value) {
    return value.hashCode() * 31 + 17;
  }

  /** Performs modulo but shifts negative values to be positive. */
  private static int modAbs(int num, int modulo) {
    int value = num % modulo;
    if (value < 0) {
      value += modulo;
    }
    return value;
  }

  /**
   * Returns a fraction of {@code size} by using {@code rate}. Uses {@code id} to deterministically
   * do ceiling or floor at a {@code rate}.
   */
  public static int fromRateToCount(float rate, int size, String id, String additionalHash) {
    if (matchesRateOption(rate, id, additionalHash)) {
      // Ceiling.
      return (int) Math.ceil(rate * size);
    } else {
      // Floor.
      return (int) Math.floor(rate * size);
    }
  }

  private static long toContentIdLong(String contentId) {
    return Long.parseLong(contentId.substring(contentId.lastIndexOf('-') + 1));
  }

  private static String getAnyLogUserId(UserInfo userInfo) {
    if (!userInfo.getAnonUserId().isEmpty()) {
      return userInfo.getAnonUserId();
    }
    if (!userInfo.getLogUserId().isEmpty()) {
      return userInfo.getLogUserId();
    }
    return userInfo.getUserId();
  }

  public boolean hasNext() {
    processStatesUntilNextStateIsLogRequest();
    return nextState != null;
  }

  public Instant peekTime() {
    processStatesUntilNextStateIsLogRequest();
    return nextState != null ? nextState.time() : null;
  }

  /**
   * Processes the earliest heap states up to {@code stateHeap} is {@code LogRequest}. This will
   * call {@code runnable().run()} on each state that is processed.
   */
  private void processStatesUntilNextStateIsLogRequest() {
    // Loop until we have an LogRequest or no more state.
    while (!stateHeap.isEmpty() || nextState != null) {
      if (nextState == null) {
        nextState = stateHeap.removeFirst();
      }
      if (nextState.logRequest().isPresent()) {
        return;
      } else {
        nextState.runnable().run();
        // If no logRequest, set to null so we can pull on the next loop.
        nextState = null;
      }
    }
  }

  /**
   * Used to delay events to simulate delayed event behavior. Uses delayMultiplier to allow for
   * randomness and scaling.
   */
  private Instant delay(Instant now, Duration maxDelay) {
    return now.plus(options.delayMultiplier().apply(maxDelay));
  }

  public LogRequest next() {
    processStatesUntilNextStateIsLogRequest();
    Preconditions.checkState(
        nextState != null, "No more LogRequests.  Make sure to call hasNext first.");
    nextState.runnable().run();
    State returnState = nextState;
    nextState = null;
    return returnState.logRequest().get();
  }

  private void addUsersToHeap() {
    IntStream.range(0, options.users()).forEach(i -> addUserToHeap());
  }

  private void addUserToHeap() {
    String userId = options.userUuid();
    String anonUserId = options.anonUserUuid(userId);

    Instant now = options.now();
    Instant eventApiTimestamp = options.eventApiTimestamp();
    UserInfo.Builder userInfoBuilder = UserInfo.newBuilder();
    if (options.writeAnonUserId()) {
      userInfoBuilder.setAnonUserId(anonUserId);
    }
    if (options.writeLogUserId()) {
      // In earlier versions, platforms are writing their anonUserId to the logUserId field.
      userInfoBuilder.setLogUserId(anonUserId);
    }
    if (options.writeUserId()) {
      userInfoBuilder.setUserId(userId);
    }
    UserInfo userInfo = userInfoBuilder.build();
    Timing timing = createTiming(eventApiTimestamp);

    LogRequest.Builder builder =
        LogRequest.newBuilder()
            .setPlatformId(PLATFORM_ID)
            .setUserInfo(userInfo)
            .setTiming(timing)
            .addUser(User.newBuilder().setUserInfo(userInfo).setTiming(timing));
    final LogRequest external = builder.build();
    if (options.writeUsers()) {
      addToHeap(now, external);
    }
    addToHeap(now, () -> addCohortMembershipsToHeap(external, userId));
    addToHeap(now, () -> addSessionsToHeap(external, userInfo));
  }

  private void addCohortMembershipsToHeap(LogRequest request, String userId) {
    boolean inExperiment = userId.hashCode() % 2 == 0;
    Instant now = options.now();
    Instant eventApiTimestamp = options.eventApiTimestamp();
    // For now, create one CohortMembership per User.
    LogRequest external =
        toCommonFieldsBuilder(request)
            .setTiming(createTiming(eventApiTimestamp))
            .addCohortMembership(
                CohortMembership.newBuilder()
                    .setCohortId("PERSONALIZE_V1")
                    .setMembershipId(options.cohortMembershipUuid())
                    .setArm(inExperiment ? CohortArm.TREATMENT : CohortArm.CONTROL)
                    .setTiming(createTiming(eventApiTimestamp)))
            .build();
    addToHeap(delay(now, Duration.ofSeconds(1)), external);
  }

  private void addSessionsToHeap(LogRequest request, UserInfo userInfo) {
    IntStream.range(0, options.sessionsPerUser()).forEach(i -> addSessionToHeap(request, userInfo));
  }

  private void addSessionToHeap(LogRequest request, UserInfo userInfo) {
    Instant now = options.now();
    Instant eventApiTimestamp = options.eventApiTimestamp();
    LogRequest.Builder builder =
        toCommonFieldsBuilder(request)
            .setUserInfo(userInfo)
            .setTiming(createTiming(eventApiTimestamp));
    final String sessionId = options.sessionUuid();
    builder.addSessionProfile(
        options
            .sessionProfileTransform()
            .apply(
                SessionProfile.newBuilder()
                    .setSessionId(sessionId)
                    .setTiming(createTiming(eventApiTimestamp))));

    builder.addSession(
        options
            .sessionTransform()
            .apply(
                Session.newBuilder()
                    .setSessionId(sessionId)
                    // TODO - use a separate log.
                    .setUserInfo(userInfo.toBuilder().clearUserId().build())
                    .setTiming(createTiming(eventApiTimestamp))));
    // TODO - change delay per i.
    Instant nextTime = delay(now, Duration.ofSeconds(1));
    LogRequest logRequest = builder.build();
    addToHeap(nextTime, logRequest);
    addToHeap(nextTime, () -> addViewsToHeap(logRequest, sessionId, getRequestInsertionSurfaces()));
    addToHeap(nextTime, () -> addAutoViewsToHeap(logRequest, sessionId));
  }

  // Expected to be required.
  private void addViewsToHeap(
      LogRequest request,
      String sessionId,
      ImmutableList<FakeInsertionSurface> requestInsertionSurfaces) {
    IntStream.range(0, options.viewsPerSession())
        .forEach(
            i ->
                addViewToHeap(
                    request,
                    sessionId,
                    i,
                    "",
                    "All " + requestInsertionSurfaces.get(0).contentType().name(),
                    true,
                    // The initial list of content only filters by type (no ID filters).
                    ImmutableList.of(
                        ContentDBFilter.whereFieldIs(
                            Content.TYPE, requestInsertionSurfaces.get(0).contentType().name())),
                    requestInsertionSurfaces));
  }

  private void addViewToHeap(
      LogRequest request,
      String sessionId,
      int index,
      String contentId,
      String viewName,
      boolean addDeliveryLogs,
      ImmutableList<ContentDBFilter> filters,
      ImmutableList<FakeInsertionSurface> requestInsertionSurfaces) {
    Instant now = options.now();
    Instant eventApiTimestamp = options.eventApiTimestamp();
    Instant viewNow = delay(now, Duration.ofSeconds(index * options.delayBetweenViewInSeconds()));
    Instant viewEventApiTimestamp =
        delay(eventApiTimestamp, Duration.ofSeconds(index * options.delayBetweenViewInSeconds()));
    String viewId = options.viewUuid();
    LogRequest logRequest =
        toCommonFieldsBuilder(request)
            .setTiming(createTiming(viewEventApiTimestamp))
            .addView(
                options
                    .viewTransform()
                    .apply(
                        View.newBuilder()
                            .setViewId(viewId)
                            .setSessionId(sessionId)
                            .setTiming(createTiming(viewEventApiTimestamp))
                            .setContentId(contentId)
                            .setName(viewName)))
            .build();

    if (!isMissingEvent(options.missingViewRate(), viewId, "addViewToHeap")) {
      addToHeap(viewNow, logRequest);
    }
    if (addDeliveryLogs) {
      addToHeap(
          viewNow,
          () -> {
            // TODO - vary up the order.
            List<Content> contents = contentDB.listContent(filters);
            contents =
                contents.subList(
                    0, Math.min(contents.size(), options.responseInsertionsPerRequest()));
            addDeliveryLogsToHeap(logRequest, viewId, copyOf(contents), requestInsertionSurfaces);
          });
      addToHeap(viewNow, () -> addDiagnosticsToHeap(logRequest));
    }
  }

  private void addAutoViewsToHeap(LogRequest request, String sessionId) {
    IntStream.range(0, options.autoViewsPerSession())
        .forEach(i -> addAutoViewToHeap(request, sessionId, i));
  }

  private void addAutoViewToHeap(LogRequest request, String sessionId, int index) {
    Instant now = options.now();
    Instant eventApiTimestamp = options.eventApiTimestamp();
    Instant autoViewNow =
        delay(now, Duration.ofSeconds(index * options.delayBetweenAutoViewInSeconds()));
    Instant autoViewEventApiTimestamp =
        delay(
            eventApiTimestamp, Duration.ofSeconds(index * options.delayBetweenAutoViewInSeconds()));
    String autoViewId = options.autoViewUuid();
    LogRequest logRequest =
        toCommonFieldsBuilder(request)
            .setTiming(createTiming(autoViewEventApiTimestamp))
            .addAutoView(
                options
                    .autoViewTransform()
                    .apply(
                        AutoView.newBuilder()
                            .setAutoViewId(autoViewId)
                            .setSessionId(sessionId)
                            .setTiming(createTiming(autoViewEventApiTimestamp))))
            .build();
    if (!isMissingEvent(options.missingAutoViewRate(), autoViewId, "addAutoViewsToHeap")) {
      addToHeap(autoViewNow, logRequest);
    }
  }

  private void addDeliveryLogsToHeap(
      LogRequest logRequest,
      String viewId,
      ImmutableList<? extends Content> contents,
      ImmutableList<FakeInsertionSurface> requestInsertionSurfaces) {
    IntStream.range(0, options.requestsPerViews())
        .forEach(
            i -> addDeliveryLogToHeap(logRequest, viewId, contents, requestInsertionSurfaces, i));
  }

  // TODO - support SDK fallback SDKs.
  // TODO - change the response insertions in shadow traffic so they are different than SDK response
  // insertions.
  private void addDeliveryLogToHeap(
      LogRequest logRequest,
      String viewId,
      ImmutableList<? extends Content> contents,
      ImmutableList<FakeInsertionSurface> requestInsertionSurfaces,
      int index) {
    boolean redundantInsertions =
        matchesRateOption(options.redundantInsertionRate(), viewId, "addDeliveryLogToHeap");
    int numDeliveryLogs = redundantInsertions ? 2 : 1;
    for (int i = 0; i < numDeliveryLogs; i++) {
      addInnerDeliveryLogToHeap(logRequest, viewId, contents, requestInsertionSurfaces, index);
    }
  }

  private void addInnerDeliveryLogToHeap(
      LogRequest logRequest,
      String viewId,
      ImmutableList<? extends Content> contents,
      ImmutableList<FakeInsertionSurface> requestInsertionSurfaces,
      int index) {
    Instant now = options.now();
    Instant eventApiTimestamp = options.eventApiTimestamp();
    List<InsertedContent> insertedContents = newInsertedContents(contents);
    DeliveryLog baseDeliveryLog =
        createBaseDeliveryLog(
            logRequest,
            now,
            viewId,
            insertedContents,
            requestInsertionSurfaces.get(0).contentType());
    // TODO - fix after merge with recent PR.
    String requestId = baseDeliveryLog.getRequest().getRequestId();
    String clientRequestId = "client-" + baseDeliveryLog.getRequest().getRequestId();
    baseDeliveryLog =
        baseDeliveryLog.toBuilder()
            .setRequest(
                baseDeliveryLog.getRequest().toBuilder().setClientRequestId(clientRequestId))
            .build();
    boolean generateMiniSdkDeliveryLog =
        matchesRateOption(
            options.miniSdkRate(),
            baseDeliveryLog.getRequest().getRequestId(),
            "addDeliveryLogsToHeap");
    DeliveryLog undecoratedDeliveryLog;
    if (generateMiniSdkDeliveryLog) {
      undecoratedDeliveryLog = createSdkMiniDeliveryLog(baseDeliveryLog);
    } else {
      undecoratedDeliveryLog = createProductionDeliveryApiDeliveryLog(baseDeliveryLog);
    }
    DeliveryLog deliveryLog =
        options.deliveryLogTransform().apply(undecoratedDeliveryLog.toBuilder()).build();
    LogRequest.Builder baseBuilder =
        toCommonFieldsBuilder(logRequest).setTiming(createTiming(eventApiTimestamp));
    Instant requestInstant = delay(now, Duration.ofMillis(500 * (index + 1)));

    // Tmp code.
    final LogRequest deliveryLogLogRequest =
        baseBuilder.clone().addDeliveryLog(deliveryLog).build();
    if (!isMissingEvent(options.missingDeliveryLogRate(), requestId, "addDeliveryLogsToHeap1")) {
      addToHeap(requestInstant, deliveryLogLogRequest);
    }
    addToHeap(
        requestInstant,
        () ->
            addImpressionsToHeap(
                deliveryLogLogRequest,
                deliveryLog.getRequest().getRequestId(),
                copyOf(insertedContents),
                requestInsertionSurfaces));

    boolean generateShadowTrafficDeliveryLog =
        generateMiniSdkDeliveryLog
            && matchesRateOption(
                options.shadowTrafficRate(),
                baseDeliveryLog.getRequest().getRequestId(),
                "addDeliveryLogsToHeap2");
    if (generateShadowTrafficDeliveryLog) {
      // We need a new requestId so create a new base.
      DeliveryLog baseShadowTrafficDeliveryLog =
          baseDeliveryLog.toBuilder()
              .setRequest(
                  baseDeliveryLog.getRequest().toBuilder()
                      .setRequestId(options.requestUuid())
                      .setClientRequestId(clientRequestId))
              .build();
      DeliveryLog shadowTrafficDeliveryLog =
          options
              .deliveryLogTransform()
              .apply(createShadowDeliveryApiDeliveryLog(baseShadowTrafficDeliveryLog).toBuilder())
              .build();
      baseBuilder = baseBuilder.addDeliveryLog(shadowTrafficDeliveryLog);
      // No internal LogRequest for shadow traffic.  We want to log it but we don't want to generate
      // child records from that DeliveryLog.
      addToHeap(requestInstant, baseBuilder.build());
    }
  }

  /**
   * Get the response insertions and create impressions for insertionImpressionRate of them. Done as
   * a batch since Impressions are usually logged in batches.
   */
  private void addImpressionsToHeap(
      LogRequest logRequest,
      String requestId,
      ImmutableList<InsertedContent> insertedContents,
      ImmutableList<FakeInsertionSurface> requestInsertionSurfaces) {
    // If we should log redundant impressions, add all of them to the heap with different delays.
    boolean redundantImpressions =
        matchesRateOption(options.redundantImpressionRate(), requestId, "addImpressionsToHeap1");
    int numRedundantImpressions =
        redundantImpressions ? options.maxRedundantImpressionsPerDeliveryLog() : 1;
    int maxImpressions =
        fromRateToCount(
            options.insertionImpressedRate(),
            insertedContents.size(),
            requestId,
            "addImpressionsToHeap2");
    // Sends duplicates of all impressions during this time.
    for (int i = 0; i < numRedundantImpressions; i++) {
      // Only create actions on the last redundant impression.
      boolean createActions = i == numRedundantImpressions - 1;
      // Copy the impressionContent and create new impressionIds.
      List<ImpressedContent> impressedContents =
          newImpressedContent(insertedContents.subList(0, maxImpressions));
      addImpressionsToHeap(
          logRequest,
          copyOf(impressedContents),
          Duration.ofSeconds(2 + i * 2),
          createActions,
          requestInsertionSurfaces);
    }
  }

  /**
   * @param impressedContents impressions are logged for all of the content
   */
  private void addImpressionsToHeap(
      LogRequest logRequest,
      ImmutableList<ImpressedContent> impressedContents,
      Duration delay,
      boolean createActions,
      ImmutableList<FakeInsertionSurface> requestInsertionSurfaces) {
    if (impressedContents.isEmpty()) {
      return;
    }

    // Just floor for now.  This won't result in an accurate rate.
    Instant now = options.now();
    Instant eventApiTimestamp = options.eventApiTimestamp();
    LogRequest.Builder builder =
        toCommonFieldsBuilder(logRequest).setTiming(createTiming(eventApiTimestamp));
    // Add the top insertions as impressions to the LogRequest.
    for (ImpressedContent impressedContent : impressedContents) {
      Impression.Builder impression =
          Impression.newBuilder()
              .setImpressionId(impressedContent.impressionId())
              .setContentId(impressedContent.getContentId())
              .setTiming(createTiming(eventApiTimestamp));
      if (!options.setupForInferredIds()) {
        impression.setInsertionId(impressedContent.getResponseInsertionId());
      }
      if (requestInsertionSurfaces.get(0).logImpressions()) {
        builder.addImpression(options.impressionTransform().apply(impression));
      }
    }
    // For now, just add a couple seconds for the impressions to happen.
    Instant impressionInstant = delay(now, delay);
    // Use the first impressionId as a way to deterministically deal with floor/ceiling.
    String firstImpressionId = impressedContents.get(0).impressionId();
    boolean isMissingEvent =
        builder.getImpressionCount() > 0
            && isMissingEvent(
                options.missingImpressionRate(), firstImpressionId, "addImpressionsToHeap1");
    LogRequest external = builder.build();
    if (!isMissingEvent) {
      addToHeap(impressionInstant, external);
    }
    if (createActions) {
      addToHeap(
          impressionInstant,
          () -> {
            int maxNavigates =
                fromRateToCount(
                    options.impressionNavigateRate(),
                    impressedContents.size(),
                    firstImpressionId,
                    "addImpressionsToHeap2");
            List<ImpressedContent> navigateContents = impressedContents.subList(0, maxNavigates);
            addNavigatesToHeap(
                external, copyOf(navigateContents), copyOf(requestInsertionSurfaces));
          });
    }
  }

  /** The method calls handle the batch since LogRequest might have multiple actions. */
  private void addNavigatesToHeap(
      LogRequest request,
      List<ImpressedContent> navigateContents,
      List<FakeInsertionSurface> requestInsertionSurfaces) {
    if (navigateContents.isEmpty()) {
      return;
    }

    // Just floor for now.  This won't result in an accurate rate.
    Instant now = options.now();
    Instant eventApiTimestamp = options.eventApiTimestamp();
    LogRequest.Builder builder =
        toCommonFieldsBuilder(request).setTiming(createTiming(eventApiTimestamp));
    for (int i = 0; i < navigateContents.size(); i++) {
      ImpressedContent content = navigateContents.get(i);
      String contentId = content.getContentId();
      Action.Builder action =
          Action.newBuilder()
              .setActionId(options.actionUuid())
              .setContentId(contentId)
              .setTiming(createTiming(eventApiTimestamp))
              .setActionType(ActionType.NAVIGATE);
      if (!options.setupForInferredIds()) {
        action.setImpressionId(content.impressionId());
      }
      if (requestInsertionSurfaces.get(0).logNavigates()) {
        builder.addAction(options.navigateTransform().apply(action));
      }
      // Writes the leaf views.
      if (requestInsertionSurfaces.size() == 1 && options.writeProductViews()) {
        addViewToHeap(
            builder.clone().build(),
            "",
            i,
            contentId,
            "Page for " + contentId,
            false,
            ImmutableList.of(),
            ImmutableList.of());
      }
    }

    if (requestInsertionSurfaces.size() == 1) {
      addAddToCartActions(request, navigateContents, eventApiTimestamp, builder);
    }

    Instant actionInstant = delay(now, Duration.ofSeconds(5));
    LogRequest external = builder.build();
    addToHeap(actionInstant, external);

    addToHeap(
        actionInstant,
        () -> {
          String firstImpressionId = navigateContents.get(0).impressionId();
          if (requestInsertionSurfaces.size() == 1) {
            int maxNavigates =
                fromRateToCount(
                    options.navigateCheckoutRate(),
                    navigateContents.size(),
                    firstImpressionId,
                    "addNavigatesToHeap");
            List<ImpressedContent> nextContents = navigateContents.subList(0, maxNavigates);
            addCheckoutsToHeap(external, nextContents);
          } else if (requestInsertionSurfaces.size() > 1) {
            // TODO - pass through sessionId.
            // TODO - pass through a filter for the next request.
            List<FakeInsertionSurface> nextRequestInsertionSurfaces =
                requestInsertionSurfaces.subList(1, requestInsertionSurfaces.size());
            for (int i = 0; i < navigateContents.size(); i++) {
              String contentId = navigateContents.get(i).id();
              addViewToHeap(
                  external,
                  "",
                  i,
                  contentId,
                  "Items for " + contentId,
                  true,
                  // Filters down the content on the View to...
                  ImmutableList.of(
                      // Content matching the same ID field.
                      ContentDBFilter.whereFieldIs(
                          requestInsertionSurfaces.get(0).contentType().id, contentId),
                      // AND matching the desired type.
                      ContentDBFilter.whereFieldIs(
                          Content.TYPE, nextRequestInsertionSurfaces.get(0).contentType().name())),
                  copyOf(nextRequestInsertionSurfaces));
            }
          } else {
            throw new IllegalStateException("requestContentTypes should contain at least one item");
          }
        });
  }

  private void addAddToCartActions(
      LogRequest request,
      List<ImpressedContent> navigateContents,
      Instant eventApiTimestamp,
      LogRequest.Builder builder) {
    String randomSeed = navigateContents.get(0).impressionId();
    int numAddToCart =
        fromRateToCount(
            options.navigateAddToCartRate(), navigateContents.size(), randomSeed, "addToCart");
    List<ImpressedContent> addToCartContents = navigateContents.subList(0, numAddToCart);
    // TODO - add delays for ADD_TO_CART.
    for (ImpressedContent addToCartContent : addToCartContents) {
      // TODO - add cart info to this action.
      builder.addAction(
          Action.newBuilder()
              .setActionId(options.actionUuid())
              .setContentId(addToCartContent.getContentId())
              .setTiming(createTiming(eventApiTimestamp))
              .setActionType(ActionType.ADD_TO_CART)
              .build());

      // Add cart
      Cart cart =
          anyUserIdToCart.getOrDefault(
              getAnyLogUserId(request.getUserInfo()), Cart.getDefaultInstance());
      long quantity = 1 + modAbs(hash(addToCartContent.getContentId()), 4);
      long priceMicrosPerUnit = 1000000 * (1 + modAbs(hash(addToCartContent.getContentId()), 5));
      cart =
          cart.toBuilder()
              .addContents(
                  CartContent.newBuilder()
                      .setContentId(addToCartContent.getContentId())
                      .setQuantity(quantity)
                      .setPricePerUnit(
                          Money.newBuilder()
                              .setCurrencyCode(CurrencyCode.USD)
                              .setAmountMicros(priceMicrosPerUnit)))
              .build();
      anyUserIdToCart.put(getAnyLogUserId(request.getUserInfo()), cart);
    }
  }

  private void addCheckoutsToHeap(LogRequest request, List<ImpressedContent> checkoutContents) {
    if (checkoutContents.isEmpty()) {
      return;
    }
    // Just floor for now.  This won't result in an accurate rate.
    Instant now = options.now();
    Instant eventApiTimestamp = options.eventApiTimestamp();
    LogRequest.Builder builder =
        toCommonFieldsBuilder(request).setTiming(createTiming(eventApiTimestamp));
    for (ImpressedContent content : checkoutContents) {
      Action.Builder action =
          Action.newBuilder()
              .setActionId(options.actionUuid())
              .setContentId(content.getContentId())
              .setTiming(createTiming(eventApiTimestamp))
              .setActionType(ActionType.CHECKOUT);
      if (!options.setupForInferredIds()) {
        action.setImpressionId(content.impressionId());
      }
      Cart cart = anyUserIdToCart.get(getAnyLogUserId(request.getUserInfo()));
      if (cart != null) {
        action.setCart(cart);
      }
      builder.addAction(options.checkoutTransform().apply(action));
    }
    Instant actionInstant = delay(now, Duration.ofSeconds(5));
    LogRequest external = builder.build();
    addToHeap(actionInstant, external);
    addToHeap(
        actionInstant,
        () -> {
          String firstImpressionId = checkoutContents.get(0).impressionId();
          int maxPurchases =
              fromRateToCount(
                  options.checkoutPurchaseRate(),
                  checkoutContents.size(),
                  firstImpressionId,
                  "addCheckoutsToHeap");
          List<ImpressedContent> purchaseContents = checkoutContents.subList(0, maxPurchases);
          addPurchasesToHeap(external, purchaseContents);
        });
  }

  private void addPurchasesToHeap(LogRequest request, List<ImpressedContent> purchaseContents) {
    if (purchaseContents.isEmpty()) {
      return;
    }
    // Just floor for now.  This won't result in an accurate rate.
    Instant now = options.now();
    Instant eventApiTimestamp = options.eventApiTimestamp();
    LogRequest.Builder builder =
        toCommonFieldsBuilder(request).setTiming(createTiming(eventApiTimestamp));
    for (ImpressedContent content : purchaseContents) {
      Action.Builder action =
          Action.newBuilder()
              .setActionId(options.actionUuid())
              .setContentId(content.getContentId())
              .setTiming(createTiming(eventApiTimestamp))
              .setActionType(ActionType.PURCHASE);
      if (!options.setupForInferredIds()) {
        action.setImpressionId(content.impressionId());
      }
      Cart cart = anyUserIdToCart.get(getAnyLogUserId(request.getUserInfo()));
      if (cart != null) {
        action.setCart(cart);
      }
      builder.addAction(options.purchaseTransform().apply(action));
    }
    Instant actionInstant = delay(now, Duration.ofSeconds(5));
    addToHeap(actionInstant, builder.build());
  }

  private void addDiagnosticsToHeap(LogRequest request) {
    Instant now = options.now();
    Instant eventApiTimestamp = options.eventApiTimestamp();
    LogRequest.Builder builder =
        toCommonFieldsBuilder(request).setTiming(createTiming(eventApiTimestamp));
    // For now, only add one Diagnostics to the LogRequest.
    Diagnostics.Builder diagnostics =
        Diagnostics.newBuilder()
            .setTiming(createTiming(eventApiTimestamp))
            .setMobileDiagnostics(MobileDiagnostics.newBuilder().setDeviceIdentifier("abcd-efgh"));
    builder.addDiagnostics(diagnostics);
    Instant diagnosticsInstant = delay(now, Duration.ofSeconds(5));
    addToHeap(diagnosticsInstant, builder.build());
  }

  /** Creates a base DeliveryLog that can be used to generate other types of DeliveryLogs. */
  private DeliveryLog createBaseDeliveryLog(
      LogRequest logRequest,
      Instant eventApiTimestamp,
      String viewId,
      List<InsertedContent> contents,
      ContentType requestContentType) {
    // Usually, each Request+Insertions are it's own LogRequest.
    Request.Builder request = createRequest(viewId, eventApiTimestamp, logRequest.getUserInfo());
    String requestId = request.getRequestId();
    Response.Builder response = Response.newBuilder();

    if (options.insertionMatrixFormat()) {
      addInsertionMatrix(contents, request);
    }
    if (options.insertionFullFormat()) {
      for (int position = 0; position < contents.size(); position++) {
        request.addInsertion(createRequestInsertion(contents.get(position).content()));
      }
    }

    for (int position = 0; position < contents.size(); position++) {
      InsertedContent content = contents.get(position);
      response.addInsertion(
          createResponseInsertion(
              content, requestContentType, requestId, eventApiTimestamp, position));
    }

    return DeliveryLog.newBuilder().setRequest(request).setResponse(response).build();
  }

  private void addInsertionMatrix(List<InsertedContent> contents, Request.Builder request) {
    Set<String> headers = new HashSet<>();
    List<Map<String, Value>> insertionMatrix = new ArrayList<>();
    for (int position = 0; position < contents.size(); position++) {
      Map<String, Value> matrixRow =
          createRequestInsertionMatrixRow(contents.get(position).content());
      insertionMatrix.add(matrixRow);
      headers.addAll(matrixRow.keySet());
    }
    List<String> headerList = headers.stream().sorted().collect(Collectors.toList());
    request.addAllInsertionMatrixHeaders(headerList);
    ListValue.Builder matrixBuilder = request.getInsertionMatrixBuilder();
    for (Map<String, Value> inputRow : insertionMatrix) {
      ListValue.Builder row = ListValue.newBuilder();
      for (String header : headerList) {
        Value value = inputRow.get(header);
        row.addValues(value != null ? value : Value.getDefaultInstance());
      }
      matrixBuilder.addValues(Value.newBuilder().setListValue(row.build()).build());
    }
  }

  private List<InsertedContent> newInsertedContents(List<? extends Content> contents) {
    return contents.stream().map(this::newInsertedContent).collect(Collectors.toList());
  }

  private InsertedContent newInsertedContent(Content content) {
    return InsertedContent.builder()
        .setContent(content)
        .setResponseInsertionId(options.responseInsertionUuid())
        .build();
  }

  private List<ImpressedContent> newImpressedContent(List<InsertedContent> insertedContents) {
    return insertedContents.stream().map(this::newImpressedContent).collect(Collectors.toList());
  }

  private ImpressedContent newImpressedContent(InsertedContent insertedContent) {
    return ImpressedContent.builder()
        .setInsertedContent(insertedContent)
        .setImpressionId(options.impressionUuid())
        .build();
  }

  private ImmutableList<FakeInsertionSurface> getRequestInsertionSurfaces() {
    ImmutableList.Builder<FakeInsertionSurface> builder =
        ImmutableList.<FakeInsertionSurface>builder().add(options.requestInsertionSurface0());
    options.requestInsertionSurface1().ifPresent(surface -> builder.add(surface));
    return builder.build();
  }

  /**
   * Creates a Production Delivery API DeliveryLog from a baseDeliveryLog. It adds execution
   * insertions with a fake feature.
   */
  private DeliveryLog createProductionDeliveryApiDeliveryLog(DeliveryLog baseDeliveryLog) {
    return createDeliveryApiDeliveryLog(
        baseDeliveryLog,
        ClientInfo.TrafficType.PRODUCTION,
        (deliveryLog) -> deliveryLog.getResponse().getInsertionList());
  }

  /**
   * Creates a Shadow Traffic Delivery API DeliveryLog from a baseDeliveryLog. It adds execution
   * insertions with a fake feature.
   */
  private DeliveryLog createShadowDeliveryApiDeliveryLog(DeliveryLog baseDeliveryLog) {
    return createDeliveryApiDeliveryLog(
        baseDeliveryLog,
        ClientInfo.TrafficType.SHADOW,
        // For shadow traffic, we log execution insertions for the request insertions.
        (deliveryLog) -> deliveryLog.getRequest().getInsertionList());
  }

  /**
   * Creates a Delivery API DeliveryLog from a baseDeliveryLog. It adds execution insertions with a
   * fake feature. Supports custom TrafficType arg.
   */
  private DeliveryLog createDeliveryApiDeliveryLog(
      DeliveryLog baseDeliveryLog,
      ClientInfo.TrafficType trafficType,
      Function<DeliveryLog, List<Insertion>> toBaseExecutionInsertions) {
    DeliveryLog.Builder baseDeliveryLogBuilder = baseDeliveryLog.toBuilder();
    Request.Builder requestBuilder = baseDeliveryLogBuilder.getRequestBuilder();
    DeliveryExecution.Builder executionBuilder =
        DeliveryExecution.newBuilder().setExecutionServer(ExecutionServer.API);
    for (Insertion baseExecutionInsertion : toBaseExecutionInsertions.apply(baseDeliveryLog)) {
      long contentId = toContentIdLong(baseExecutionInsertion.getContentId());
      // Fake features based on contentId.
      Insertion executionInsertion =
          baseExecutionInsertion.toBuilder()
              .setFeatureStage(
                  FeatureStage.newBuilder()
                      .setFeatures(Features.newBuilder().putNumeric(100, 0.1f * (contentId % 11))))
              .build();
      executionBuilder.addExecutionInsertion(executionInsertion);
    }
    return baseDeliveryLog.toBuilder()
        .setRequest(
            requestBuilder.setClientInfo(
                ClientInfo.newBuilder()
                    .setClientType(ClientInfo.ClientType.PLATFORM_SERVER)
                    .setTrafficType(trafficType)))
        .setExecution(executionBuilder)
        .build();
  }

  /** Creates a SDK mini-delivery DeliveryLog from a baseDeliveryLog. */
  private DeliveryLog createSdkMiniDeliveryLog(DeliveryLog baseDeliveryLog) {
    DeliveryLog.Builder baseDeliveryLogBuilder = baseDeliveryLog.toBuilder();
    Request.Builder requestBuilder = baseDeliveryLogBuilder.getRequestBuilder();
    DeliveryExecution.Builder executionBuilder =
        DeliveryExecution.newBuilder().setExecutionServer(ExecutionServer.SDK);
    return baseDeliveryLog.toBuilder()
        .setRequest(
            requestBuilder.setClientInfo(
                ClientInfo.newBuilder()
                    .setClientType(ClientInfo.ClientType.PLATFORM_SERVER)
                    .setTrafficType(ClientInfo.TrafficType.PRODUCTION)))
        .setExecution(executionBuilder)
        .build();
  }

  // Hack - Passes in userInfo directly onto Request to force the auth userId onto it.
  // TODO - change this when we build scrubbing appropriately.
  private Request.Builder createRequest(
      String viewId, Instant eventApiTimestamp, UserInfo userInfo) {
    String requestId = options.requestUuid();
    Request.Builder requestBuilder =
        Request.newBuilder()
            .setRequestId(requestId)
            .setUserInfo(userInfo)
            .setTiming(createTiming(eventApiTimestamp));
    if (!options.setupForInferredIds()) requestBuilder.setViewId(viewId);
    // TODO - request insertions.
    return requestBuilder;
  }

  private Insertion.Builder createResponseInsertion(
      InsertedContent content,
      ContentType requestContentType,
      String requestId,
      Instant eventApiTimestamp,
      int position) {
    return options
        .responseInsertionTransform()
        .apply(
            Insertion.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setRequestId(requestId)
                .setInsertionId(content.responseInsertionId())
                .setTiming(createTiming(eventApiTimestamp))
                .setContentId(content.getContentId())
                .setPosition(position));
  }

  private LogRequest.Builder toCommonFieldsBuilder(LogRequest request) {
    return LogRequest.newBuilder().setPlatformId(PLATFORM_ID).setUserInfo(request.getUserInfo());
  }

  private Timing createTiming(Instant timestamp) {
    Timing.Builder builder =
        Timing.newBuilder()
            .setClientLogTimestamp(timestamp.toEpochMilli())
            .setEventApiTimestamp(timestamp.toEpochMilli());
    if (options.logTimestamp()) {
      builder.setLogTimestamp(timestamp.toEpochMilli());
    }
    return builder.build();
  }

  private void addToHeap(Instant time, LogRequest external) {
    int index = nextStateIndex;
    nextStateIndex++;
    stateHeap.add(State.create(time, index, external));
  }

  private void addToHeap(Instant time, Runnable runnable) {
    int index = nextStateIndex;
    nextStateIndex++;
    stateHeap.add(State.create(time, index, runnable));
  }

  // TODO - make some of this logic optional so we can fetch from the DB.
  private Insertion.Builder createRequestInsertion(Content content) {
    Struct.Builder structBuilder = Struct.newBuilder();
    content.requestFields().entrySet().stream()
        // Sort the keys to make the test results reproducible.
        .sorted((e1, e2) -> e1.getKey().compareTo(e2.getKey()))
        .forEach(
            entry -> {
              structBuilder.putFields(
                  entry.getKey(), Value.newBuilder().setStringValue(entry.getValue()).build());
            });
    return Insertion.newBuilder()
        .setContentId(content.contentId())
        .setProperties(Properties.newBuilder().setStruct(structBuilder));
  }

  // This function modifies headers.
  private Map<String, Value> createRequestInsertionMatrixRow(Content content) {
    ImmutableMap.Builder<String, Value> builder = ImmutableMap.builder();
    content
        .requestFields()
        .entrySet()
        .forEach(
            entry -> {
              builder.put(
                  entry.getKey(), Value.newBuilder().setStringValue(entry.getValue()).build());
            });
    builder.put("contentId", Value.newBuilder().setStringValue(content.contentId()).build());
    return builder.build();
  }
}
