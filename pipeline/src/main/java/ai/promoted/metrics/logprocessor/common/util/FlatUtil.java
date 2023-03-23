package ai.promoted.metrics.logprocessor.common.util;

import static ai.promoted.metrics.logprocessor.common.util.StringUtil.isBlank;
import static ai.promoted.metrics.logprocessor.common.util.StringUtil.xxhash;

import ai.promoted.metrics.common.Field;
import ai.promoted.metrics.common.RecordType;
import ai.promoted.metrics.error.LogFunctionName;
import ai.promoted.metrics.error.MismatchError;
import ai.promoted.proto.delivery.DeliveryExecution;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.delivery.Response;
import ai.promoted.proto.delivery.internal.features.AggMetric;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.FlatResponseInsertion;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.JoinedEvent;
import ai.promoted.proto.event.JoinedIdentifiers;
import ai.promoted.proto.event.User;
import ai.promoted.proto.event.View;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.protobuf.GeneratedMessageV3;
import java.util.function.BiConsumer;
import org.apache.flink.util.OutputTag;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// TODO - REFACTOR - split by Joined and Flat.
/** Utilities for JoinedEvent and FlatResponseInsertion. */
public final class FlatUtil {
  public static final long EMPTY_STRING_HASH = -1205034819632174695L;
  private static final Logger LOGGER = LogManager.getLogger(FlatUtil.class);

  private FlatUtil() {}

  public static JoinedEvent setFlatUserAndBuild(
      JoinedEvent event,
      User user,
      BiConsumer<OutputTag<MismatchError>, MismatchError> errorLogger) {
    return setFlatUser(event, user, errorLogger).build();
  }

  public static JoinedEvent.Builder setFlatUser(
      JoinedEvent event,
      User user,
      BiConsumer<OutputTag<MismatchError>, MismatchError> errorLogger) {
    return setFlatUser(event.toBuilder(), user, errorLogger);
  }

  public static JoinedEvent.Builder setFlatUser(
      JoinedEvent.Builder builder,
      User user,
      BiConsumer<OutputTag<MismatchError>, MismatchError> errorLogger) {
    JoinedIdentifiers.Builder idsBuilder = builder.getIdsBuilder();
    User.Builder userBuilder = user.toBuilder();

    JoinValueSetter setter =
        new JoinValueSetter(
            JoinValueSetterOptions.builder()
                .setRecordType(RecordType.USER)
                .setRecordId(user.getUserInfo().getUserId())
                .setLogTimestamp(user.getTiming().getLogTimestamp())
                .setLhsIds(JoinValueSetter.toAvro(builder.getIds()))
                .setLogFunctionName(LogFunctionName.FLAT_UTIL_SET_FLAT_USER_FLAT_EVENT)
                .setErrorLogger(errorLogger)
                .build());

    setter.setValue(
        Field.PLATFORM_ID,
        idsBuilder::setPlatformId,
        idsBuilder.getPlatformId(),
        user.getPlatformId());
    setter.setValue(
        Field.LOG_USER_ID,
        idsBuilder::setLogUserId,
        idsBuilder.getLogUserId(),
        user.getUserInfo().getLogUserId());
    setter.setValue(
        Field.USER_ID,
        idsBuilder::setUserId,
        idsBuilder.getUserId(),
        user.getUserInfo().getUserId());

    userBuilder.clearPlatformId();
    userBuilder.getUserInfoBuilder().clearLogUserId().clearUserId();
    if (isEmpty(userBuilder.getUserInfoBuilder())) {
      userBuilder.clearUserInfo();
    }
    builder.setIds(idsBuilder).setUser(userBuilder);
    return builder;
  }

  public static FlatResponseInsertion setFlatUserAndBuild(
      FlatResponseInsertion flat,
      User user,
      BiConsumer<OutputTag<MismatchError>, MismatchError> errorLogger) {
    return setFlatUser(flat.toBuilder(), user, errorLogger).build();
  }

  // TODO - look at changing the error logging interface to a BiFunction.  Might reduce the memory
  // pressure.
  public static FlatResponseInsertion.Builder setFlatUser(
      FlatResponseInsertion.Builder builder,
      User user,
      BiConsumer<OutputTag<MismatchError>, MismatchError> errorLogger) {
    JoinedIdentifiers.Builder idsBuilder = builder.getIdsBuilder();
    User.Builder userBuilder = user.toBuilder();

    JoinValueSetter setter =
        new JoinValueSetter(
            JoinValueSetterOptions.builder()
                .setRecordType(RecordType.USER)
                .setRecordId(user.getUserInfo().getUserId())
                .setLogTimestamp(user.getTiming().getLogTimestamp())
                .setLhsIds(JoinValueSetter.toAvro(builder.getIds()))
                .setLogFunctionName(LogFunctionName.FLAT_UTIL_SET_FLAT_USER_FLAT_RESPONSE_INSERTION)
                .setErrorLogger(errorLogger)
                .build());

    setter.setValue(
        Field.PLATFORM_ID,
        idsBuilder::setPlatformId,
        idsBuilder.getPlatformId(),
        user.getPlatformId());
    setter.setValue(
        Field.LOG_USER_ID,
        idsBuilder::setLogUserId,
        idsBuilder.getLogUserId(),
        user.getUserInfo().getLogUserId());
    setter.setValue(
        Field.USER_ID,
        idsBuilder::setUserId,
        idsBuilder.getUserId(),
        user.getUserInfo().getUserId());

    userBuilder.clearPlatformId();
    userBuilder.getUserInfoBuilder().clearLogUserId().clearUserId();
    if (isEmpty(userBuilder.getUserInfoBuilder())) {
      userBuilder.clearUserInfo();
    }
    builder.setIds(idsBuilder).setUser(userBuilder);
    return builder;
  }

  public static JoinedEvent.Builder setFlatView(
      JoinedEvent.Builder builder,
      View view,
      BiConsumer<OutputTag<MismatchError>, MismatchError> errorLogger) {
    JoinedIdentifiers.Builder idsBuilder = builder.getIdsBuilder();
    View.Builder viewBuilder = view.toBuilder();

    JoinValueSetter setter =
        new JoinValueSetter(
            JoinValueSetterOptions.builder()
                .setRecordType(RecordType.VIEW)
                .setRecordId(view.getViewId())
                .setLogTimestamp(view.getTiming().getLogTimestamp())
                .setLhsIds(JoinValueSetter.toAvro(builder.getIds()))
                .setLogFunctionName(LogFunctionName.FLAT_UTIL_SET_FLAT_VIEW)
                .setErrorLogger(errorLogger)
                .build());

    setter.setValue(
        Field.PLATFORM_ID,
        idsBuilder::setPlatformId,
        idsBuilder.getPlatformId(),
        view.getPlatformId());
    setter.setValue(
        Field.LOG_USER_ID,
        idsBuilder::setLogUserId,
        idsBuilder.getLogUserId(),
        view.getUserInfo().getLogUserId());
    setter.setValue(
        Field.SESSION_ID, idsBuilder::setSessionId, idsBuilder.getSessionId(), view.getSessionId());
    setter.setValue(Field.VIEW_ID, idsBuilder::setViewId, idsBuilder.getViewId(), view.getViewId());

    viewBuilder = clearRedundantFlatViewFields(viewBuilder);
    // clearSessionId needs to be here since it's not on TinyEvent.
    viewBuilder.clearPlatformId().clearSessionId().clearViewId();
    viewBuilder.getUserInfoBuilder().clearLogUserId();
    if (isEmpty(viewBuilder.getUserInfoBuilder())) {
      viewBuilder.clearUserInfo();
    }
    builder.setIds(idsBuilder).setView(viewBuilder);
    return builder;
  }

  // Excludes primary field.
  public static View.Builder clearRedundantFlatViewFields(View.Builder viewBuilder) {
    viewBuilder.getUserInfoBuilder().clearUserId();
    return viewBuilder;
  }

  public static JoinedEvent.Builder setFlatRequest(
      JoinedEvent.Builder builder,
      Request request,
      BiConsumer<OutputTag<MismatchError>, MismatchError> errorLogger) {
    JoinedIdentifiers.Builder idsBuilder = builder.getIdsBuilder();
    Request.Builder requestBuilder = request.toBuilder();

    JoinValueSetter setter =
        new JoinValueSetter(
            JoinValueSetterOptions.builder()
                .setRecordType(RecordType.DELIVERY_LOG)
                .setRecordId(request.getRequestId())
                .setLogTimestamp(request.getTiming().getLogTimestamp())
                .setLhsIds(JoinValueSetter.toAvro(builder.getIds()))
                .setLogFunctionName(LogFunctionName.FLAT_UTIL_SET_FLAT_REQUEST)
                .setErrorLogger(errorLogger)
                .build());

    setter.setValue(
        Field.PLATFORM_ID,
        idsBuilder::setPlatformId,
        idsBuilder.getPlatformId(),
        request.getPlatformId());
    setter.setValue(
        Field.LOG_USER_ID,
        idsBuilder::setLogUserId,
        idsBuilder.getLogUserId(),
        request.getUserInfo().getLogUserId());
    setter.setValue(
        Field.SESSION_ID,
        idsBuilder::setSessionId,
        idsBuilder.getSessionId(),
        request.getSessionId());
    setter.setValue(
        Field.VIEW_ID, idsBuilder::setViewId, idsBuilder.getViewId(), request.getViewId());
    setter.setValue(
        Field.REQUEST_ID,
        idsBuilder::setRequestId,
        idsBuilder.getRequestId(),
        request.getRequestId());

    requestBuilder = clearRedundantFlatRequestFields(requestBuilder);
    // clearSessionId needs to be here since it's not on TinyEvent.
    requestBuilder.clearPlatformId().clearSessionId().clearRequestId();
    requestBuilder.getUserInfoBuilder().clearLogUserId();
    if (isEmpty(requestBuilder.getUserInfoBuilder())) {
      requestBuilder.clearUserInfo();
    }

    builder.setIds(idsBuilder).setRequest(requestBuilder);
    return builder;
  }

  // Excludes primary field.
  public static Request.Builder clearRedundantFlatRequestFields(Request.Builder requestBuilder) {
    requestBuilder.clearViewId();
    requestBuilder.getUserInfoBuilder().clearUserId();
    requestBuilder.clearInsertion();
    return requestBuilder;
  }

  public static JoinedEvent.Builder setFlatResponse(
      JoinedEvent.Builder builder, Response response) {
    return builder.setResponse(clearRedundantFlatResponseFields(response.toBuilder()));
  }

  // TODO - clear other fields.
  public static Response.Builder clearRedundantFlatResponseFields(
      Response.Builder responseBuilder) {
    return responseBuilder.clearInsertion();
  }

  public static JoinedEvent.Builder setFlatApiExecution(
      JoinedEvent.Builder builder, DeliveryExecution execution) {
    return builder.setApiExecution(clearRedundantFlatExecutionFields(execution.toBuilder()));
  }

  public static JoinedEvent.Builder setFlatSdkExecution(
      JoinedEvent.Builder builder, DeliveryExecution execution) {
    return builder.setSdkExecution(clearRedundantFlatExecutionFields(execution.toBuilder()));
  }

  public static DeliveryExecution.Builder clearRedundantFlatExecutionFields(
      DeliveryExecution.Builder executionBuilder) {
    return executionBuilder.clearExecutionInsertion();
  }

  public static JoinedEvent.Builder setFlatRequestInsertion(
      JoinedEvent.Builder builder,
      Insertion requestInsertion,
      long logTimestamp,
      BiConsumer<OutputTag<MismatchError>, MismatchError> errorLogger) {
    Insertion.Builder requestInsertionBuilder =
        setBaseFlatInsertion(
            builder,
            requestInsertion,
            RecordType.REQUEST_INSERTION,
            logTimestamp,
            false,
            true,
            LogFunctionName.FLAT_UTIL_SET_FLAT_REQUEST_INSERTION,
            errorLogger);
    builder.setRequestInsertion(requestInsertionBuilder);
    return builder;
  }

  public static JoinedEvent.Builder setFlatApiExecutionInsertion(
      JoinedEvent.Builder builder,
      Insertion executionInsertion,
      long logTimestamp,
      BiConsumer<OutputTag<MismatchError>, MismatchError> errorLogger) {
    Insertion.Builder executionInsertionBuilder =
        setBaseFlatInsertion(
            builder,
            executionInsertion,
            RecordType.EXECUTION_INSERTION,
            logTimestamp,
            true,
            false,
            LogFunctionName.FLAT_UTIL_SET_FLAT_API_EXECUTION_INSERTION,
            errorLogger);
    builder.setApiExecutionInsertion(executionInsertionBuilder);
    return builder;
  }

  public static JoinedEvent.Builder setFlatResponseInsertion(
      JoinedEvent.Builder builder,
      Insertion responseInsertion,
      long logTimestamp,
      BiConsumer<OutputTag<MismatchError>, MismatchError> errorLogger) {
    Insertion.Builder responseInsertionBuilder =
        setBaseFlatInsertion(
            builder,
            responseInsertion,
            RecordType.RESPONSE_INSERTION,
            logTimestamp,
            true,
            true,
            LogFunctionName.FLAT_UTIL_SET_FLAT_RESPONSE_INSERTION,
            errorLogger);
    builder.setResponseInsertion(responseInsertionBuilder);
    return builder;
  }

  // TODO - After we override Request.insertion_id in Delivery API, we can change this.
  // https://toil.kitemaker.co/se6ONh-Promoted/hdRZPv-Promoted/items/677
  // If allowDifferentValues is true, we're fine with different values for requestId and
  // insertionId.
  private static Insertion.Builder setBaseFlatInsertion(
      JoinedEvent.Builder builder,
      Insertion insertion,
      RecordType recordType,
      long logTimestamp,
      boolean moveInsertionId,
      boolean logDifferentRequestValues,
      LogFunctionName logFunctionName,
      BiConsumer<OutputTag<MismatchError>, MismatchError> errorLogger) {
    JoinedIdentifiers.Builder idsBuilder = builder.getIdsBuilder();
    Insertion.Builder insertionBuilder = insertion.toBuilder();

    JoinValueSetter setter =
        new JoinValueSetter(
            JoinValueSetterOptions.builder()
                .setRecordType(recordType)
                .setRecordId(insertion.getInsertionId())
                .setLogTimestamp(logTimestamp)
                .setLhsIds(JoinValueSetter.toAvro(builder.getIds()))
                .setLogFunctionName(logFunctionName)
                .setErrorLogger(errorLogger)
                .build());

    setter.setValue(
        Field.PLATFORM_ID,
        idsBuilder::setPlatformId,
        idsBuilder.getPlatformId(),
        insertion.getPlatformId());
    setter.setValue(
        Field.LOG_USER_ID,
        idsBuilder::setLogUserId,
        idsBuilder.getLogUserId(),
        insertion.getUserInfo().getLogUserId());
    setter.setValue(
        Field.SESSION_ID,
        idsBuilder::setSessionId,
        idsBuilder.getSessionId(),
        insertion.getSessionId());
    setter.setValue(
        Field.VIEW_ID, idsBuilder::setViewId, idsBuilder.getViewId(), insertion.getViewId());
    setter.setValue(
        Field.REQUEST_ID,
        idsBuilder::setRequestId,
        idsBuilder.getRequestId(),
        insertion.getRequestId(),
        logDifferentRequestValues);
    if (moveInsertionId) {
      setter.setValue(
          Field.INSERTION_ID,
          idsBuilder::setInsertionId,
          idsBuilder.getInsertionId(),
          insertion.getInsertionId(),
          logDifferentRequestValues);
    }

    clearRedundantFlatInsertionFields(insertionBuilder);
    // clearSessionId needs to be here since it's not on TinyEvent.
    insertionBuilder.clearPlatformId().clearSessionId();
    insertionBuilder.getUserInfoBuilder().clearLogUserId();
    if (moveInsertionId) {
      insertionBuilder.clearInsertionId();
    }
    if (isEmpty(insertionBuilder.getUserInfoBuilder())) {
      insertionBuilder.clearUserInfo();
    }
    builder.setIds(idsBuilder);
    return insertionBuilder;
  }

  // Excludes primary key.
  public static Insertion.Builder clearRedundantFlatInsertionFields(
      Insertion.Builder insertionBuilder) {
    insertionBuilder.clearViewId().clearRequestId();
    insertionBuilder.getUserInfoBuilder().clearUserId();
    return insertionBuilder;
  }

  public static JoinedEvent.Builder setFlatImpression(
      JoinedEvent.Builder builder,
      Impression impression,
      BiConsumer<OutputTag<MismatchError>, MismatchError> errorLogger) {
    JoinedIdentifiers.Builder idsBuilder = builder.getIdsBuilder();
    Impression.Builder impressionBuilder = impression.toBuilder();

    JoinValueSetter setter =
        new JoinValueSetter(
            JoinValueSetterOptions.builder()
                .setRecordType(RecordType.IMPRESSION)
                .setRecordId(impression.getImpressionId())
                .setLogTimestamp(impression.getTiming().getLogTimestamp())
                .setLhsIds(JoinValueSetter.toAvro(builder.getIds()))
                .setLogFunctionName(LogFunctionName.FLAT_UTIL_SET_FLAT_IMPRESSION)
                .setErrorLogger(errorLogger)
                .build());

    setter.setValue(
        Field.PLATFORM_ID,
        idsBuilder::setPlatformId,
        idsBuilder.getPlatformId(),
        impression.getPlatformId());
    setter.setValue(
        Field.LOG_USER_ID,
        idsBuilder::setLogUserId,
        idsBuilder.getLogUserId(),
        impression.getUserInfo().getLogUserId());
    setter.setValue(
        Field.SESSION_ID,
        idsBuilder::setSessionId,
        idsBuilder.getSessionId(),
        impression.getSessionId());
    setter.setValue(
        Field.VIEW_ID, idsBuilder::setViewId, idsBuilder.getViewId(), impression.getViewId());
    setter.setValue(
        Field.REQUEST_ID,
        idsBuilder::setRequestId,
        idsBuilder.getRequestId(),
        impression.getRequestId());
    setter.setValue(
        Field.INSERTION_ID,
        idsBuilder::setInsertionId,
        idsBuilder.getInsertionId(),
        impression.getInsertionId());
    setter.setValue(
        Field.IMPRESSION_ID,
        idsBuilder::setImpressionId,
        idsBuilder.getImpressionId(),
        impression.getImpressionId());

    impressionBuilder = clearRedundantFlatImpressionFields(impressionBuilder);
    // clearSessionId needs to be here since it's not on TinyEvent.
    impressionBuilder.clearPlatformId().clearSessionId().clearImpressionId();
    impressionBuilder.getUserInfoBuilder().clearLogUserId();
    if (isEmpty(impressionBuilder.getUserInfoBuilder())) {
      impressionBuilder.clearUserInfo();
    }
    builder.setIds(idsBuilder).setImpression(impressionBuilder);
    return builder;
  }

  public static Impression.Builder clearRedundantFlatImpressionFields(
      Impression.Builder impressionBuilder) {
    impressionBuilder.clearViewId().clearRequestId().clearInsertionId();
    impressionBuilder.getUserInfoBuilder().clearUserId();
    return impressionBuilder;
  }

  public static JoinedEvent.Builder setFlatAction(
      JoinedEvent.Builder builder,
      Action action,
      BiConsumer<OutputTag<MismatchError>, MismatchError> errorLogger) {
    JoinedIdentifiers.Builder idsBuilder = builder.getIdsBuilder();
    Action.Builder actionBuilder = action.toBuilder();

    JoinValueSetter setter =
        new JoinValueSetter(
            JoinValueSetterOptions.builder()
                .setRecordType(RecordType.ACTION)
                .setRecordId(action.getActionId())
                .setLogTimestamp(action.getTiming().getLogTimestamp())
                .setLhsIds(JoinValueSetter.toAvro(builder.getIds()))
                .setErrorLogger(errorLogger)
                .setLogFunctionName(LogFunctionName.FLAT_UTIL_SET_FLAT_ACTION)
                .build());

    setter.setValue(
        Field.PLATFORM_ID,
        idsBuilder::setPlatformId,
        idsBuilder.getPlatformId(),
        action.getPlatformId());
    setter.setValue(
        Field.LOG_USER_ID,
        idsBuilder::setLogUserId,
        idsBuilder.getLogUserId(),
        action.getUserInfo().getLogUserId());
    setter.setValue(
        Field.SESSION_ID,
        idsBuilder::setSessionId,
        idsBuilder.getSessionId(),
        action.getSessionId());
    setter.setValue(
        Field.VIEW_ID, idsBuilder::setViewId, idsBuilder.getViewId(), action.getViewId());
    setter.setValue(
        Field.REQUEST_ID,
        idsBuilder::setRequestId,
        idsBuilder.getRequestId(),
        action.getRequestId());
    setter.setValue(
        Field.INSERTION_ID,
        idsBuilder::setInsertionId,
        idsBuilder.getInsertionId(),
        action.getInsertionId());
    setter.setValue(
        Field.IMPRESSION_ID,
        idsBuilder::setImpressionId,
        idsBuilder.getImpressionId(),
        action.getImpressionId());
    // action_id doesn't exist in JoinedIdentifiers since actions do not join as a "parent" to
    // another event.

    actionBuilder = clearRedundantFlatActionFields(actionBuilder);
    // clearSessionId needs to be here since it's not on TinyEvent.
    actionBuilder.clearPlatformId().clearSessionId();
    actionBuilder.getUserInfoBuilder().clearLogUserId();
    if (isEmpty(actionBuilder.getUserInfoBuilder())) {
      actionBuilder.clearUserInfo();
    }
    builder.setIds(idsBuilder).setAction(actionBuilder);
    return builder;
  }

  public static Action.Builder clearRedundantFlatActionFields(Action.Builder actionBuilder) {
    actionBuilder.clearViewId().clearRequestId().clearInsertionId().clearImpressionId();
    actionBuilder.getUserInfoBuilder().clearUserId();
    return actionBuilder;
  }

  public static FlatResponseInsertion.Builder createFlatResponseInsertion(
      Iterable<JoinedEvent> joinedImpressions, Iterable<JoinedEvent> joinedActions) {
    // Since flat actions are flat impressions joined with action data, all joined
    // state is consistent between the (flat) impression and action.  So, it's safe
    // to simply merge these "parent" values from either flat events.  Really, under
    // normal conditions, impressions should always exist.  However, if the event
    // time session gap duration is less than the max join interval between
    // impressions and actions, it is possible for impressions to not exist but
    // actions to exist.
    JoinedEvent source =
        Iterables.getFirst(
            joinedImpressions,
            Iterables.getFirst(joinedActions, null /* NPE is ok b/c we should never hit this. */));
    FluentIterable<Impression> impressions =
        FluentIterable.from(joinedImpressions)
            .transform(
                joined -> {
                  // The impression ids are set in JoinedEvent.ids.
                  Impression.Builder builder = joined.toBuilder().getImpressionBuilder();
                  builder.setImpressionId(joined.getIds().getImpressionId());
                  return builder.build();
                });
    FluentIterable<Action> actions =
        FluentIterable.from(joinedActions).transform(JoinedEvent::getAction);

    FlatResponseInsertion.Builder builder =
        FlatResponseInsertion.newBuilder()
            .setIds(source.getIds().toBuilder().clearImpressionId())
            .setTiming(source.getRequest().getTiming())
            .setSessionProfile(source.getSessionProfile())
            .setSession(source.getSession())
            .setView(source.getView())
            .setRequest(source.getRequest())
            .setRequestInsertion(source.getRequestInsertion())
            .setResponse(source.getResponse())
            .setResponseInsertion(source.getResponseInsertion())
            .setApiExecution(source.getApiExecution())
            .setApiExecutionInsertion(source.getApiExecutionInsertion())
            .setSdkExecution(source.getSdkExecution())
            .addAllImpression(impressions)
            .addAllAction(actions);
    if (source.hasHiddenApiRequest()) {
      builder.setHiddenApiRequest(source.getHiddenApiRequest());
    }
    return builder;
  }

  public static long getPlatformId(JoinedEvent event) {
    return event.getIds().getPlatformId() != 0
        ? event.getIds().getPlatformId()
        : event.getUser().getPlatformId() != 0
            ? event.getUser().getPlatformId()
            : event.getView().getPlatformId() != 0
                ? event.getView().getPlatformId()
                : event.getRequest().getPlatformId() != 0
                    ? event.getRequest().getPlatformId()
                    : event.getResponseInsertion().getPlatformId() != 0
                        ? event.getResponseInsertion().getPlatformId()
                        : event.getImpression().getPlatformId() != 0
                            ? event.getImpression().getPlatformId()
                            : event.getAction().getPlatformId() != 0
                                ? event.getAction().getPlatformId()
                                : 0; // mimic proto default value of unit64
  }

  public static String getLogUserId(JoinedEvent event) {
    return !isBlank(event.getIds().getLogUserId())
        ? event.getIds().getLogUserId()
        : !isBlank(event.getUser().getUserInfo().getLogUserId())
            ? event.getUser().getUserInfo().getLogUserId()
            : !isBlank(event.getView().getUserInfo().getLogUserId())
                ? event.getView().getUserInfo().getLogUserId()
                : !isBlank(event.getRequest().getUserInfo().getLogUserId())
                    ? event.getRequest().getUserInfo().getLogUserId()
                    : !isBlank(event.getResponseInsertion().getUserInfo().getLogUserId())
                        ? event.getResponseInsertion().getUserInfo().getLogUserId()
                        : !isBlank(event.getImpression().getUserInfo().getLogUserId())
                            ? event.getImpression().getUserInfo().getLogUserId()
                            : !isBlank(event.getAction().getUserInfo().getLogUserId())
                                ? event.getAction().getUserInfo().getLogUserId()
                                : ""; // mimic proto default value of string
  }

  public static String getUserId(JoinedEvent event) {
    return !isBlank(event.getIds().getUserId())
        ? event.getIds().getUserId()
        : !isBlank(event.getUser().getUserInfo().getUserId())
            ? event.getUser().getUserInfo().getUserId()
            : !isBlank(event.getView().getUserInfo().getUserId())
                ? event.getView().getUserInfo().getUserId()
                : !isBlank(event.getRequest().getUserInfo().getUserId())
                    ? event.getRequest().getUserInfo().getUserId()
                    : !isBlank(event.getResponseInsertion().getUserInfo().getUserId())
                        ? event.getResponseInsertion().getUserInfo().getUserId()
                        : !isBlank(event.getImpression().getUserInfo().getUserId())
                            ? event.getImpression().getUserInfo().getUserId()
                            : !isBlank(event.getAction().getUserInfo().getUserId())
                                ? event.getAction().getUserInfo().getUserId()
                                : ""; // mimic proto default value of string
  }

  public static String getSessionId(JoinedEvent event) {
    return !isBlank(event.getIds().getSessionId())
        ? event.getIds().getSessionId()
        : !isBlank(event.getView().getSessionId())
            ? event.getView().getSessionId()
            : !isBlank(event.getRequest().getSessionId())
                ? event.getRequest().getSessionId()
                : !isBlank(event.getResponseInsertion().getSessionId())
                    ? event.getResponseInsertion().getSessionId()
                    : !isBlank(event.getImpression().getSessionId())
                        ? event.getImpression().getSessionId()
                        : !isBlank(event.getAction().getSessionId())
                            ? event.getAction().getSessionId()
                            : ""; // mimic proto default value of string
  }

  public static String getViewId(JoinedEvent event) {
    return !isBlank(event.getIds().getViewId())
        ? event.getIds().getViewId()
        : !isBlank(event.getView().getViewId())
            ? event.getView().getViewId()
            : !isBlank(event.getRequest().getViewId())
                ? event.getRequest().getViewId()
                : !isBlank(event.getResponseInsertion().getViewId())
                    ? event.getResponseInsertion().getViewId()
                    : !isBlank(event.getImpression().getViewId())
                        ? event.getImpression().getViewId()
                        : !isBlank(event.getAction().getViewId())
                            ? event.getAction().getViewId()
                            : ""; // mimic proto default value of string
  }

  public static String getRequestId(JoinedEvent event) {
    return !isBlank(event.getIds().getRequestId())
        ? event.getIds().getRequestId()
        : !isBlank(event.getRequest().getRequestId())
            ? event.getRequest().getRequestId()
            : !isBlank(event.getResponseInsertion().getRequestId())
                ? event.getResponseInsertion().getRequestId()
                : !isBlank(event.getImpression().getRequestId())
                    ? event.getImpression().getRequestId()
                    : !isBlank(event.getAction().getRequestId())
                        ? event.getAction().getRequestId()
                        : ""; // mimic proto default value of string
  }

  public static String getInsertionId(JoinedEvent event) {
    return !isBlank(event.getIds().getInsertionId())
        ? event.getIds().getInsertionId()
        : !isBlank(event.getResponseInsertion().getInsertionId())
            ? event.getResponseInsertion().getInsertionId()
            : !isBlank(event.getImpression().getInsertionId())
                ? event.getImpression().getInsertionId()
                : !isBlank(event.getAction().getInsertionId())
                    ? event.getAction().getInsertionId()
                    : ""; // mimic proto default value of string
  }

  public static String getImpressionId(JoinedEvent event) {
    return !isBlank(event.getIds().getImpressionId())
        ? event.getIds().getImpressionId()
        : !isBlank(event.getImpression().getImpressionId())
            ? event.getImpression().getImpressionId()
            : !isBlank(event.getAction().getImpressionId())
                ? event.getAction().getImpressionId()
                : ""; // mimic proto default value of string
  }

  public static String getContentId(JoinedEvent event) {
    return !isBlank(event.getResponseInsertion().getContentId())
        ? event.getResponseInsertion().getContentId()
        : !isBlank(event.getApiExecutionInsertion().getContentId())
            ? event.getApiExecutionInsertion().getContentId()
            // Skipping requestInsertions since we'll currently always have responseInsertions or
            // executionInsertions.
            : !isBlank(event.getImpression().getContentId())
                ? event.getImpression().getContentId()
                : !isBlank(event.getAction().getContentId())
                    ? event.getAction().getContentId()
                    : ""; // mimic proto default value of string
  }

  public static long getLogTimestamp(JoinedEvent event) {
    return event.getTiming().getLogTimestamp() != 0
        ? event.getTiming().getLogTimestamp()
        // We order things from the expected late to earlier event timings.
        : event.getAction().getTiming().getLogTimestamp() != 0
            ? event.getAction().getTiming().getLogTimestamp()
            : event.getImpression().getTiming().getLogTimestamp() != 0
                ? event.getImpression().getTiming().getLogTimestamp()
                : event.getApiExecutionInsertion().getTiming().getLogTimestamp() != 0
                    ? event.getApiExecutionInsertion().getTiming().getLogTimestamp()
                    : event.getResponseInsertion().getTiming().getLogTimestamp() != 0
                        ? event.getResponseInsertion().getTiming().getLogTimestamp()
                        // TODO - set more timings.
                        : event.getRequest().getTiming().getLogTimestamp() != 0
                            ? event.getRequest().getTiming().getLogTimestamp()
                            : event.getView().getTiming().getLogTimestamp() != 0
                                ? event.getView().getTiming().getLogTimestamp()
                                : event.getSession().getTiming().getLogTimestamp() != 0
                                    ? event.getSession().getTiming().getLogTimestamp()
                                    : event.getUser().getTiming().getLogTimestamp() != 0
                                        ? event.getUser().getTiming().getLogTimestamp()
                                        : 0; // mimic proto default value of long
  }

  public static long getEventApiTimestamp(JoinedEvent event) {
    // We order things from the expected late to earlier event timings.
    return event.getAction().getTiming().getEventApiTimestamp() != 0
        ? event.getAction().getTiming().getEventApiTimestamp()
        : event.getImpression().getTiming().getEventApiTimestamp() != 0
            ? event.getImpression().getTiming().getEventApiTimestamp()
            : event.getResponseInsertion().getTiming().getEventApiTimestamp() != 0
                ? event.getResponseInsertion().getTiming().getEventApiTimestamp()
                : event.getRequest().getTiming().getEventApiTimestamp() != 0
                    ? event.getRequest().getTiming().getEventApiTimestamp()
                    : event.getView().getTiming().getEventApiTimestamp() != 0
                        ? event.getView().getTiming().getEventApiTimestamp()
                        : event.getSession().getTiming().getEventApiTimestamp() != 0
                            ? event.getSession().getTiming().getEventApiTimestamp()
                            : event.getUser().getTiming().getEventApiTimestamp() != 0
                                ? event.getUser().getTiming().getEventApiTimestamp()
                                : 0; // mimic proto default value of long
  }

  // TODO - restructure JoinedEvent to have a merged UserInfo.
  public static boolean hasIgnoreUsage(JoinedEvent event) {
    // Just check the records that can be logged from the client.
    return event.getAction().getUserInfo().getIgnoreUsage()
        || event.getImpression().getUserInfo().getIgnoreUsage()
        || event.getRequest().getUserInfo().getIgnoreUsage()
        || event.getView().getUserInfo().getIgnoreUsage()
        || event.getSession().getUserInfo().getIgnoreUsage()
        || event.getUser().getUserInfo().getIgnoreUsage();
  }

  // TODO - restructure JoinedEvent to have a merged UserInfo.
  public static boolean hasIgnoreUsage(FlatResponseInsertion event) {
    // Just check the records that can be logged from the client.
    return event.getActionList().stream().anyMatch(action -> action.getUserInfo().getIgnoreUsage())
        || event.getImpressionList().stream()
            .anyMatch(action -> action.getUserInfo().getIgnoreUsage())
        || event.getRequest().getUserInfo().getIgnoreUsage()
        || event.getView().getUserInfo().getIgnoreUsage()
        || event.getSession().getUserInfo().getIgnoreUsage()
        || event.getUser().getUserInfo().getIgnoreUsage();
  }

  /**
   * Gets the action type string from a JoinedEvent.
   *
   * <p>Custom actions' custom string values are returned.
   */
  public static String getActionString(JoinedEvent event) {
    if (!event.hasAction()) {
      return ActionType.UNKNOWN_ACTION_TYPE.toString();
    }
    ActionType type = event.getAction().getActionType();
    if (ActionType.CUSTOM_ACTION_TYPE.equals(type)) {
      return event.getAction().getCustomActionType();
    } else {
      return type.toString();
    }
  }

  /** Gets the counter aggregated value from a JoinedEvent. */
  public static AggMetric getAggMetricValue(JoinedEvent event) {
    if (!event.hasAction() && event.hasImpression()) {
      return AggMetric.COUNT_IMPRESSION;
    }
    ActionType type = event.getAction().getActionType();
    switch (type) {
      case UNRECOGNIZED:
      case CUSTOM_ACTION_TYPE:
        break;
      default:
        AggMetric metric = AggMetric.forNumber(type.getNumber() << 5);
        if (metric != null) return metric;
    }
    return AggMetric.UNKNOWN_AGGREGATE;
  }

  /** Gets the query string from a JoinedEvent. */
  public static String getQueryString(JoinedEvent event) {
    return event.getRequest().getSearchQuery();
  }

  /** Gets the lowercased query string from a JoinedEvent. */
  public static String getQueryStringLowered(JoinedEvent event) {
    return event.getRequest().getSearchQuery().toLowerCase();
  }

  /**
   * Gets the hashed query string value as a long from a JoinedEvent.
   *
   * <p>This is not compatible with our go-hashlib implementation. It MUST be compatible with
   * cespare/xxhash's Sum64String golang impl.
   */
  public static long getQueryHash(JoinedEvent event) {
    // TODO: do we need to normalize the query string further?
    return xxhash(getQueryStringLowered(event));
  }

  /**
   * Gets the hashed query string value as a long from a JoinedEvent.
   *
   * <p>This is not compatible with our go-hashlib implementation. It MUST be compatible with
   * cespare/xxhash's Sum64String golang impl.
   */
  public static long getQueryHash(String s) {
    // TODO: do we need to normalize the query string further?
    return xxhash(s);
  }

  /**
   * Gets the hashed query string value as a hex string from a JoinedEvent.
   *
   * <p>Note, this may return len(string) < 16 characters. It should be interpreted as a full 64bit
   * hex value though.
   */
  public static String getQueryHashHex(JoinedEvent event) {
    return Long.toHexString(getQueryHash(event));
  }

  private static <T extends GeneratedMessageV3.Builder> boolean isEmpty(T builder) {
    return builder.getAllFields().isEmpty();
  }
  ;
}
