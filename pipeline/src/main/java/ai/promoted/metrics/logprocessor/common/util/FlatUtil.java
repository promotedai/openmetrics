package ai.promoted.metrics.logprocessor.common.util;

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
import ai.promoted.proto.event.AttributedAction;
import ai.promoted.proto.event.FlatResponseInsertion;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.JoinedIdentifiers;
import ai.promoted.proto.event.JoinedImpression;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.protobuf.GeneratedMessageV3;
import java.util.function.Consumer;

// TODO - REFACTOR - split by Joined and Flat.
/** Utilities for JoinedEvent and FlatResponseInsertion. */
public final class FlatUtil {
  public static final long EMPTY_STRING_HASH = -1205034819632174695L;

  private FlatUtil() {}

  public static JoinedImpression.Builder setFlatRequest(
      JoinedImpression.Builder builder, Request request, Consumer<MismatchError> errorLogger) {
    JoinedIdentifiers.Builder idsBuilder = builder.getIdsBuilder();
    Request.Builder requestBuilder = request.toBuilder();

    JoinValueSetter setter =
        new JoinValueSetter(
            JoinValueSetterOptions.builder()
                .setRecordType(RecordType.DELIVERY_LOG)
                .setRecordId(request.getRequestId())
                .setEventApiTimestamp(request.getTiming().getEventApiTimestamp())
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
        Field.ANON_USER_ID,
        idsBuilder::setAnonUserId,
        idsBuilder.getAnonUserId(),
        request.getUserInfo().getAnonUserId());
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
    requestBuilder.getUserInfoBuilder().clearAnonUserId().clearLogUserId();
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

  public static JoinedImpression.Builder setFlatResponse(
      JoinedImpression.Builder builder, Response response) {
    return builder.setResponse(clearRedundantFlatResponseFields(response.toBuilder()));
  }

  // TODO - clear other fields.
  public static Response.Builder clearRedundantFlatResponseFields(
      Response.Builder responseBuilder) {
    return responseBuilder.clearInsertion();
  }

  public static JoinedImpression.Builder setFlatApiExecution(
      JoinedImpression.Builder builder, DeliveryExecution execution) {
    return builder.setApiExecution(clearRedundantFlatExecutionFields(execution.toBuilder()));
  }

  public static JoinedImpression.Builder setFlatSdkExecution(
      JoinedImpression.Builder builder, DeliveryExecution execution) {
    return builder.setSdkExecution(clearRedundantFlatExecutionFields(execution.toBuilder()));
  }

  public static DeliveryExecution.Builder clearRedundantFlatExecutionFields(
      DeliveryExecution.Builder executionBuilder) {
    return executionBuilder.clearExecutionInsertion();
  }

  public static JoinedImpression.Builder setFlatRequestInsertion(
      JoinedImpression.Builder builder,
      Insertion requestInsertion,
      long eventApiTimestamp,
      Consumer<MismatchError> errorLogger) {
    Insertion.Builder requestInsertionBuilder =
        setBaseFlatInsertion(
            builder,
            requestInsertion,
            RecordType.REQUEST_INSERTION,
            eventApiTimestamp,
            false,
            true,
            LogFunctionName.FLAT_UTIL_SET_FLAT_REQUEST_INSERTION,
            errorLogger);
    builder.setRequestInsertion(requestInsertionBuilder);
    return builder;
  }

  public static JoinedImpression.Builder setFlatApiExecutionInsertion(
      JoinedImpression.Builder builder,
      Insertion executionInsertion,
      long eventApiTimestamp,
      Consumer<MismatchError> errorLogger) {
    Insertion.Builder executionInsertionBuilder =
        setBaseFlatInsertion(
            builder,
            executionInsertion,
            RecordType.EXECUTION_INSERTION,
            eventApiTimestamp,
            true,
            false,
            LogFunctionName.FLAT_UTIL_SET_FLAT_API_EXECUTION_INSERTION,
            errorLogger);
    builder.setApiExecutionInsertion(executionInsertionBuilder);
    return builder;
  }

  public static JoinedImpression.Builder setFlatResponseInsertion(
      JoinedImpression.Builder builder,
      Insertion responseInsertion,
      long eventApiTimestamp,
      Consumer<MismatchError> errorLogger) {
    Insertion.Builder responseInsertionBuilder =
        setBaseFlatInsertion(
            builder,
            responseInsertion,
            RecordType.RESPONSE_INSERTION,
            eventApiTimestamp,
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
      JoinedImpression.Builder builder,
      Insertion insertion,
      RecordType recordType,
      long eventApiTimestamp,
      boolean moveInsertionId,
      boolean logDifferentRequestValues,
      LogFunctionName logFunctionName,
      Consumer<MismatchError> errorLogger) {
    JoinedIdentifiers.Builder idsBuilder = builder.getIdsBuilder();
    Insertion.Builder insertionBuilder = insertion.toBuilder();

    JoinValueSetter setter =
        new JoinValueSetter(
            JoinValueSetterOptions.builder()
                .setRecordType(recordType)
                .setRecordId(insertion.getInsertionId())
                .setEventApiTimestamp(eventApiTimestamp)
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
        Field.ANON_USER_ID,
        idsBuilder::setAnonUserId,
        idsBuilder.getAnonUserId(),
        insertion.getUserInfo().getAnonUserId());
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
    insertionBuilder.getUserInfoBuilder().clearAnonUserId().clearLogUserId();
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

  public static JoinedImpression.Builder setFlatImpression(
      JoinedImpression.Builder builder,
      Impression impression,
      Consumer<MismatchError> errorLogger) {
    JoinedIdentifiers.Builder idsBuilder = builder.getIdsBuilder();
    Impression.Builder impressionBuilder = impression.toBuilder();

    JoinValueSetter setter =
        new JoinValueSetter(
            JoinValueSetterOptions.builder()
                .setRecordType(RecordType.IMPRESSION)
                .setRecordId(impression.getImpressionId())
                .setEventApiTimestamp(impression.getTiming().getEventApiTimestamp())
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
        Field.ANON_USER_ID,
        idsBuilder::setAnonUserId,
        idsBuilder.getAnonUserId(),
        impression.getUserInfo().getAnonUserId());
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
    impressionBuilder.getUserInfoBuilder().clearAnonUserId().clearLogUserId();
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

  public static Action.Builder clearRedundantFlatActionFields(Action.Builder actionBuilder) {
    actionBuilder.clearViewId().clearRequestId().clearInsertionId().clearImpressionId();
    actionBuilder.getUserInfoBuilder().clearUserId();
    return actionBuilder;
  }

  public static FlatResponseInsertion.Builder createFlatResponseInsertion(
      Iterable<JoinedImpression> joinedImpressions, Iterable<AttributedAction> attributedActions) {
    // Since flat actions are flat impressions joined with action data, all joined
    // state is consistent between the (flat) impression and action.  So, it's safe
    // to simply merge these "parent" values from either flat events.  Really, under
    // normal conditions, impressions should always exist.  However, if the event
    // time session gap duration is less than the max join interval between
    // impressions and actions, it is possible for impressions to not exist but
    // actions to exist.

    JoinedImpression anyImpression = Iterables.getFirst(joinedImpressions, null);
    AttributedAction anyAction = Iterables.getFirst(attributedActions, null);
    Preconditions.checkArgument(
        anyImpression != null || anyAction != null,
        "createFlatResponseInsertion needs at least one impression or action.  This indicates a code bug in the pipeline");

    // Currently, there must be a JoinedImpression from either the joinedImpressions stream or
    // attributedActions stream.  Eventually, this will change.
    JoinedImpression firstImpression =
        anyImpression != null ? anyImpression : anyAction.getTouchpoint().getJoinedImpression();

    FlatResponseInsertion.Builder builder =
        FlatResponseInsertion.newBuilder()
            .setIds(firstImpression.getIds().toBuilder().clearImpressionId())
            .setTiming(firstImpression.getRequest().getTiming())
            .setRequest(firstImpression.getRequest())
            .setRequestInsertion(firstImpression.getRequestInsertion())
            .setResponse(firstImpression.getResponse())
            .setResponseInsertion(firstImpression.getResponseInsertion())
            .setApiExecution(firstImpression.getApiExecution())
            .setApiExecutionInsertion(firstImpression.getApiExecutionInsertion())
            .setSdkExecution(firstImpression.getSdkExecution());
    builder.getTimingBuilder().setProcessingTimestamp(TrackingUtil.getProcessingTime());
    if (firstImpression.hasHiddenApiRequest()) {
      builder.setHiddenApiRequest(firstImpression.getHiddenApiRequest());
    }
    builder.addAllImpression(
        FluentIterable.from(joinedImpressions)
            .transform(
                joined ->
                    // Get the `Impression` field and `JoinedImpression.ids.impression_id` back
                    // to the `Impression` record.
                    joined.toBuilder()
                        .getImpressionBuilder()
                        .setImpressionId(joined.getIds().getImpressionId())
                        .build()));

    for (AttributedAction attributedAction : attributedActions) {
      builder
          .addAttributedActionBuilder()
          .setAction(attributedAction.getAction())
          .setAttribution(attributedAction.getAttribution());
    }

    return builder;
  }

  public static long getEventApiTimestamp(JoinedImpression event) {
    // We order things from the expected late to earlier event timings.
    return event.getTiming().getEventApiTimestamp();
  }

  // TODO - restructure JoinedEvent to have a merged UserInfo.
  public static boolean hasIgnoreUsage(JoinedImpression event) {
    // Just check the records that can be logged from the client.
    return event.getImpression().getUserInfo().getIgnoreUsage()
        || event.getRequest().getUserInfo().getIgnoreUsage();
  }

  // TODO - restructure JoinedEvent to have a merged UserInfo.
  public static boolean hasIgnoreUsage(AttributedAction attributedAction) {
    // Just check the records that can be logged from the client.
    return attributedAction.getAction().getUserInfo().getIgnoreUsage()
        || hasIgnoreUsage(attributedAction.getTouchpoint().getJoinedImpression());
  }

  // TODO - restructure JoinedEvent to have a merged UserInfo.
  public static boolean hasIgnoreUsage(FlatResponseInsertion event) {
    // Just check the records that can be logged from the client.
    return event.getAttributedActionList().stream()
            .anyMatch(
                attributedAction -> attributedAction.getAction().getUserInfo().getIgnoreUsage())
        || event.getImpressionList().stream()
            .anyMatch(impression -> impression.getUserInfo().getIgnoreUsage())
        || event.getRequest().getUserInfo().getIgnoreUsage();
  }

  public static String getContentIdPreferAction(AttributedAction attributedAction) {
    String contentId = attributedAction.getAction().getSingleCartContent().getContentId();
    if (!contentId.isEmpty()) {
      return contentId;
    }
    contentId = attributedAction.getAction().getContentId();
    if (!contentId.isEmpty()) {
      return contentId;
    }
    return attributedAction
        .getTouchpoint()
        .getJoinedImpression()
        .getResponseInsertion()
        .getContentId();
  }

  /**
   * Gets the action type string from a JoinedEvent.
   *
   * <p>Custom actions' custom string values are returned.
   */
  public static String getActionString(AttributedAction event) {
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
  public static AggMetric getAggMetricValue(AttributedAction event) {
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
  public static String getQueryStringLowered(JoinedImpression impression) {
    return getQueryStringLowered(impression.getRequest());
  }

  /** Gets the query string from an AttributedAction. */
  public static String getQueryStringLowered(AttributedAction event) {
    return getQueryStringLowered(event.getTouchpoint().getJoinedImpression().getRequest());
  }

  /** Gets the lowercased query string from a Request. */
  public static String getQueryStringLowered(Request request) {
    return request.getSearchQuery().toLowerCase();
  }

  /**
   * Gets the hashed query string value as a long from a JoinedEvent.
   *
   * <p>This is not compatible with our go-hashlib implementation. It MUST be compatible with
   * cespare/xxhash's Sum64String golang impl.
   */
  public static long getQueryHash(Request request) {
    // TODO: do we need to normalize the query string further?
    return xxhash(getQueryStringLowered(request));
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
  public static String getQueryHashHex(Request request) {
    return Long.toHexString(getQueryHash(request));
  }

  private static <T extends GeneratedMessageV3.Builder> boolean isEmpty(T builder) {
    return builder.getAllFields().isEmpty();
  }
}
