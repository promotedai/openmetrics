package ai.promoted.metrics.logprocessor.common.util;

import ai.promoted.proto.delivery.DeliveryExecution;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.delivery.Response;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.CombinedDeliveryLog;
import ai.promoted.proto.event.FlatResponseInsertion;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.JoinedIdentifiers;
import ai.promoted.proto.event.JoinedImpression;
import ai.promoted.proto.event.TinyAction;
import ai.promoted.proto.event.TinyActionPath;
import ai.promoted.proto.event.TinyAttributedAction;
import ai.promoted.proto.event.TinyCommonInfo;
import ai.promoted.proto.event.TinyDeliveryLog;
import ai.promoted.proto.event.TinyImpression;
import ai.promoted.proto.event.TinyInsertion;
import ai.promoted.proto.event.TinyInsertionCore;
import ai.promoted.proto.event.TinyJoinedImpression;
import ai.promoted.proto.event.TinyTouchpoint;
import ai.promoted.proto.event.UnnestedTinyAction;
import ai.promoted.proto.event.UnnestedTinyJoinedImpression;
import ai.promoted.proto.event.User;
import ai.promoted.proto.event.View;
import com.google.auto.value.AutoValue;
import com.google.auto.value.extension.memoized.Memoized;
import com.google.common.collect.ImmutableSet;
import java.io.Serializable;
import java.util.List;
import java.util.Set;

// TODO - make this an interface and then have specific impls for (1) empty, (2) all (for tests) and
// (3) specific-fields.
/** Used to produce extra logging for specific log record IDs. */
@AutoValue
public abstract class DebugIds implements Serializable {
  private static final long serialVersionUID = 1L;

  // Set of userIds substrings to log more flatten event debug logs. Acts as an OR with other debug
  // fields. Used to track down missing flattened events.
  abstract Set<String> userIds();

  // Set of anonUserIds substrings to log more flatten event debug logs. Acts as an OR with other
  // debug fields. Used to track down missing flattened events.
  abstract Set<String> anonUserIds();

  // Set of sessionIds substrings to log more flatten event debug logs. Acts as an OR with other
  // debug fields. Used to track down missing flattened events.
  abstract Set<String> sessionIds();

  // Set of viewIds substrings to log more flatten event debug logs. Acts as an OR with other debug
  // fields. Used to track down missing flattened events.
  abstract Set<String> viewIds();

  // Set of requestIds substrings to log more flatten event debug logs. Acts as an OR with other
  // debug fields. Used to track down missing flattened events.
  abstract Set<String> requestIds();

  // Set of insertionIds substrings to log more flatten event debug logs. Acts as an OR with other
  // debug fields. Used to track down missing flattened events.
  abstract Set<String> insertionIds();

  // Set of impressionIds substrings to log more flatten event debug logs. Acts as an OR with other
  // debug fields. Used to track down missing flattened events.
  abstract Set<String> impressionIds();

  // Set of actionIds substrings to log more flatten event debug logs. Acts as an OR with other
  // debug fields. Used to track down missing flattened events.
  abstract Set<String> actionIds();

  public static Builder builder() {
    return new AutoValue_DebugIds.Builder()
        .setUserIds(ImmutableSet.of())
        .setAnonUserIds(ImmutableSet.of())
        .setSessionIds(ImmutableSet.of())
        .setViewIds(ImmutableSet.of())
        .setRequestIds(ImmutableSet.of())
        .setInsertionIds(ImmutableSet.of())
        .setImpressionIds(ImmutableSet.of())
        .setActionIds(ImmutableSet.of());
  }

  // Return an empty DebugIds (good for a default).
  public static DebugIds empty() {
    return builder().build();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setUserIds(Set<String> userIds);

    public abstract Builder setAnonUserIds(Set<String> anonUserIds);

    public abstract Builder setSessionIds(Set<String> sessionIds);

    public abstract Builder setViewIds(Set<String> viewIds);

    public abstract Builder setRequestIds(Set<String> requestIds);

    public abstract Builder setInsertionIds(Set<String> insertionIds);

    public abstract Builder setImpressionIds(Set<String> impressionIds);

    public abstract Builder setActionIds(Set<String> actionIds);

    abstract Set<String> userIds();

    abstract Set<String> anonUserIds();

    abstract Set<String> sessionIds();

    abstract Set<String> viewIds();

    abstract Set<String> requestIds();

    abstract Set<String> insertionIds();

    abstract Set<String> impressionIds();

    abstract Set<String> actionIds();

    public abstract DebugIds build();
  }

  @Memoized
  public boolean hasAnyIds() {
    return !userIds().isEmpty()
        || !anonUserIds().isEmpty()
        || !sessionIds().isEmpty()
        || !viewIds().isEmpty()
        || !requestIds().isEmpty()
        || !insertionIds().isEmpty()
        || !impressionIds().isEmpty()
        || !actionIds().isEmpty();
  }

  public boolean matchesViewId(String viewId) {
    // TODO - I don't know if having a hasAnyIds is faster or slower.  These sets are usually empty
    // in prod.
    return matches(viewIds(), viewId);
  }

  public boolean matchesRequestId(String requestId) {
    return matches(requestIds(), requestId);
  }

  public boolean matchesInsertionId(String insertionId) {
    return matches(insertionIds(), insertionId);
  }

  public boolean matchesImpressionId(String impressionId) {
    return matches(impressionIds(), impressionId);
  }

  public boolean matchesActionId(String actionId) {
    return matches(actionIds(), actionId);
  }

  public boolean matches(TinyTouchpoint event) {
    if (!hasAnyIds()) {
      return false;
    }
    return matches(event.getJoinedImpression());
  }

  public boolean matches(TinyAttributedAction action) {
    if (!hasAnyIds()) {
      return false;
    }
    return matches(action.getAction()) || matches(action.getTouchpoint());
  }

  public boolean matches(TinyJoinedImpression joinedImpression) {
    if (!hasAnyIds()) {
      return false;
    }
    return matches(joinedImpression.getInsertion()) || matches(joinedImpression.getImpression());
  }

  public boolean matches(UnnestedTinyJoinedImpression unnested) {
    if (!hasAnyIds()) {
      return false;
    }
    return matches(unnested.getJoinedImpression());
  }

  public boolean matches(JoinedImpression impression) {
    if (!hasAnyIds()) {
      return false;
    }
    JoinedIdentifiers ids = impression.getIds();
    return matches(userIds(), ids.getUserId())
        || matches(anonUserIds(), ids.getAnonUserId())
        || matches(sessionIds(), ids.getSessionId())
        || matches(viewIds(), ids.getViewId())
        || matches(requestIds(), ids.getRequestId())
        || matches(insertionIds(), ids.getInsertionId())
        || matches(impressionIds(), ids.getImpressionId());
  }

  public boolean matches(TinyCommonInfo common) {
    if (!hasAnyIds()) {
      return false;
    }
    return matches(anonUserIds(), common.getAnonUserId());
  }

  public boolean matches(TinyDeliveryLog deliveryLog) {
    if (!hasAnyIds()) {
      return false;
    }
    return matches(deliveryLog.getCommon())
        // TODO - decide if we'll add sessionId.
        || matches(viewIds(), deliveryLog.getViewId())
        // TODO - decide if we want autoViewId.
        || matches(requestIds(), deliveryLog.getRequestId())
        || deliveryLog.getResponseInsertionList().stream().anyMatch(this::matches);
  }

  private boolean matches(TinyInsertionCore insertion) {
    if (!hasAnyIds()) {
      return false;
    }
    return matches(insertionIds(), insertion.getInsertionId());
  }

  public boolean matches(TinyInsertion insertion) {
    if (!hasAnyIds()) {
      return false;
    }
    return matches(insertion.getCommon())
        || matches(viewIds(), insertion.getViewId())
        || matches(requestIds(), insertion.getRequestId())
        || matches(insertion.getCore());
  }

  public boolean matches(TinyImpression impression) {
    if (!hasAnyIds()) {
      return false;
    }
    return matches(impression.getCommon())
        || matches(viewIds(), impression.getViewId())
        || matches(requestIds(), impression.getRequestId())
        || matches(insertionIds(), impression.getInsertionId())
        || matches(impressionIds(), impression.getImpressionId());
  }

  public boolean matches(TinyAction action) {
    if (!hasAnyIds()) {
      return false;
    }
    return matches(action.getCommon())
        || matches(viewIds(), action.getViewId())
        || matches(requestIds(), action.getRequestId())
        || matches(insertionIds(), action.getInsertionId())
        || matches(impressionIds(), action.getImpressionId())
        || matches(actionIds(), action.getActionId());
  }

  public boolean matches(UnnestedTinyAction unnested) {
    if (!hasAnyIds()) {
      return false;
    }
    return matches(unnested.getAction());
  }

  public boolean matches(TinyActionPath actionPath) {
    if (!hasAnyIds()) {
      return false;
    }
    return matches(actionPath.getAction())
        || actionPath.getTouchpointsList().stream().anyMatch(this::matches);
  }

  private boolean matches(Set<String> idSubstrings, String id) {
    return !idSubstrings.isEmpty()
        && idSubstrings.stream().anyMatch(substring -> id.contains(substring));
  }

  public boolean matches(FlatResponseInsertion flatInsertion) {
    if (!hasAnyIds()) {
      return false;
    }
    JoinedIdentifiers ids = flatInsertion.getIds();
    return matches(userIds(), ids.getUserId())
        || matches(anonUserIds(), ids.getAnonUserId())
        || matches(sessionIds(), ids.getSessionId())
        || matches(viewIds(), ids.getViewId())
        || matches(requestIds(), ids.getRequestId())
        || matches(insertionIds(), ids.getInsertionId());
  }

  public boolean matches(String id) {
    if (!hasAnyIds()) {
      return false;
    }
    return matches(userIds(), id)
        || matches(anonUserIds(), id)
        || matches(sessionIds(), id)
        || matches(viewIds(), id)
        || matches(requestIds(), id)
        || matches(insertionIds(), id)
        || matches(impressionIds(), id)
        // ActionId is not on JoinedEvent so we need to check the message.
        || matches(actionIds(), id);
  }

  public boolean matches(DeliveryLog deliveryLog) {
    if (!hasAnyIds()) {
      return false;
    }
    return matches(deliveryLog.getRequest())
        || matches(deliveryLog.getExecution())
        || matches(deliveryLog.getResponse());
  }

  public boolean matches(CombinedDeliveryLog combinedDeliveryLog) {
    if (!hasAnyIds()) {
      return false;
    }
    return (combinedDeliveryLog.hasSdk() && matches(combinedDeliveryLog.getSdk()))
        || (combinedDeliveryLog.hasApi() && matches(combinedDeliveryLog.getApi()));
  }

  public boolean matches(User user) {
    if (!hasAnyIds()) {
      return false;
    }
    return matches(userIds(), user.getUserInfo().getUserId())
        || matches(anonUserIds(), user.getUserInfo().getAnonUserId());
  }

  public boolean matches(View view) {
    if (!hasAnyIds()) {
      return false;
    }
    return matches(userIds(), view.getUserInfo().getUserId())
        || matches(anonUserIds(), view.getUserInfo().getAnonUserId())
        || matches(sessionIds(), view.getSessionId())
        || matches(viewIds(), view.getViewId());
  }

  private boolean matches(Request request) {
    if (!hasAnyIds()) {
      return false;
    }
    return matches(userIds(), request.getUserInfo().getUserId())
        || matches(anonUserIds(), request.getUserInfo().getAnonUserId())
        || matches(sessionIds(), request.getSessionId())
        || matches(viewIds(), request.getViewId())
        || matches(requestIds(), request.getRequestId())
        || matches(request.getInsertionList());
  }

  private boolean matches(DeliveryExecution execution) {
    return matches(execution.getExecutionInsertionList());
  }

  private boolean matches(Response response) {
    return matches(response.getInsertionList());
  }

  private boolean matches(List<Insertion> insertions) {
    if (!hasAnyIds()) {
      return false;
    }
    return insertions.stream().anyMatch(this::matches);
  }

  private boolean matches(Insertion insertion) {
    return matches(userIds(), insertion.getUserInfo().getUserId())
        || matches(anonUserIds(), insertion.getUserInfo().getAnonUserId())
        || matches(sessionIds(), insertion.getSessionId())
        || matches(viewIds(), insertion.getViewId())
        || matches(requestIds(), insertion.getRequestId())
        || matches(insertionIds(), insertion.getInsertionId());
  }

  public boolean matches(Impression impression) {
    if (!hasAnyIds()) {
      return false;
    }
    return matches(userIds(), impression.getUserInfo().getUserId())
        || matches(anonUserIds(), impression.getUserInfo().getAnonUserId())
        || matches(sessionIds(), impression.getSessionId())
        || matches(viewIds(), impression.getViewId())
        || matches(requestIds(), impression.getRequestId())
        || matches(insertionIds(), impression.getInsertionId())
        || matches(impressionIds(), impression.getImpressionId());
  }

  public boolean matches(Action action) {
    if (!hasAnyIds()) {
      return false;
    }
    return matches(userIds(), action.getUserInfo().getUserId())
        || matches(anonUserIds(), action.getUserInfo().getAnonUserId())
        || matches(sessionIds(), action.getSessionId())
        || matches(viewIds(), action.getViewId())
        || matches(requestIds(), action.getRequestId())
        || matches(insertionIds(), action.getInsertionId())
        || matches(impressionIds(), action.getImpressionId())
        || matches(actionIds(), action.getActionId());
  }
}
