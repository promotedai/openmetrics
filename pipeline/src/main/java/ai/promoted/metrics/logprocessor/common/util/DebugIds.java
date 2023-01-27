package ai.promoted.metrics.logprocessor.common.util;

import ai.promoted.proto.delivery.DeliveryExecution;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.delivery.Response;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.CombinedDeliveryLog;
import ai.promoted.proto.event.JoinedEvent;
import ai.promoted.proto.event.FlatResponseInsertion;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.JoinedIdentifiers;
import ai.promoted.proto.event.TinyDeliveryLog;
import ai.promoted.proto.event.TinyEvent;
import ai.promoted.proto.event.User;
import ai.promoted.proto.event.View;
import com.google.auto.value.AutoValue;
import com.google.auto.value.extension.memoized.Memoized;
import com.google.common.collect.ImmutableSet;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Set;

// TODO - make this an interface and then have specific impls for (1) empty, (2) all (for tests) and (3) specific-fields.
/**
 * Used to produce extra logging for specific log record IDs.
 */
@AutoValue
public abstract class DebugIds implements Serializable {
    private static final long serialVersionUID = 1L;

    // Set of userIds substrings to log more flatten event debug logs. Acts as an OR with other debug fields. Used to track down missing flattened events.
    abstract Set<String> userIds();

    // Set of logUserIds substrings to log more flatten event debug logs. Acts as an OR with other debug fields. Used to track down missing flattened events.
    abstract Set<String> logUserIds();
    
    // Set of sessionIds substrings to log more flatten event debug logs. Acts as an OR with other debug fields. Used to track down missing flattened events.
    abstract Set<String> sessionIds();

    // Set of viewIds substrings to log more flatten event debug logs. Acts as an OR with other debug fields. Used to track down missing flattened events.
    abstract Set<String> viewIds();

    // Set of autoViewIds substrings to log more flatten event debug logs. Acts as an OR with other debug fields. Used to track down missing flattened events.
    abstract Set<String>autoViewIds();

    // Set of requestIds substrings to log more flatten event debug logs. Acts as an OR with other debug fields. Used to track down missing flattened events.
    abstract Set<String> requestIds();
    
    // Set of insertionIds substrings to log more flatten event debug logs. Acts as an OR with other debug fields. Used to track down missing flattened events.
    abstract Set<String> insertionIds();
    
    // Set of impressionIds substrings to log more flatten event debug logs. Acts as an OR with other debug fields. Used to track down missing flattened events.
    abstract Set<String> impressionIds();
    
    // Set of actionIds substrings to log more flatten event debug logs. Acts as an OR with other debug fields. Used to track down missing flattened events.
    abstract Set<String> actionIds();

    public static Builder builder() {
        return new AutoValue_DebugIds.Builder()
                .setUserIds(ImmutableSet.of())
                .setLogUserIds(ImmutableSet.of())
                .setSessionIds(ImmutableSet.of())
                .setViewIds(ImmutableSet.of())
                .setAutoViewIds(ImmutableSet.of())
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
        public abstract Builder setLogUserIds(Set<String> logUserIds);
        public abstract Builder setSessionIds(Set<String> sessionIds);
        public abstract Builder setViewIds(Set<String> viewIds);
        public abstract Builder setAutoViewIds(Set<String> autoViewIds);
        public abstract Builder setRequestIds(Set<String> requestIds);
        public abstract Builder setInsertionIds(Set<String> insertionIds);
        public abstract Builder setImpressionIds(Set<String> impressionIds);
        public abstract Builder setActionIds(Set<String> actionIds);
        abstract Set<String> userIds();
        abstract Set<String> logUserIds();
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
                || !logUserIds().isEmpty()
                || !sessionIds().isEmpty()
                || !viewIds().isEmpty()
                || !autoViewIds().isEmpty()
                || !requestIds().isEmpty()
                || !insertionIds().isEmpty()
                || !impressionIds().isEmpty()
                || !actionIds().isEmpty();
    }

    public boolean matchesViewId(String viewId) {
        // TODO - I don't know if having a hasAnyIds is faster or slower.  These sets are usually empty in prod.
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

    public boolean matches(JoinedEvent joinedEvent) {
        if (!hasAnyIds()) {
            return false;
        }
        JoinedIdentifiers ids = joinedEvent.getIds();
        return matches(userIds(), ids.getUserId())
            || matches(logUserIds(), ids.getLogUserId())
            || matches(sessionIds(), ids.getSessionId())
            || matches(viewIds(), ids.getViewId())
            || matches(autoViewIds(), ids.getAutoViewId())
            || matches(requestIds(), ids.getRequestId())
            || matches(insertionIds(), ids.getInsertionId())
            || matches(impressionIds(), ids.getImpressionId())
            // ActionId is not on JoinedEvent so we need to check the message.
            || matches(actionIds(), joinedEvent.getAction().getActionId());
    }

    public boolean matches(TinyEvent joinedEvent) {
        if (!hasAnyIds()) {
            return false;
        }
        return matches(logUserIds(), joinedEvent.getLogUserId())
            // TODO - decide if we'll add sessionId.
            || matches(viewIds(), joinedEvent.getViewId())
            || matches(autoViewIds(), joinedEvent.getAutoViewId())
            || matches(requestIds(), joinedEvent.getRequestId())
            || matches(insertionIds(), joinedEvent.getInsertionId())
            || matches(impressionIds(), joinedEvent.getImpressionId())
            || matches(actionIds(), joinedEvent.getActionId());
    }

    public boolean matches(TinyDeliveryLog deliveryLog) {
        if (!hasAnyIds()) {
            return false;
        }
        return matches(logUserIds(), deliveryLog.getLogUserId())
                // TODO - decide if we'll add sessionId.
                || matches(viewIds(), deliveryLog.getViewId())
                // TODO - decide if we want autoViewId.
                || matches(requestIds(), deliveryLog.getRequestId())
                || deliveryLog.getResponseInsertionList().stream().anyMatch(this::matches);
    }

    private boolean matches(TinyDeliveryLog.TinyInsertion insertion) {
        return matches(insertionIds(), insertion.getInsertionId());
    }

    private boolean matches(Set<String> idSubstrings, String id) {
        return !idSubstrings.isEmpty() &&  idSubstrings.stream().anyMatch(substring -> id.contains(substring));
    }

    public boolean matches(FlatResponseInsertion flatInsertion) {
        if (!hasAnyIds()) {
            return false;
        }
        JoinedIdentifiers ids = flatInsertion.getIds();
        return matches(userIds(), ids.getUserId())
            || matches(logUserIds(), ids.getLogUserId())
            || matches(sessionIds(), ids.getSessionId())
            || matches(viewIds(), ids.getViewId())
            || matches(autoViewIds(), ids.getAutoViewId())
            || matches(requestIds(), ids.getRequestId())
            || matches(insertionIds(), ids.getInsertionId());
    }

    public boolean matches(String id) {
        if (!hasAnyIds()) {
            return false;
        }
        return matches(userIds(), id)
            || matches(logUserIds(), id)
            || matches(sessionIds(), id)
            || matches(viewIds(), id)
            || matches(autoViewIds(), id)
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
                || matches(logUserIds(), user.getUserInfo().getLogUserId());
    }

    public boolean matches(View view) {
        if (!hasAnyIds()) {
            return false;
        }
        return matches(userIds(), view.getUserInfo().getUserId())
                || matches(logUserIds(), view.getUserInfo().getLogUserId())
                || matches(sessionIds(), view.getSessionId())
                || matches(viewIds(), view.getViewId());
    }

    private boolean matches(Request request) {
        if (!hasAnyIds()) {
            return false;
        }
        return matches(userIds(), request.getUserInfo().getUserId())
                || matches(logUserIds(), request.getUserInfo().getLogUserId())
                || matches(sessionIds(), request.getSessionId())
                || matches(viewIds(), request.getViewId())
                || matches(autoViewIds(), request.getAutoViewId())
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
                || matches(logUserIds(), insertion.getUserInfo().getLogUserId())
                || matches(sessionIds(), insertion.getSessionId())
                || matches(viewIds(), insertion.getViewId())
                || matches(autoViewIds(), insertion.getAutoViewId())
                || matches(requestIds(), insertion.getRequestId())
                || matches(insertionIds(), insertion.getInsertionId());
    }

    public boolean matches(Impression impression) {
        if (!hasAnyIds()) {
            return false;
        }
        return matches(userIds(), impression.getUserInfo().getUserId())
                || matches(logUserIds(), impression.getUserInfo().getLogUserId())
                || matches(sessionIds(), impression.getSessionId())
                || matches(viewIds(), impression.getViewId())
                || matches(autoViewIds(), impression.getAutoViewId())
                || matches(requestIds(), impression.getRequestId())
                || matches(insertionIds(), impression.getInsertionId())
                || matches(impressionIds(), impression.getImpressionId());
    }

    public boolean matches(Action action) {
        if (!hasAnyIds()) {
            return false;
        }
        return matches(userIds(), action.getUserInfo().getUserId())
                || matches(logUserIds(), action.getUserInfo().getLogUserId())
                || matches(sessionIds(), action.getSessionId())
                || matches(viewIds(), action.getViewId())
                || matches(autoViewIds(), action.getAutoViewId())
                || matches(requestIds(), action.getRequestId())
                || matches(insertionIds(), action.getInsertionId())
                || matches(impressionIds(), action.getImpressionId())
                || matches(actionIds(), action.getActionId());
    }
}
