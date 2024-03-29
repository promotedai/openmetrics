package ai.promoted.metrics.logprocessor.common.functions;

import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.AutoView;
import ai.promoted.proto.event.CohortMembership;
import ai.promoted.proto.event.Diagnostics;
import ai.promoted.proto.event.DroppedMergeDetailsEvent;
import ai.promoted.proto.event.FlatResponseInsertion;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.JoinedEvent;
import ai.promoted.proto.event.Session;
import ai.promoted.proto.event.SessionProfile;
import ai.promoted.proto.event.User;
import ai.promoted.proto.event.View;

/** Util for UserInfo. */
public class UserInfoUtil {

  public static final JoinedEvent clearUserId(JoinedEvent joinedEvent) {
    return clearUserId(joinedEvent.toBuilder()).build();
  }

  public static final JoinedEvent.Builder clearUserId(JoinedEvent.Builder builder) {
    if (builder.hasIds()) {
      builder.getIdsBuilder().clearUserId();
    }
    if (builder.hasUser()) {
      clearUserId(builder.getUserBuilder());
    }
    if (builder.hasSessionProfile()) {
      clearUserId(builder.getSessionProfileBuilder());
    }
    if (builder.hasSession()) {
      clearUserId(builder.getSessionBuilder());
    }
    if (builder.hasView()) {
      clearUserId(builder.getViewBuilder());
    }
    if (builder.hasAutoView()) {
      clearUserId(builder.getAutoViewBuilder());
    }
    if (builder.hasRequest()) {
      clearUserId(builder.getRequestBuilder());
    }
    if (builder.hasImpression()) {
      clearUserId(builder.getImpressionBuilder());
    }
    if (builder.hasAction()) {
      clearUserId(builder.getActionBuilder());
    }
    return builder;
  }

  public static final FlatResponseInsertion clearUserId(FlatResponseInsertion flat) {
    return clearUserId(flat.toBuilder()).build();
  }

  public static final FlatResponseInsertion.Builder clearUserId(
      FlatResponseInsertion.Builder builder) {
    if (builder.hasIds()) {
      builder.getIdsBuilder().clearUserId();
    }
    if (builder.hasUser()) {
      clearUserId(builder.getUserBuilder());
    }
    if (builder.hasSessionProfile()) {
      clearUserId(builder.getSessionProfileBuilder());
    }
    if (builder.hasSession()) {
      clearUserId(builder.getSessionBuilder());
    }
    if (builder.hasView()) {
      clearUserId(builder.getViewBuilder());
    }
    if (builder.hasAutoView()) {
      clearUserId(builder.getAutoViewBuilder());
    }
    if (builder.hasRequest()) {
      clearUserId(builder.getRequestBuilder());
    }
    builder.getImpressionBuilderList().forEach(UserInfoUtil::clearUserId);
    builder.getActionBuilderList().forEach(UserInfoUtil::clearUserId);
    return builder;
  }

  public static final DroppedMergeDetailsEvent clearUserId(DroppedMergeDetailsEvent event) {
    return clearUserId(event.toBuilder()).build();
  }

  public static final DroppedMergeDetailsEvent.Builder clearUserId(
      DroppedMergeDetailsEvent.Builder builder) {
    if (builder.hasJoinedEvent()) {
      clearUserId(builder.getJoinedEventBuilder());
    }
    return builder;
  }

  public static final User clearUserId(User user) {
    return clearUserId(user.toBuilder()).build();
  }

  public static final User.Builder clearUserId(User.Builder builder) {
    if (builder.hasUserInfo()) {
      builder.getUserInfoBuilder().clearUserId();
    }
    return builder;
  }

  public static final SessionProfile clearUserId(SessionProfile sessionProfile) {
    return clearUserId(sessionProfile.toBuilder()).build();
  }

  public static final SessionProfile.Builder clearUserId(SessionProfile.Builder builder) {
    if (builder.hasUserInfo()) {
      builder.getUserInfoBuilder().clearUserId();
    }
    return builder;
  }

  public static final Session clearUserId(Session session) {
    return clearUserId(session.toBuilder()).build();
  }

  public static final Session.Builder clearUserId(Session.Builder builder) {
    if (builder.hasUserInfo()) {
      builder.getUserInfoBuilder().clearUserId();
    }
    return builder;
  }

  public static final View clearUserId(View view) {
    return clearUserId(view.toBuilder()).build();
  }

  public static final View.Builder clearUserId(View.Builder builder) {
    if (builder.hasUserInfo()) {
      builder.getUserInfoBuilder().clearUserId();
    }
    return builder;
  }

  public static final AutoView clearUserId(AutoView autoView) {
    return clearUserId(autoView.toBuilder()).build();
  }

  public static final AutoView.Builder clearUserId(AutoView.Builder builder) {
    if (builder.hasUserInfo()) {
      builder.getUserInfoBuilder().clearUserId();
    }
    return builder;
  }

  public static final DeliveryLog clearUserId(DeliveryLog deliveryLog) {
    return clearUserId(deliveryLog.toBuilder()).build();
  }

  public static final DeliveryLog.Builder clearUserId(DeliveryLog.Builder builder) {
    if (builder.hasRequest()) {
      clearUserId(builder.getRequestBuilder());
    }
    // Skip cleaning on Insertions.  We'll want to remove that field soon anyways.
    return builder;
  }

  public static final Request.Builder clearUserId(Request.Builder builder) {
    if (builder.hasUserInfo()) {
      builder.getUserInfoBuilder().clearUserId();
    }
    // Skip cleaning on Insertions.  We'll want to remove that field soon anyways.
    return builder;
  }

  public static final Impression clearUserId(Impression impression) {
    return clearUserId(impression.toBuilder()).build();
  }

  public static final Impression.Builder clearUserId(Impression.Builder builder) {
    if (builder.hasUserInfo()) {
      builder.getUserInfoBuilder().clearUserId();
    }
    return builder;
  }

  public static final Action clearUserId(Action action) {
    return clearUserId(action.toBuilder()).build();
  }

  public static final Action.Builder clearUserId(Action.Builder builder) {
    if (builder.hasUserInfo()) {
      builder.getUserInfoBuilder().clearUserId();
    }
    return builder;
  }

  public static final CohortMembership clearUserId(CohortMembership cohortMembership) {
    return clearUserId(cohortMembership.toBuilder()).build();
  }

  public static final CohortMembership.Builder clearUserId(CohortMembership.Builder builder) {
    if (builder.hasUserInfo()) {
      builder.getUserInfoBuilder().clearUserId();
    }
    return builder;
  }

  public static final Diagnostics clearUserId(Diagnostics diagnostics) {
    return clearUserId(diagnostics.toBuilder()).build();
  }

  public static final Diagnostics.Builder clearUserId(Diagnostics.Builder builder) {
    if (builder.hasUserInfo()) {
      builder.getUserInfoBuilder().clearUserId();
    }
    return builder;
  }
}
