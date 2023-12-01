package ai.promoted.metrics.logprocessor.job.validateenrich;

import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.CohortMembership;
import ai.promoted.proto.event.Diagnostics;
import ai.promoted.proto.event.EnrichmentUnion;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.View;

interface EnrichmentUnionUtil {

  static EnrichmentUnion toCohortMembershipUnion(CohortMembership cohortMembership) {
    return EnrichmentUnion.newBuilder().setCohortMembership(cohortMembership).build();
  }

  static EnrichmentUnion toViewUnion(View view) {
    return EnrichmentUnion.newBuilder().setView(view).build();
  }

  static EnrichmentUnion toDeliveryLogUnion(DeliveryLog deliveryLog) {
    return EnrichmentUnion.newBuilder().setDeliveryLog(deliveryLog).build();
  }

  static EnrichmentUnion toImpressionUnion(Impression impression) {
    return EnrichmentUnion.newBuilder().setImpression(impression).build();
  }

  static EnrichmentUnion toActionUnion(Action action) {
    return EnrichmentUnion.newBuilder().setAction(action).build();
  }

  static EnrichmentUnion toDiagnosticsUnion(Diagnostics diagnostics) {
    return EnrichmentUnion.newBuilder().setDiagnostics(diagnostics).build();
  }

  static AuthUserIdKey toAuthUserIdKey(EnrichmentUnion union) {
    return new AuthUserIdKey(getPlatformId(union), getUserInfo(union).getUserId());
  }

  static AnonUserIdKey toAnonUserIdKey(EnrichmentUnion union) {
    return new AnonUserIdKey(getPlatformId(union), getUserInfo(union).getAnonUserId());
  }

  static long getPlatformId(EnrichmentUnion union) {
    switch (union.getEventCase()) {
      case ACTION:
        return union.getAction().getPlatformId();
      case COHORT_MEMBERSHIP:
        return union.getCohortMembership().getPlatformId();
      case DIAGNOSTICS:
        return union.getDiagnostics().getPlatformId();
      case DELIVERY_LOG:
        return union.getDeliveryLog().getPlatformId();
      case IMPRESSION:
        return union.getImpression().getPlatformId();
      case VIEW:
        return union.getView().getPlatformId();
      default:
        throw new UnsupportedOperationException("Unsupported EnrichmentUnion=" + union);
    }
  }

  static UserInfo getUserInfo(EnrichmentUnion union) {
    switch (union.getEventCase()) {
      case ACTION:
        return union.getAction().getUserInfo();
      case COHORT_MEMBERSHIP:
        return union.getCohortMembership().getUserInfo();
      case DIAGNOSTICS:
        return union.getDiagnostics().getUserInfo();
      case DELIVERY_LOG:
        return union.getDeliveryLog().getRequest().getUserInfo();
      case IMPRESSION:
        return union.getImpression().getUserInfo();
      case VIEW:
        return union.getView().getUserInfo();
      default:
        throw new UnsupportedOperationException("Unsupported EnrichmentUnion=" + union);
    }
  }

  static EnrichmentUnion setUserInfo(EnrichmentUnion union, UserInfo userInfo) {
    EnrichmentUnion.Builder builder = union.toBuilder();
    switch (union.getEventCase()) {
      case ACTION:
        builder.getActionBuilder().setUserInfo(userInfo);
        break;
      case COHORT_MEMBERSHIP:
        builder.getCohortMembershipBuilder().setUserInfo(userInfo);
        break;
      case DIAGNOSTICS:
        builder.getDiagnosticsBuilder().setUserInfo(userInfo);
        break;
      case DELIVERY_LOG:
        builder.getDeliveryLogBuilder().getRequestBuilder().setUserInfo(userInfo);
        break;
      case IMPRESSION:
        builder.getImpressionBuilder().setUserInfo(userInfo);
        break;
      case VIEW:
        builder.getViewBuilder().setUserInfo(userInfo);
        break;
      default:
        throw new UnsupportedOperationException("Unsupported EnrichmentUnion=" + union);
    }
    return builder.build();
  }
}
