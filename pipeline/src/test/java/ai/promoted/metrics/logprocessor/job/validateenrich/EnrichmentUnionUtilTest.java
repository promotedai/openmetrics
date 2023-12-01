package ai.promoted.metrics.logprocessor.job.validateenrich;

import static com.google.common.truth.Truth.assertThat;

import ai.promoted.proto.common.Timing;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.CohortMembership;
import ai.promoted.proto.event.Diagnostics;
import ai.promoted.proto.event.EnrichmentUnion;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.View;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class EnrichmentUnionUtilTest {

  private static final CohortMembership COHORT_MEMBERSHIP =
      CohortMembership.newBuilder()
          .setPlatformId(1L)
          .setTiming(Timing.newBuilder().setEventApiTimestamp(1))
          .setUserInfo(createUserInfo(1))
          .build();
  private static final View VIEW =
      View.newBuilder()
          .setPlatformId(2L)
          .setTiming(Timing.newBuilder().setEventApiTimestamp(2))
          .setUserInfo(createUserInfo(2))
          .build();
  private static final DeliveryLog DELIVERY_LOG =
      DeliveryLog.newBuilder()
          .setPlatformId(3L)
          .setRequest(
              Request.newBuilder()
                  .setTiming(Timing.newBuilder().setEventApiTimestamp(3))
                  .setUserInfo(createUserInfo(3)))
          .build();
  private static final Impression IMPRESSION =
      Impression.newBuilder()
          .setPlatformId(4L)
          .setTiming(Timing.newBuilder().setEventApiTimestamp(4))
          .setUserInfo(createUserInfo(4))
          .build();
  private static final Action ACTION =
      Action.newBuilder()
          .setPlatformId(5L)
          .setTiming(Timing.newBuilder().setEventApiTimestamp(5))
          .setUserInfo(createUserInfo(5))
          .build();
  private final Diagnostics DIAGNOSTICS =
      Diagnostics.newBuilder()
          .setPlatformId(6L)
          .setTiming(Timing.newBuilder().setEventApiTimestamp(6))
          .setUserInfo(createUserInfo(6))
          .build();

  private static final UserInfo createUserInfo(int num) {
    return UserInfo.newBuilder()
        .setAnonUserId("anonUserId" + num)
        .setUserId("userId" + num)
        .build();
  }

  @Test
  public void testcatchNewEnrichmentUnionTypes() {
    assertThat(
            EnrichmentUnion.getDescriptor().getFields().stream()
                .map(descriptor -> descriptor.getNumber())
                .collect(Collectors.toSet()))
        .containsExactly(1, 2, 3, 4, 5, 6);
  }

  @Test
  public void toFrom_CohortMembership() {
    assertThat(EnrichmentUnionUtil.toCohortMembershipUnion(COHORT_MEMBERSHIP).getCohortMembership())
        .isEqualTo(COHORT_MEMBERSHIP);
  }

  @Test
  public void toFrom_View() {
    assertThat(EnrichmentUnionUtil.toViewUnion(VIEW).getView()).isEqualTo(VIEW);
  }

  @Test
  public void toFrom_DeliveryLog() {
    assertThat(EnrichmentUnionUtil.toDeliveryLogUnion(DELIVERY_LOG).getDeliveryLog())
        .isEqualTo(DELIVERY_LOG);
  }

  @Test
  public void toFrom_Impression() {
    assertThat(EnrichmentUnionUtil.toImpressionUnion(IMPRESSION).getImpression())
        .isEqualTo(IMPRESSION);
  }

  @Test
  public void toFrom_Action() {
    assertThat(EnrichmentUnionUtil.toActionUnion(ACTION).getAction()).isEqualTo(ACTION);
  }

  @Test
  public void toFrom_Diagnostics() {
    assertThat(EnrichmentUnionUtil.toDiagnosticsUnion(DIAGNOSTICS).getDiagnostics())
        .isEqualTo(DIAGNOSTICS);
  }

  @Test
  public void toAuthUserIdKey() {
    assertThat(
            EnrichmentUnionUtil.toAuthUserIdKey(
                EnrichmentUnionUtil.toCohortMembershipUnion(COHORT_MEMBERSHIP)))
        .isEqualTo(new AuthUserIdKey(1, "userId1"));
  }

  @Test
  public void toAnonUserIdKey() {
    assertThat(
            EnrichmentUnionUtil.toAnonUserIdKey(
                EnrichmentUnionUtil.toCohortMembershipUnion(COHORT_MEMBERSHIP)))
        .isEqualTo(new AnonUserIdKey(1, "anonUserId1"));
  }

  @Test
  public void getPlatformId_CohortMembership() {
    assertThat(
            EnrichmentUnionUtil.getPlatformId(
                EnrichmentUnionUtil.toCohortMembershipUnion(COHORT_MEMBERSHIP)))
        .isEqualTo(1L);
  }

  @Test
  public void getPlatformId_View() {
    assertThat(EnrichmentUnionUtil.getPlatformId(EnrichmentUnionUtil.toViewUnion(VIEW)))
        .isEqualTo(2L);
  }

  @Test
  public void getPlatformId_DeliveryLog() {
    assertThat(
            EnrichmentUnionUtil.getPlatformId(EnrichmentUnionUtil.toDeliveryLogUnion(DELIVERY_LOG)))
        .isEqualTo(3L);
  }

  @Test
  public void getPlatformId_Impression() {
    assertThat(EnrichmentUnionUtil.getPlatformId(EnrichmentUnionUtil.toImpressionUnion(IMPRESSION)))
        .isEqualTo(4L);
  }

  @Test
  public void getPlatformId_Action() {
    assertThat(EnrichmentUnionUtil.getPlatformId(EnrichmentUnionUtil.toActionUnion(ACTION)))
        .isEqualTo(5L);
  }

  @Test
  public void getPlatformId_Diagnostics() {
    assertThat(
            EnrichmentUnionUtil.getPlatformId(EnrichmentUnionUtil.toDiagnosticsUnion(DIAGNOSTICS)))
        .isEqualTo(6L);
  }

  @Test
  public void getUserInfo_CohortMembership() {
    assertThat(
            EnrichmentUnionUtil.getUserInfo(
                EnrichmentUnionUtil.toCohortMembershipUnion(COHORT_MEMBERSHIP)))
        .isEqualTo(COHORT_MEMBERSHIP.getUserInfo());
  }

  @Test
  public void getUserInfo_View() {
    assertThat(EnrichmentUnionUtil.getUserInfo(EnrichmentUnionUtil.toViewUnion(VIEW)))
        .isEqualTo(VIEW.getUserInfo());
  }

  @Test
  public void getUserInfo_DeliveryLog() {
    assertThat(
            EnrichmentUnionUtil.getUserInfo(EnrichmentUnionUtil.toDeliveryLogUnion(DELIVERY_LOG)))
        .isEqualTo(DELIVERY_LOG.getRequest().getUserInfo());
  }

  @Test
  public void getUserInfo_Impression() {
    assertThat(EnrichmentUnionUtil.getUserInfo(EnrichmentUnionUtil.toImpressionUnion(IMPRESSION)))
        .isEqualTo(IMPRESSION.getUserInfo());
  }

  @Test
  public void getUserInfo_Action() {
    assertThat(EnrichmentUnionUtil.getUserInfo(EnrichmentUnionUtil.toActionUnion(ACTION)))
        .isEqualTo(ACTION.getUserInfo());
  }

  @Test
  public void getUserInfo_Diagnostics() {
    assertThat(EnrichmentUnionUtil.getUserInfo(EnrichmentUnionUtil.toDiagnosticsUnion(DIAGNOSTICS)))
        .isEqualTo(DIAGNOSTICS.getUserInfo());
  }

  // Tmp

  @Test
  public void setUserInfo_CohortMembership() {
    assertThat(
            EnrichmentUnionUtil.setUserInfo(
                EnrichmentUnionUtil.toCohortMembershipUnion(COHORT_MEMBERSHIP), createUserInfo(7)))
        .isEqualTo(
            EnrichmentUnionUtil.toCohortMembershipUnion(
                COHORT_MEMBERSHIP.toBuilder().setUserInfo(createUserInfo(7)).build()));
  }

  @Test
  public void setUserInfo_View() {
    assertThat(
            EnrichmentUnionUtil.setUserInfo(
                EnrichmentUnionUtil.toViewUnion(VIEW), createUserInfo(7)))
        .isEqualTo(
            EnrichmentUnionUtil.toViewUnion(
                VIEW.toBuilder().setUserInfo(createUserInfo(7)).build()));
  }

  @Test
  public void setUserInfo_DeliveryLog() {
    DeliveryLog.Builder expectedBuilder = DELIVERY_LOG.toBuilder();
    expectedBuilder.getRequestBuilder().setUserInfo(createUserInfo(7));
    assertThat(
            EnrichmentUnionUtil.setUserInfo(
                EnrichmentUnionUtil.toDeliveryLogUnion(DELIVERY_LOG), createUserInfo(7)))
        .isEqualTo(EnrichmentUnionUtil.toDeliveryLogUnion(expectedBuilder.build()));
  }

  @Test
  public void setUserInfo_Impression() {
    assertThat(
            EnrichmentUnionUtil.setUserInfo(
                EnrichmentUnionUtil.toImpressionUnion(IMPRESSION), createUserInfo(7)))
        .isEqualTo(
            EnrichmentUnionUtil.toImpressionUnion(
                IMPRESSION.toBuilder().setUserInfo(createUserInfo(7)).build()));
  }

  @Test
  public void setUserInfo_Action() {
    assertThat(
            EnrichmentUnionUtil.setUserInfo(
                EnrichmentUnionUtil.toActionUnion(ACTION), createUserInfo(7)))
        .isEqualTo(
            EnrichmentUnionUtil.toActionUnion(
                ACTION.toBuilder().setUserInfo(createUserInfo(7)).build()));
  }

  @Test
  public void setUserInfo_Diagnostics() {
    assertThat(
            EnrichmentUnionUtil.setUserInfo(
                EnrichmentUnionUtil.toDiagnosticsUnion(DIAGNOSTICS), createUserInfo(7)))
        .isEqualTo(
            EnrichmentUnionUtil.toDiagnosticsUnion(
                DIAGNOSTICS.toBuilder().setUserInfo(createUserInfo(7)).build()));
  }
}
