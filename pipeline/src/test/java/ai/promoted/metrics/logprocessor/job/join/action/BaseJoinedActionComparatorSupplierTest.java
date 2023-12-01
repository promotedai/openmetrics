package ai.promoted.metrics.logprocessor.job.join.action;

import static com.google.common.truth.Truth.assertThat;

import ai.promoted.proto.event.TinyAction;
import ai.promoted.proto.event.TinyCommonInfo;
import ai.promoted.proto.event.TinyImpression;
import ai.promoted.proto.event.TinyInsertion;
import ai.promoted.proto.event.TinyJoinedAction;
import ai.promoted.proto.event.TinyJoinedImpression;
import java.util.Comparator;
import org.junit.jupiter.api.Test;

public class BaseJoinedActionComparatorSupplierTest {

  @Test
  public void hasMatchingAnonUserIdAndContentIdsComparator() throws Exception {
    Comparator<TinyJoinedAction> comparator =
        BaseJoinedActionComparatorSupplier.hasMatchingAnonUserIdAndContentIdsComparator();

    assertThat(
            comparator.compare(
                newJoinedAction(
                    newInsertion("u1", "c1", true),
                    newImpression("u1", ""),
                    newAction("u1", "c1", true)),
                newJoinedAction(
                    newInsertion("u1", "c1", true),
                    newImpression("u1", ""),
                    newAction("u1", "c1", true))))
        .isEqualTo(0);
    assertThat(
            comparator.compare(
                newJoinedAction(
                    newInsertion("u2", "c1", true),
                    newImpression("u2", ""),
                    newAction("u1", "c1", true)),
                newJoinedAction(
                    newInsertion("u1", "c1", true),
                    newImpression("u1", ""),
                    newAction("u1", "c1", true))))
        .isLessThan(0);
    assertThat(
            comparator.compare(
                newJoinedAction(
                    newInsertion("u1", "c2", true),
                    newImpression("u1", ""),
                    newAction("u1", "c1", true)),
                newJoinedAction(
                    newInsertion("u1", "c1", true),
                    newImpression("u1", ""),
                    newAction("u1", "c1", true))))
        .isLessThan(0);

    assertThat(
            comparator.compare(
                newJoinedAction(
                    newInsertion("u1", "c1", false),
                    newImpression("u1", ""),
                    newAction("u1", "c1", true)),
                newJoinedAction(
                    newInsertion("u1", "c1", true),
                    newImpression("u1", ""),
                    newAction("u1", "c1", true))))
        .isLessThan(0);
    assertThat(
            comparator.compare(
                newJoinedAction(
                    newInsertion("u1", "c1", true),
                    newImpression("u1", ""),
                    newAction("u1", "c1", false)),
                newJoinedAction(
                    newInsertion("u1", "c1", true),
                    newImpression("u1", ""),
                    newAction("u1", "c1", true))))
        .isLessThan(0);
    assertThat(
            comparator.compare(
                newJoinedAction(
                    newInsertion("u1", "c1", false),
                    newImpression("u1", ""),
                    newAction("u1", "c1", false)),
                newJoinedAction(
                    newInsertion("u1", "c1", false),
                    newImpression("u1", ""),
                    newAction("u1", "c1", false))))
        .isEqualTo(0);
    assertThat(
            comparator.compare(
                newJoinedAction(
                    newInsertion("u1", "c1", false),
                    newImpression("u1", ""),
                    newAction("u1", "c1", false)),
                newJoinedAction(
                    newInsertion("u1", "c2", false),
                    newImpression("u1", ""),
                    newAction("u1", "c1", false))))
        .isGreaterThan(0);
  }

  @Test
  public void hasMatchingImpressionId() throws Exception {
    assertThat(
            BaseJoinedActionComparatorSupplier.hasMatchingImpressionId(
                newJoinedAction(
                    TinyInsertion.newBuilder().build(),
                    TinyImpression.newBuilder().setImpressionId("imp1").build(),
                    TinyAction.newBuilder().setImpressionId("imp1").build())))
        .isTrue();
    assertThat(
            BaseJoinedActionComparatorSupplier.hasMatchingImpressionId(
                newJoinedAction(
                    TinyInsertion.newBuilder().build(),
                    TinyImpression.newBuilder().setImpressionId("imp1").build(),
                    TinyAction.newBuilder().setImpressionId("imp2").build())))
        .isFalse();
  }

  @Test
  public void hasMatchingOtherContentId() throws Exception {
    assertThat(
            BaseJoinedActionComparatorSupplier.hasMatchingOtherContentId(
                newJoinedAction(
                    newInsertion("", "c1", false),
                    newImpression("", ""),
                    newAction("", "c1", false))))
        .isTrue();

    assertThat(
            BaseJoinedActionComparatorSupplier.hasMatchingOtherContentId(
                newJoinedAction(
                    newInsertion("", "c2", false),
                    newImpression("", ""),
                    newAction("", "c1", false))))
        .isFalse();

    assertThat(
            BaseJoinedActionComparatorSupplier.hasMatchingOtherContentId(
                newJoinedAction(
                    newInsertion("", "c1", true),
                    newImpression("", ""),
                    newAction("", "c1", false))))
        .isTrue();

    assertThat(
            BaseJoinedActionComparatorSupplier.hasMatchingOtherContentId(
                newJoinedAction(
                    newInsertion("", "c1", false),
                    newImpression("", ""),
                    newAction("", "c1", true))))
        .isTrue();

    assertThat(
            BaseJoinedActionComparatorSupplier.hasMatchingOtherContentId(
                newJoinedAction(
                    newInsertion("", "c2", true),
                    newImpression("", ""),
                    newAction("", "c1", false))))
        .isFalse();

    // The wrapper Comparator actually handles this case separately.
    assertThat(
            BaseJoinedActionComparatorSupplier.hasMatchingOtherContentId(
                newJoinedAction(
                    newInsertion("", "c1", true),
                    newImpression("", ""),
                    newAction("", "c1", true))))
        .isFalse();
  }

  private TinyJoinedAction newJoinedAction(
      TinyInsertion insertion, TinyImpression impression, TinyAction action) {
    return TinyJoinedAction.newBuilder()
        .setJoinedImpression(newJoinedImpression(insertion, impression))
        .setAction(action)
        .build();
  }

  private static TinyAction newAction(
      String anonUserId, String contentId, boolean contentIdOnAction) {
    TinyAction.Builder builder = TinyAction.newBuilder().setCommon(newCommonInfo(anonUserId));
    if (!contentId.isEmpty()) {
      if (contentIdOnAction) {
        builder.setContentId(contentId);
      } else {
        builder.putOtherContentIds(1, contentId);
      }
    }
    return builder.build();
  }

  // otherContentId = used to create lower priority TinyInsertions.
  private static TinyInsertion newInsertion(
      String anonUserId, String contentId, boolean contentIdOnAction) {
    TinyInsertion.Builder builder = TinyInsertion.newBuilder().setCommon(newCommonInfo(anonUserId));
    if (!contentId.isEmpty()) {
      if (contentIdOnAction) {
        builder.getCoreBuilder().setContentId(contentId);
      } else {
        builder.getCoreBuilder().putOtherContentIds(1, contentId);
      }
    }
    return builder.build();
  }

  private static TinyImpression newImpression(String anonUserId, String contentId) {
    return TinyImpression.newBuilder()
        .setCommon(newCommonInfo(anonUserId))
        .setContentId(contentId)
        .build();
  }

  private static TinyJoinedImpression newJoinedImpression(
      TinyInsertion insertion, TinyImpression impression) {
    return TinyJoinedImpression.newBuilder()
        .setInsertion(insertion)
        .setImpression(impression)
        .build();
  }

  private static TinyCommonInfo newCommonInfo(String anonUserId) {
    return TinyCommonInfo.newBuilder().setAnonUserId(anonUserId).build();
  }
}
