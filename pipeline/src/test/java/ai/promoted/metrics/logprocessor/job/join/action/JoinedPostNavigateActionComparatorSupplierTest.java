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

public class JoinedPostNavigateActionComparatorSupplierTest {

  @Test
  public void get() throws Exception {
    Comparator<TinyJoinedAction> comparator =
        new JoinedPostNavigateActionComparatorSupplier().get();

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
                    newInsertion("u1", "c1", true),
                    newImpression("u1", ""),
                    newAction("u1", "c1", true)),
                newJoinedAction(
                    newInsertion("u1", "c1", true),
                    newImpression("u1", "").toBuilder().setImpressionId("i1").build(),
                    newAction("u1", "c1", true).toBuilder().setImpressionId("i1").build())))
        .isLessThan(0);

    assertThat(
            comparator.compare(
                newJoinedAction(
                    newInsertion("u1", "c1", true),
                    newImpression("u1", ""),
                    newAction("u1", "c1", true)),
                newJoinedAction(
                        newInsertion("u1", "c1", true),
                        newImpression("u1", ""),
                        newAction("u1", "c1", true))
                    .toBuilder()
                    .setNavigateJoin(true)
                    .build()))
        .isLessThan(0);

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
