package ai.promoted.metrics.logprocessor.job.join.impression;

import static com.google.common.truth.Truth.assertThat;

import ai.promoted.proto.event.TinyCommonInfo;
import ai.promoted.proto.event.TinyImpression;
import ai.promoted.proto.event.TinyInsertion;
import ai.promoted.proto.event.TinyJoinedImpression;
import java.util.Comparator;
import org.junit.jupiter.api.Test;

public class JoinedImpressionComparatorSupplierTest {

  @Test
  public void get() throws Exception {
    Comparator<TinyJoinedImpression> comparator = new JoinedImpressionComparatorSupplier().get();

    assertThat(
            comparator.compare(
                newJoinedImpression(newInsertion("u1", "c1", true), newImpression("u1", "")),
                newJoinedImpression(newInsertion("u1", "c1", true), newImpression("u1", ""))))
        .isEqualTo(0);

    assertThat(
            comparator.compare(
                newJoinedImpression(newInsertion("u1", "c1", true), newImpression("u1", "c1")),
                newJoinedImpression(newInsertion("u1", "c1", true), newImpression("u1", "c1"))))
        .isEqualTo(0);

    // Having insertionId should not change this Comparator.
    assertThat(
            comparator.compare(
                newJoinedImpression(newInsertion("u1", "c1", true), newImpression("u1", "")),
                newJoinedImpression(
                    newInsertion("u1", "c1", true),
                    newImpression("u1", "").toBuilder().setInsertionId("i1").build())))
        .isEqualTo(0);

    assertThat(
            comparator.compare(
                newJoinedImpression(newInsertion("u1", "c1", true), newImpression("u2", "c1")),
                newJoinedImpression(newInsertion("u1", "c1", true), newImpression("u1", "c1"))))
        .isLessThan(0);
    assertThat(
            comparator.compare(
                newJoinedImpression(newInsertion("u1", "c2", true), newImpression("u1", "c1")),
                newJoinedImpression(newInsertion("u1", "c1", true), newImpression("u1", "c1"))))
        .isLessThan(0);

    assertThat(
            comparator.compare(
                newJoinedImpression(newInsertion("u1", "c1", false), newImpression("u1", "c1")),
                newJoinedImpression(newInsertion("u1", "c1", true), newImpression("u1", "c1"))))
        .isLessThan(0);
    assertThat(
            comparator.compare(
                newJoinedImpression(newInsertion("u1", "c1", false), newImpression("u1", "c1")),
                newJoinedImpression(newInsertion("u1", "c1", false), newImpression("u1", "c1"))))
        .isEqualTo(0);
    assertThat(
            comparator.compare(
                newJoinedImpression(newInsertion("u1", "c1", false), newImpression("u1", "c1")),
                newJoinedImpression(newInsertion("u1", "c2", false), newImpression("u1", "c1"))))
        .isGreaterThan(0);
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
