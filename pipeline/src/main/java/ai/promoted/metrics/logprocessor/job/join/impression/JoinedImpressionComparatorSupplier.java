package ai.promoted.metrics.logprocessor.job.join.impression;

import ai.promoted.metrics.logprocessor.common.functions.base.SerializableSupplier;
import ai.promoted.proto.event.TinyJoinedImpression;
import java.util.Comparator;

public class JoinedImpressionComparatorSupplier
    implements SerializableSupplier<Comparator<TinyJoinedImpression>> {

  // Higher is more important.
  public Comparator<TinyJoinedImpression> get() {
    // The direct foreign key is handled through a separate check.
    // TODO - optimize for parameters.  E.g. is other_content_ids enabled?  E.g. retained_user_id
    // enabled?
    return hasMatchingAnonUserIdAndContentIdsComparator();
  }

  private static Comparator<TinyJoinedImpression> hasMatchingAnonUserIdAndContentIdsComparator() {
    return new Comparator<TinyJoinedImpression>() {
      @Override
      public int compare(TinyJoinedImpression o1, TinyJoinedImpression o2) {
        // Do in one Comparator to avoid doing equals twice on anonUserId.
        boolean matchesAnonUserId1 = hasMatchingAnonUserId(o1);
        boolean matchesAnonUserId2 = hasMatchingAnonUserId(o2);
        boolean matchesAnonUserIdAndContentId1 = matchesAnonUserId1 && hasMatchingContentId(o1);
        boolean matchesAnonUserIdAndContentId2 = matchesAnonUserId2 && hasMatchingContentId(o2);
        int cmp = Boolean.compare(matchesAnonUserIdAndContentId1, matchesAnonUserIdAndContentId2);

        if (cmp != 0) {
          return cmp;
        }

        boolean matchesAnonUserIdAndOtherContentId1 =
            matchesAnonUserId1 && hasMatchingOtherContentId(o1);
        boolean matchesAnonUserIdAndOtherContentId2 =
            matchesAnonUserId2 && hasMatchingOtherContentId(o2);
        return Boolean.compare(
            matchesAnonUserIdAndOtherContentId1, matchesAnonUserIdAndOtherContentId2);
      }
    };
  }

  public static boolean hasMatchingInsertionId(TinyJoinedImpression join) {
    String insertionInsertionId = join.getInsertion().getCore().getInsertionId();
    String impressionInsertionId = join.getImpression().getInsertionId();
    return !insertionInsertionId.isEmpty()
        && !impressionInsertionId.isEmpty()
        && insertionInsertionId.equals(impressionInsertionId);
  }

  private static boolean hasMatchingAnonUserId(TinyJoinedImpression join) {
    String insertionAnonUserId = join.getInsertion().getCommon().getAnonUserId();
    String impressionAnonUserId = join.getImpression().getCommon().getAnonUserId();
    return !insertionAnonUserId.isEmpty()
        && !impressionAnonUserId.isEmpty()
        && insertionAnonUserId.equals(impressionAnonUserId);
  }

  private static boolean hasMatchingContentId(TinyJoinedImpression join) {
    String insertionContentId = join.getInsertion().getCore().getContentId();
    String impressionContentId = join.getImpression().getContentId();
    return !insertionContentId.isEmpty()
        && !impressionContentId.isEmpty()
        && insertionContentId.equals(impressionContentId);
  }

  private static boolean hasMatchingOtherContentId(TinyJoinedImpression join) {
    String impressionContentId = join.getImpression().getContentId();
    return !impressionContentId.isEmpty()
        && join.getInsertion().getCore().getOtherContentIdsMap().containsValue(impressionContentId);
  }
}
