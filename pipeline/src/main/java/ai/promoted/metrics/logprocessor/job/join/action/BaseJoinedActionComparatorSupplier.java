package ai.promoted.metrics.logprocessor.job.join.action;

import ai.promoted.metrics.logprocessor.common.functions.base.SerializableSupplier;
import ai.promoted.proto.event.TinyJoinedAction;
import com.google.common.annotations.VisibleForTesting;
import java.util.Comparator;
import java.util.Map;

/**
 * Base class for *JoinedActionComparatorSupplier. A Supplier is used to make it easier to
 * serialize.
 */
public abstract class BaseJoinedActionComparatorSupplier
    implements SerializableSupplier<Comparator<TinyJoinedAction>> {

  protected static Comparator<TinyJoinedAction> hasMatchingAnonUserIdAndContentIdsComparator() {
    return (o1, o2) -> {
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
    };
  }

  public static boolean hasMatchingImpressionId(TinyJoinedAction join) {
    String impressionImpressionId = join.getJoinedImpression().getImpression().getImpressionId();
    String actionImpressionId = join.getAction().getImpressionId();
    return !impressionImpressionId.isEmpty()
        && !actionImpressionId.isEmpty()
        && impressionImpressionId.equals(actionImpressionId);
  }

  private static boolean hasMatchingAnonUserId(TinyJoinedAction join) {
    // TODO - should this be on the unnested version?
    String insertionAnonUserId =
        join.getJoinedImpression().getInsertion().getCommon().getAnonUserId();
    String actionAnonUserId = join.getAction().getCommon().getAnonUserId();
    if (!insertionAnonUserId.isEmpty()
        && !actionAnonUserId.isEmpty()
        && insertionAnonUserId.equals(actionAnonUserId)) {
      return true;
    }
    String impressionAnonUserId =
        join.getJoinedImpression().getImpression().getCommon().getAnonUserId();
    return !impressionAnonUserId.isEmpty()
        && !actionAnonUserId.isEmpty()
        && impressionAnonUserId.equals(actionAnonUserId);
  }

  // Just the direct contentId.
  private static boolean hasMatchingContentId(TinyJoinedAction join) {
    String insertionContentId = join.getJoinedImpression().getInsertion().getCore().getContentId();
    String actionContentId = join.getAction().getContentId();
    if (!insertionContentId.isEmpty()
        && !actionContentId.isEmpty()
        && insertionContentId.equals(actionContentId)) {
      return true;
    }
    String impressionContentId = join.getJoinedImpression().getImpression().getContentId();
    return !impressionContentId.isEmpty()
        && !actionContentId.isEmpty()
        && impressionContentId.equals(actionContentId);
  }

  @VisibleForTesting
  static boolean hasMatchingOtherContentId(TinyJoinedAction join) {
    String actionContentId = join.getAction().getContentId();
    Map<?, String> insertionOtherContentIdMap =
        join.getJoinedImpression().getInsertion().getCore().getOtherContentIdsMap();
    if (!actionContentId.isEmpty()) {
      if (insertionOtherContentIdMap.containsValue(actionContentId)) {
        return true;
      }
    }
    Map<?, String> actionOtherContentIdMap = join.getAction().getOtherContentIdsMap();

    String insertionContentId = join.getJoinedImpression().getInsertion().getCore().getContentId();
    if (!insertionContentId.isEmpty()
        && actionOtherContentIdMap.containsValue(insertionContentId)) {
      return true;
    }

    String impressionContentId = join.getJoinedImpression().getImpression().getContentId();
    if (!impressionContentId.isEmpty()
        && actionOtherContentIdMap.containsValue(impressionContentId)) {
      return true;
    }

    for (String actionOtherContentId : actionOtherContentIdMap.values()) {
      if (!actionOtherContentId.isEmpty()
          && insertionOtherContentIdMap.containsValue(actionOtherContentId)) {
        return true;
      }
    }

    return false;
  }
}
