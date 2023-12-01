package ai.promoted.metrics.logprocessor.job.join.action;

import ai.promoted.proto.event.TinyJoinedAction;
import java.util.Comparator;

public class JoinedImpressionActionComparatorSupplier extends BaseJoinedActionComparatorSupplier {

  // Higher is more important.
  public Comparator<TinyJoinedAction> get() {
    // The direct foreign key is handled through a separate check.
    // TODO - optimize for parameters.  E.g. is other_content_ids enabled?  E.g. retained_user_id
    // enabled?
    return hasMatchingAnonUserIdAndContentIdsComparator()
        .thenComparing(TinyJoinedAction::getNavigateJoin);
  }
}
