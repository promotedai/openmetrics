package ai.promoted.metrics.logprocessor.job.join.action;

import ai.promoted.proto.event.TinyJoinedAction;
import java.util.Comparator;

public class JoinedPostNavigateActionComparatorSupplier extends BaseJoinedActionComparatorSupplier {

  // Higher is more important.
  public Comparator<TinyJoinedAction> get() {
    // TODO - optimize for parameters.  E.g. is other_content_ids enabled?  E.g. retained_user_id
    // enabled?
    return Comparator.comparing(BaseJoinedActionComparatorSupplier::hasMatchingImpressionId)
        .thenComparing(hasMatchingAnonUserIdAndContentIdsComparator())
        .thenComparing(TinyJoinedAction::getNavigateJoin);
  }
}
