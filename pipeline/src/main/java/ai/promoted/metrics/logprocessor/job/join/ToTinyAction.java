package ai.promoted.metrics.logprocessor.job.join;

import ai.promoted.metrics.logprocessor.common.util.TinyFlatUtil;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.CartContent;
import ai.promoted.proto.event.TinyAction;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

final class ToTinyAction implements FlatMapFunction<Action, TinyAction> {
  private final OtherContentIdsConverter otherContentIdsConverter;

  ToTinyAction(List<String> requestInsertionOtherContentIdKeys) {
    this.otherContentIdsConverter =
        new OtherContentIdsConverter(requestInsertionOtherContentIdKeys);
  }

  @Override
  public void flatMap(Action action, Collector<TinyAction> out) throws Exception {
    if (shouldSplitByCartContentIds(action)) {
      for (String contentId : getBaseAndCartContentIds(action)) {
        // Clear the foreign keys since they're probably wrong.
        // TODO - if the cartContent.contentId matches the Action's primary contentId, we might be
        // able
        // to keep the foreign keys.  It might get misused by clients.
        TinyAction.Builder tinyAction =
            TinyFlatUtil.toTinyActionBuilder(action, contentId)
                .clearImpressionId()
                .clearInsertionId()
                .clearRequestId()
                .clearViewId();
        if (otherContentIdsConverter.hasKeys() && action.hasProperties()) {
          otherContentIdsConverter.putFromProperties(
              tinyAction::putOtherContentIds, action.getProperties());
        }
        out.collect(tinyAction.build());
      }
    } else {
      TinyAction.Builder tinyAction =
          TinyFlatUtil.toTinyActionBuilder(action, action.getContentId());
      if (otherContentIdsConverter.hasKeys() && action.hasProperties()) {
        otherContentIdsConverter.putFromProperties(
            tinyAction::putOtherContentIds, action.getProperties());
      }
      out.collect(tinyAction.build());
    }
  }

  private static boolean shouldSplitByCartContentIds(Action action) {
    ActionType actionType = action.getActionType();
    switch (actionType) {
      case CHECKOUT:
      case PURCHASE:
        return action.getCart().getContentsCount() > 0;
      default:
        return false;
    }
  }

  private static Set<String> getBaseAndCartContentIds(Action action) {
    Set<String> contentIds = new HashSet<>();
    if (!action.getContentId().isEmpty()) {
      contentIds.add(action.getContentId());
    }
    // TODO - decide if we should look at singleItemCart.  We don't expose it yet so we don't need
    // to.
    // Might make sense to add it for ADD_TO_CART, etc.
    for (CartContent content : action.getCart().getContentsList()) {
      if (!content.getContentId().isEmpty()) {
        contentIds.add(content.getContentId());
      }
    }
    return contentIds;
  }
}
