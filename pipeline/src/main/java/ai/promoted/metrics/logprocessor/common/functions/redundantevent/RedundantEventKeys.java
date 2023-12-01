package ai.promoted.metrics.logprocessor.common.functions.redundantevent;

import ai.promoted.proto.event.TinyAction;
import ai.promoted.proto.event.TinyActionPath;
import ai.promoted.proto.event.TinyInsertion;
import ai.promoted.proto.event.TinyJoinedImpression;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;

/**
 * The key to use for removing redundant events (insertions, impressions and actions).
 *
 * <p>The primary goal for the key design is to remove redundant impressions. This can happen for a
 * few reasons:
 *
 * <ul>
 *   <li>Auto-refreshing pages.
 *   <li>Loading a previous page (reloading or going back).
 *   <li>Reloading pages using cached Delivery responses.
 *   <li>Bugs in clients and SDKs where events are logged multiple times.
 *   <li>RPC retries. They should have the same record IDs though There are currently unrelated bugs
 *       around this in Flink.
 *   <li>Bots.
 * </ul>
 *
 * <p>A secondary goal for the key design is to have better support for MRC impressions. <a
 * href="http://www.mediaratingcouncil.org/063014%20Viewable%20Ad%20Impression%20Guideline_Final.pdf">Spec</a>.
 *
 * <p>There is not a perfect, common key design that works across all marketplaces. This will
 * eventually need to be customized for client.
 */
public final class RedundantEventKeys {

  private RedundantEventKeys() {}

  /**
   * Returns the operator key for {@code ReducingRedundantTinyImpression}. See that class for more
   * details for why some of the redundant key dimensions are in this operator key vs internally in
   * {@code getKey}.
   *
   * <p>(platformId, anonUserId, insertionContentId).
   */
  public static Tuple3<Long, String, String> forRedundantImpressionInImpressionStream(
      TinyJoinedImpression event) {
    return forRedundantImpressions(event.getInsertion());
  }

  // Part of this key is here.  More dimensions get keyed inside the operator.
  // (platformId, anonUserId, insertionContentId).

  /**
   * Returns the operator key for {@code ReducingRedundantTinyImpression}.
   *
   * <p>(platformId, anonUserId, insertionContentId).
   */
  public static Tuple3<Long, String, String> forRedundantImpressionInActionPathStream(
      TinyActionPath actionPath) {
    // TODO - restructure this when we support attribution across multiple content types.
    if (actionPath.getTouchpointsCount() > 0) {
      // Use one of the Insertion's content_ids so this joins correctly.
      return forRedundantImpressions(
          actionPath.getTouchpoints(0).getJoinedImpression().getInsertion());
    } else {
      // TODO - a larger restructuring needs to be done to support more flexible content ID joins.
      return Tuple3.of(
          actionPath.getAction().getCommon().getPlatformId(),
          actionPath.getAction().getCommon().getAnonUserId(),
          actionPath.getAction().getContentId());
    }
  }

  private static Tuple3<Long, String, String> forRedundantImpressions(TinyInsertion insertion) {
    return Tuple3.of(
        insertion.getCommon().getPlatformId(),
        insertion.getCommon().getAnonUserId(),
        insertion.getCore().getContentId());
  }

  /**
   * Returns the operator key for {@code ReducingRedundantTinyAction} for IsImpression Actions.
   *
   * <p>(platformId, anonUserId, insertionId, actionType, customActionType).
   */
  public static Tuple5<Long, String, String, Integer, String>
      forRedundantActionsInImpressionActionPathStream(TinyActionPath actionPath) {
    // TODO - restructure this when we support attribution across multiple content types.
    String insertionId;
    if (actionPath.getTouchpointsCount() > 0) {
      insertionId =
          actionPath
              .getTouchpoints(0)
              .getJoinedImpression()
              .getInsertion()
              .getCore()
              .getInsertionId();
    } else {
      // TODO - a larger restructuring needs to be done to support more flexible content ID joins.
      // This probably won't work.
      insertionId = actionPath.getAction().getInsertionId();
    }
    TinyAction action = actionPath.getAction();
    return Tuple5.of(
        action.getCommon().getPlatformId(),
        action.getCommon().getAnonUserId(),
        insertionId,
        action.getActionType().getNumber(),
        action.getCustomActionType());
  }

  /**
   * Returns the operator key for {@code ReducingRedundantTinyAction} for PostNavigate Actions.
   *
   * <p>(platformId, anonUserId, insertionContentId, actionType, customActionType).
   */
  public static Tuple5<Long, String, String, Integer, String>
      forRedundantActionsInPostNavigateActionStream(TinyActionPath actionPath) {
    // TODO - restructure this when we support attribution across multiple content types.
    String contentId;
    if (actionPath.getTouchpointsCount() > 0) {
      contentId =
          actionPath
              .getTouchpoints(0)
              .getJoinedImpression()
              .getInsertion()
              .getCore()
              .getContentId();
    } else {
      // TODO - a larger restructuring needs to be done to support more flexible content ID joins.
      contentId = actionPath.getAction().getContentId();
    }
    TinyAction action = actionPath.getAction();
    return Tuple5.of(
        action.getCommon().getPlatformId(),
        action.getCommon().getAnonUserId(),
        contentId,
        action.getActionType().getNumber(),
        action.getCustomActionType());
  }
}
