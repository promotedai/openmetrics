package ai.promoted.metrics.logprocessor.job.join.action;

import ai.promoted.metrics.logprocessor.common.flink.operator.KeyedProcessOperatorWithWatermarkAddition;
import ai.promoted.metrics.logprocessor.common.flink.operator.ProcessOperatorWithWatermarkAddition;
import ai.promoted.metrics.logprocessor.common.functions.FilterOperator;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializablePredicate;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializablePredicates;
import ai.promoted.metrics.logprocessor.common.functions.inferred.IsImpressionAction;
import ai.promoted.metrics.logprocessor.common.job.BaseFlinkJob;
import ai.promoted.metrics.logprocessor.common.job.FlinkSegment;
import ai.promoted.metrics.logprocessor.common.table.FlinkTableUtils;
import ai.promoted.metrics.logprocessor.job.join.common.FilterToHighestPriority;
import ai.promoted.metrics.logprocessor.job.join.common.IntervalSqlUtil;
import ai.promoted.metrics.logprocessor.job.join.common.JoinUtil;
import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.TinyAction;
import ai.promoted.proto.event.TinyActionPath;
import ai.promoted.proto.event.TinyJoinedAction;
import ai.promoted.proto.event.TinyJoinedImpression;
import ai.promoted.proto.event.UnnestedTinyAction;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.GeneratedMessageV3;
import java.time.Duration;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine.Option;

// TODO - longer join window for navigates.
// TODO - split the outputs into multiple tables.
// TODO - a typo in `priority` is hard to debug.

/**
 * A Segment to encapsulates the Impression to Action join logic.
 *
 * <p>Does roughly the following:
 *
 * <ol>
 *   <li>Joins all "impression" actions (these are actions that happen on impressions) with
 *       impressions.
 *   <li>Joins all "post navigate" actions (like purchases) with joined navigates from the first
 *       step.
 * </ol>
 */
public class ActionJoinSegment implements FlinkSegment {
  private static final Logger LOGGER = LogManager.getLogger(ActionJoinSegment.class);

  // `content_id is the key since we split Actions that contain multiple content_ids.
  // Use Class KeySelector to avoid `InvalidTypesException` with lambdas.
  private static final KeySelector<TinyJoinedAction, Tuple3<Long, String, String>>
      actionIdContentIdKey =
          new KeySelector<>() {
            @Override
            public Tuple3<Long, String, String> getKey(TinyJoinedAction p) {
              return Tuple3.of(
                  p.getAction().getCommon().getPlatformId(),
                  p.getAction().getActionId(),
                  p.getAction().getContentId());
            }
          };

  @Option(
      names = {"--impressionActionJoinMin"},
      defaultValue = "-P1D",
      description =
          "Min range in Impression Action interval join. Default=-P1D. Java8 Duration parse format.")
  public Duration impressionActionJoinMin = Duration.parse("-P1D");

  @Option(
      names = {"--impressionActionJoinMaxOutOfOrder"},
      defaultValue = "PT0S",
      description =
          "Max out-of-order range in Impression Action interval join. Default=PT0S. Java8 Duration parse format.")
  public Duration impressionActionJoinMaxOutOfOrder = Duration.parse("PT0S");

  @Option(
      names = {"--impressionActionTypes"},
      description =
          "The ActionTypes that are performed directly on Impressions.  Most other ActionTypes are considered post-NAVIGATE.  Defaults to empty.")
  public EnumSet<ActionType> impressionActionTypes = EnumSet.noneOf(ActionType.class);

  @Option(
      names = {"--impressionCustomActionTypes"},
      description =
          "The custom (string) ActionTypes that are performed directly on Impressions.  Other custom ActionTypes are post-NAVIGATE.  Defaults to empty.")
  public Set<String> impressionCustomActionTypes = new HashSet();

  @Override
  public void validateArgs() {
    Preconditions.checkArgument(
        impressionActionJoinMin.isNegative(), "--impressionActionJoinMin should be negative");
    Preconditions.checkArgument(
        !impressionActionJoinMaxOutOfOrder.isNegative(),
        "--impressionActionJoinMaxOutOfOrder should not be negative");
    Preconditions.checkArgument(
        impressionActionJoinMin.abs().compareTo(impressionActionJoinMaxOutOfOrder.abs()) >= 0,
        "abs(--impressionActionJoinMin) needs to be greater than or equal to "
            + "abs(--impressionActionJoinMaxOutOfOrder).  If this is changed, the code to change "
            + "the watermark after the interval join would need changed.");
  }

  @Override
  public Set<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    return ImmutableSet.of();
  }

  /** Joins Impressions and Actions. */
  public ActionPathAndDropped joinActions(
      BaseFlinkJob job,
      StreamTableEnvironment tEnv,
      SingleOutputStreamOperator<TinyJoinedImpression> joinedImpressions,
      SingleOutputStreamOperator<UnnestedTinyAction> actions,
      boolean textLogWatermarks) {
    tEnv.createTemporaryFunction("contains_other_content_id", ContainsOtherContentId.class);
    joinedImpressions =
        job.addTextLogDebugIdsOperator(
            joinedImpressions, TinyJoinedImpression.class, "join-action-impression-input");
    actions =
        job.addTextLogDebugIdsOperator(
            actions, UnnestedTinyAction.class, "join-action-action-input");

    // TODO - debug ids.

    // 1. Create join rows for all Actions->Impressions.
    DataStream<TinyJoinedAction> joinedActions =
        joinActionsWithImpression(job, tEnv, joinedImpressions, actions, textLogWatermarks);

    // 2. Split by action stage.
    ImpressionActionSplit split = splitImpressionActions(job, joinedActions);

    // 3. Join ImpressionActions with Impressions.
    JoinedActionAndDropped impressionActionJoin =
        filterActionJoinsByPriority(split.impressionActions(), job, textLogWatermarks);

    // 4. Join PostNavigateActions with JoinedNavigates and Impressions.
    // Take all touchpoints of the best priority and create TinyActionPaths.
    SingleOutputStreamOperator<TinyJoinedAction> joinedNavigates =
        filterJoinsToNavigates(job, impressionActionJoin);
    SingleOutputStreamOperator<UnnestedTinyAction> postNavigateActions =
        filterRawToPostNavigateActions(job, actions);
    DataStream<TinyJoinedAction> navigateJoinedPostNavigateActions =
        joinWithNavigatesByKeys(tEnv, joinedNavigates, postNavigateActions, job, textLogWatermarks);
    DataStream<TinyJoinedAction> postNavigateJoinedActions = split.postNavigateActions();
    ActionPathAndDropped postNavigateJoinOutputs =
        filterPostNavigateActionPathsByPriority(
            postNavigateJoinedActions.union(navigateJoinedPostNavigateActions),
            job,
            textLogWatermarks);

    return unionOutputs(job, impressionActionJoin, postNavigateJoinOutputs);
  }

  private ActionPathAndDropped unionOutputs(
      BaseFlinkJob job,
      JoinedActionAndDropped impressionActionJoin,
      ActionPathAndDropped postNavigateJoinOutputs) {
    DataStream<TinyActionPath> postNavigateActionPaths = postNavigateJoinOutputs.actionPaths();
    DataStream<TinyActionPath> impressionActionPaths = toActionPaths(job, impressionActionJoin);
    DataStream<TinyActionPath> actionPaths = impressionActionPaths.union(postNavigateActionPaths);
    actionPaths =
        job.addTextLogDebugIdsOperatorAsDataStream(
            actionPaths, TinyActionPath.class, "join-action-action-path-output");
    DataStream<TinyAction> dropped =
        impressionActionJoin.droppedActions().union(postNavigateJoinOutputs.droppedActions());
    dropped =
        job.addTextLogDebugIdsOperatorAsDataStream(
            dropped, TinyAction.class, "join-action-dropped-action-output");
    return ActionPathAndDropped.create(actionPaths, dropped);
  }

  private DataStream<TinyActionPath> toActionPaths(
      BaseFlinkJob job, JoinedActionAndDropped impressionActionJoin) {
    DataStream<TinyActionPath> impressionActionPaths =
        job.add(
            impressionActionJoin.actions().map(new FromOneJoinedActionToActionPath()),
            "to-impression-action-path");
    return impressionActionPaths;
  }

  private SingleOutputStreamOperator<TinyJoinedAction> filterJoinsToNavigates(
      BaseFlinkJob job, JoinedActionAndDropped impressionActionJoin) {
    return job.add(
        impressionActionJoin
            .actions()
            .filter(a -> a.getAction().getActionType() == ActionType.NAVIGATE),
        "filter-to-navigates");
  }

  private SingleOutputStreamOperator<UnnestedTinyAction> filterRawToPostNavigateActions(
      BaseFlinkJob job, SingleOutputStreamOperator<UnnestedTinyAction> actions) {
    IsImpressionAction isImpressionAction =
        new IsImpressionAction(impressionActionTypes, impressionCustomActionTypes);
    SerializablePredicate isNotImpressionAction = SerializablePredicates.not(isImpressionAction);
    return job.add(
        actions.filter(unnestedAction -> isNotImpressionAction.test(unnestedAction.getAction())),
        "is-not-impression-action");
  }

  private DataStream<TinyJoinedAction> joinActionsWithImpression(
      BaseFlinkJob job,
      StreamTableEnvironment tEnv,
      SingleOutputStreamOperator<TinyJoinedImpression> joinedImpressions,
      SingleOutputStreamOperator<UnnestedTinyAction> unnestedActions,
      boolean textLogWatermarks) {
    FlinkTableUtils.registerProtoView(tEnv, joinedImpressions, "joined_impression");
    FlinkTableUtils.registerProtoView(tEnv, unnestedActions, "unnested_action");

    Schema joinedImpressionAvroSchema =
        FlinkTableUtils.protoToAvroSchema(TinyJoinedImpression.class);
    DataType joinedImpressionRowType =
        FlinkTableUtils.avroSchemaToDataType(joinedImpressionAvroSchema);
    String joinedImpressionFieldSql =
        JoinUtil.toSelectFieldSql(joinedImpressionRowType, "unnested_joined_impression.");
    String joinedImpressionSchema = JoinUtil.getSchemaSql(joinedImpressionRowType);
    Schema actionAvroSchema = FlinkTableUtils.protoToAvroSchema(TinyAction.class);
    DataType actionRowType = FlinkTableUtils.avroSchemaToDataType(actionAvroSchema);
    String actionWithImpressionIdFieldSql =
        JoinUtil.toSelectFieldSql(actionRowType, "unnested_action_with_impression_id.action.");
    String actionSchema = JoinUtil.getSchemaSql(actionRowType);

    // TODO - how does this work if the field paths are wrong?

    // 1. Join by (platform_id, impression_id)
    // Uses an outer join so the un-joined actions get sent to the reduction steps.
    // Split unnested_action before outer join since lots of empty impression ids lead to skew.
    String actionWithNotNullImpressionIdSql =
        "SELECT * FROM unnested_action WHERE unnested_action.action.impression_id IS NOT NULL";
    LOGGER.info(actionWithNotNullImpressionIdSql);
    tEnv.createTemporaryView(
        "unnested_action_with_impression_id", tEnv.sqlQuery(actionWithNotNullImpressionIdSql));
    Table unnestedJoinedImpressionTable =
        tEnv.sqlQuery(
            "SELECT\n"
                + "insertion AS insertion,\n"
                + "impression AS impression,\n"
                + "insertion.common.platform_id AS platform_id,\n"
                + "impression.impression_id AS impression_id,\n"
                + "insertion.insertion_id AS insertion_id,\n"
                + "COALESCE(insertion.common.anon_user_id, impression.common.anon_user_id) AS anon_user_id,\n"
                + "COALESCE(insertion.core.content_id, impression.content_id) AS content_id,"
                + "rowtime as rowtime\n"
                + "FROM joined_impression\n");
    tEnv.createTemporaryView("unnested_joined_impression", unnestedJoinedImpressionTable);
    String joinActionByImpressionIdSql =
        String.format(
            "SELECT\n"
                + "CAST(ROW(%s) AS %s) AS joined_impression,\n"
                + "CAST(ROW(%s) AS %s) AS action,\n"
                + "FALSE AS navigate_join,\n"
                + "unnested_action_with_impression_id.rowtime\n"
                + "FROM unnested_joined_impression\n"
                + "RIGHT OUTER JOIN unnested_action_with_impression_id\n"
                + "ON unnested_joined_impression.platform_id = unnested_action_with_impression_id.action.common.platform_id\n"
                + "AND unnested_joined_impression.impression_id = unnested_action_with_impression_id.action.impression_id\n"
                + "AND (unnested_joined_impression.rowtime >= unnested_action_with_impression_id.rowtime - %s)\n"
                + "AND (unnested_joined_impression.rowtime <= unnested_action_with_impression_id.rowtime + %s)\n"
                // TODO - make sure inputs specify platform_id.
                + "AND unnested_action_with_impression_id.action.common.platform_id <> 0\n",
            joinedImpressionFieldSql,
            joinedImpressionSchema,
            actionWithImpressionIdFieldSql,
            actionSchema,
            IntervalSqlUtil.fromDurationToSQLInterval(impressionActionJoinMin.negated()),
            IntervalSqlUtil.fromDurationToSQLInterval(impressionActionJoinMaxOutOfOrder));
    LOGGER.info(joinActionByImpressionIdSql);
    Table joinedActionByImpressionIdTable = tEnv.sqlQuery(joinActionByImpressionIdSql);

    String actionFieldSql = JoinUtil.toSelectFieldSql(actionRowType, "unnested_action.action.");
    String actionWithNullImpressionIdSql =
        String.format(
            "SELECT \n"
                // Xingcan: can also be "CAST(NULL AS %s) AS joined_impression"
                // The difference is NULL and {}
                + "CAST(ROW(%s) AS %s) AS joined_impression,\n"
                + "CAST(ROW(%s) AS %s) AS action,\n"
                + "FALSE as navigate_join,\n"
                + "unnested_action.rowtime\n"
                + "FROM unnested_action\n"
                + "WHERE unnested_action.action.impression_id IS NULL\n"
                + "AND unnested_action.action.common.platform_id <> 0",
            JoinUtil.toCastNullSql(joinedImpressionRowType),
            joinedImpressionSchema,
            actionFieldSql,
            actionSchema);
    LOGGER.info(actionWithNullImpressionIdSql);
    Table joinedActionWithEmptyImpressionIdTable = tEnv.sqlQuery(actionWithNullImpressionIdSql);
    // No need to advance watermark since the null padding stream is not joined.
    SingleOutputStreamOperator<TinyJoinedAction> joinedActionWithoutImpressionId =
        toTinyJoinedAction(joinedActionWithEmptyImpressionIdTable, tEnv, "has-no-impression-id");
    SingleOutputStreamOperator<TinyJoinedAction> joinedActionWithImpressionId =
        toTinyJoinedAction(joinedActionByImpressionIdTable, tEnv, "has-impression-id");
    joinedActionWithImpressionId =
        advanceIntervalJoinWatermark(
            joinedActionWithImpressionId, job, "has-impression-id-join-action", textLogWatermarks);

    // 2. Join by (platform_id, anon_user_id, content_id or other_content_id)
    // Inner join.  It only outputs matches.
    String selectHasAnonUserIdImpressionSql =
        String.format(
            "SELECT\n"
                + "CAST(ROW(%s) AS %s) AS joined_impression,\n"
                + "CAST(ROW(%s) AS %s) AS action,\n"
                + "FALSE AS navigate_join,\n"
                + "unnested_action.rowtime\n"
                + "FROM unnested_joined_impression\n"
                + "JOIN unnested_action\n"
                + "ON unnested_joined_impression.platform_id = unnested_action.action.common.platform_id\n"
                + "AND unnested_joined_impression.anon_user_id = unnested_action.action.common.anon_user_id\n"
                + "AND (unnested_joined_impression.content_id = unnested_action.content_id\n"
                + "    OR contains_other_content_id(unnested_joined_impression.insertion.core.other_content_ids, unnested_action.content_id))\n"
                + "AND (unnested_joined_impression.rowtime >= unnested_action.rowtime - %s)\n"
                + "AND (unnested_joined_impression.rowtime <= unnested_action.rowtime + %s)\n"
                + "WHERE unnested_action.action.common.platform_id <> 0\n"
                + "AND unnested_action.action.common.anon_user_id <> ''\n"
                + "AND unnested_action.action.common.anon_user_id IS NOT NULL\n"
                + "AND unnested_action.content_id IS NOT NULL\n"
                + "AND unnested_action.content_id <> ''",
            joinedImpressionFieldSql,
            joinedImpressionSchema,
            actionFieldSql,
            actionSchema,
            IntervalSqlUtil.fromDurationToSQLInterval(impressionActionJoinMin.negated()),
            IntervalSqlUtil.fromDurationToSQLInterval(impressionActionJoinMaxOutOfOrder));
    LOGGER.info(selectHasAnonUserIdImpressionSql);
    Table table2 = tEnv.sqlQuery(selectHasAnonUserIdImpressionSql);
    SingleOutputStreamOperator<TinyJoinedAction> joinedActionWithContentId =
        toTinyJoinedAction(table2, tEnv, "has-anon-user-id-content-id");
    joinedActionWithContentId =
        advanceIntervalJoinWatermark(
            joinedActionWithContentId, job, "has-anon-user-id-content-id", textLogWatermarks);

    return joinedActionWithoutImpressionId
        .union(joinedActionWithImpressionId)
        .union(joinedActionWithContentId);
  }

  // This is done as a separate operator so the attribution can prioritize navigates.
  private DataStream<TinyJoinedAction> joinWithNavigatesByKeys(
      StreamTableEnvironment tEnv,
      DataStream<TinyJoinedAction> postNavigateJoins,
      SingleOutputStreamOperator<UnnestedTinyAction> postNavigateActions,
      BaseFlinkJob job,
      boolean textLogWatermarks) {
    FlinkTableUtils.registerProtoView(tEnv, postNavigateJoins, "joined_navigate");
    FlinkTableUtils.registerProtoView(tEnv, postNavigateActions, "post_navigate_action");

    Schema joinedImpressionAvroSchema =
        FlinkTableUtils.protoToAvroSchema(TinyJoinedImpression.class);
    DataType joinedImpressionRowType =
        FlinkTableUtils.avroSchemaToDataType(joinedImpressionAvroSchema);
    String joinedImpressionFieldSql =
        JoinUtil.toSelectFieldSql(joinedImpressionRowType, "joined_navigate.joined_impression.");
    String joinedImpressionSchema = JoinUtil.getSchemaSql(joinedImpressionRowType);
    Schema actionAvroSchema = FlinkTableUtils.protoToAvroSchema(TinyAction.class);
    DataType actionRowType = FlinkTableUtils.avroSchemaToDataType(actionAvroSchema);
    String actionFieldSql =
        JoinUtil.toSelectFieldSql(actionRowType, "post_navigate_action.action.");
    String actionSchema = JoinUtil.getSchemaSql(actionRowType);

    // TODO - change the join windows for navigate.

    // 1. Join by (platform_id, anon_user_id, content_id)
    // Inner join.  The outer join is reused from the impression join stage.
    String selectHasAnonUserIdImpressionSql =
        String.format(
            "SELECT\n"
                // TODO - include the joined_navigate in the output too.
                + "CAST(ROW(%s) AS %s) AS joined_impression,\n"
                + "CAST(ROW(%s) AS %s) AS action,\n"
                + "true AS navigate_join,\n"
                + "post_navigate_action.rowtime\n"
                + "FROM joined_navigate\n"
                + "JOIN post_navigate_action\n"
                + "ON joined_impression.insertion.platform_id = post_navigate_action.action.common.platform_id\n"
                + "AND joined_impression.insertion.anon_user_id = post_navigate_action.action.common.anon_user_id\n"
                + "AND joined_impression.insertion.content_id = post_navigate_action.content_id\n"
                + "AND (joined_navigate.rowtime >= post_navigate_action.rowtime - %s)\n"
                + "AND (joined_navigate.rowtime <= post_navigate_action.rowtime + %s)\n"
                + "WHERE post_navigate_action.action.common.platform_id <> 0\n"
                + "AND post_navigate_action.action.common.anon_user_id IS NOT NULL\n"
                + "AND post_navigate_action.action.common.anon_user_id <> ''\n"
                + "AND post_navigate_action.content_id IS NOT NULL\n"
                + "AND post_navigate_action.content_id <> ''",
            joinedImpressionFieldSql,
            joinedImpressionSchema,
            actionFieldSql,
            actionSchema,
            IntervalSqlUtil.fromDurationToSQLInterval(impressionActionJoinMin.negated()),
            IntervalSqlUtil.fromDurationToSQLInterval(impressionActionJoinMaxOutOfOrder));
    LOGGER.info(selectHasAnonUserIdImpressionSql);
    Table table = tEnv.sqlQuery(selectHasAnonUserIdImpressionSql);

    SingleOutputStreamOperator<TinyJoinedAction> outputStream =
        toTinyJoinedAction(table, tEnv, "has-anon-user-id-content-id-navigate");
    outputStream =
        advanceIntervalJoinWatermark(
            outputStream, job, "join-post-navigate-action", textLogWatermarks);
    return outputStream;
  }

  /**
   * The watermark after the Flink SQL interval join needs shifted. The built-in interval join code
   * delays the watermark by the min window. The default implementation does not know that the event
   * time from the right-side is used. This operator can optimize it. JoinMin is negative so it
   * needs to be negated.
   */
  private SingleOutputStreamOperator<TinyJoinedAction> advanceIntervalJoinWatermark(
      SingleOutputStreamOperator<TinyJoinedAction> input,
      BaseFlinkJob job,
      String label,
      boolean textLogWatermarks) {
    String uid = "delay-" + label + "-watermark";
    return job.add(
        ProcessOperatorWithWatermarkAddition.addWatermarkAdvance(
            input,
            TypeInformation.of(TinyJoinedAction.class),
            impressionActionJoinMin.negated(),
            uid,
            textLogWatermarks),
        uid);
  }

  private ImpressionActionSplit splitImpressionActions(
      BaseFlinkJob job, DataStream<TinyJoinedAction> joinedActions) {
    IsImpressionAction isImpressionAction =
        new IsImpressionAction(impressionActionTypes, impressionCustomActionTypes);
    OutputTag<TinyJoinedAction> postNavigateActionOutputTag =
        new OutputTag<>("post-navigate-action", TypeInformation.of(TinyJoinedAction.class));
    SingleOutputStreamOperator<TinyJoinedAction> impressionActions =
        job.add(
            joinedActions.process(
                new FilterOperator<>(
                    a -> isImpressionAction.test(a.getAction()), postNavigateActionOutputTag)),
            "filter-is-impression-action");
    SideOutputDataStream<TinyJoinedAction> postNavigateActions =
        impressionActions.getSideOutput(postNavigateActionOutputTag);

    return ImpressionActionSplit.create(impressionActions, postNavigateActions);
  }

  /** Keys by `actionId` and filters the action joins to the most important (lowest priority). */
  private JoinedActionAndDropped filterActionJoinsByPriority(
      DataStream<TinyJoinedAction> allJoinedImpressionActions,
      BaseFlinkJob job,
      boolean textLogWatermarks) {
    FilterToHighestPriority<Tuple3<Long, String, String>, TinyJoinedAction, TinyJoinedAction>
        filter =
            new FilterToHighestPriority<>(
                TypeInformation.of(TinyJoinedAction.class),
                p -> p,
                JoinedImpressionActionComparatorSupplier::hasMatchingImpressionId,
                new JoinedImpressionActionComparatorSupplier(),
                p -> p.getJoinedImpression().getImpression().getImpressionId(),
                p -> p.getAction().getCommon().getEventApiTimestamp(),
                p -> p.getJoinedImpression().getImpression().getCommon().getEventApiTimestamp(),
                // Delay clean-up for late or duplicate events.
                impressionActionJoinMin.negated());

    // Delays the watermark by maxOutOfOrder.
    // The watermark needs to be delayed by -MaxOutOfOrder.  MaxOutOfOrder is positive so it
    // needs to be negated.
    SingleOutputStreamOperator<TinyJoinedAction> joinedImpressionActions =
        job.add(
            allJoinedImpressionActions
                .keyBy(actionIdContentIdKey)
                .transform(
                    "filter-highest-priority-joined-impression-action",
                    TypeInformation.of(TinyJoinedAction.class),
                    KeyedProcessOperatorWithWatermarkAddition.withDelay(
                        filter, impressionActionJoinMaxOutOfOrder, textLogWatermarks)),
            "filter-highest-priority-joined-impression-action");

    SingleOutputStreamOperator<TinyAction> dropped =
        job.add(
            joinedImpressionActions
                .getSideOutput(filter.getDroppedEventsTag())
                .map(TinyJoinedAction::getAction),
            "to-dropped-joined-impression-action");

    // TODO - withDebugIds.

    return JoinedActionAndDropped.create(joinedImpressionActions, dropped);
  }

  private ActionPathAndDropped filterPostNavigateActionPathsByPriority(
      DataStream<TinyJoinedAction> tinyJoinedActions, BaseFlinkJob job, boolean textLogWatermarks) {
    FilterToHighestPriorityList<Tuple3<Long, String, String>, TinyJoinedAction> filter =
        new FilterToHighestPriorityList<>(
            TypeInformation.of(TinyJoinedAction.class),
            new JoinedPostNavigateActionComparatorSupplier(),
            p -> p.getJoinedImpression().getImpression().getImpressionId(),
            p -> p.getAction().getCommon().getEventApiTimestamp(),
            p -> p.getJoinedImpression().getImpression().getCommon().getEventApiTimestamp(),
            // Delay clean-up for late or duplicate events.
            impressionActionJoinMin.negated());

    // Delays the watermark by maxOutOfOrder.
    // The watermark needs to be delayed by -MaxOutOfOrder.  MaxOutOfOrder is positive so it
    // needs to be negated.
    // The two layers of joins (->navigate->postNavigate) will have two out of order waits.
    SingleOutputStreamOperator<List<TinyJoinedAction>> joinedActions =
        job.add(
            tinyJoinedActions
                .keyBy(actionIdContentIdKey)
                .transform(
                    "filter-highest-priority-post-action-joins-transform",
                    Types.LIST(TypeInformation.of(TinyJoinedAction.class)),
                    KeyedProcessOperatorWithWatermarkAddition.withDelay(
                        filter, impressionActionJoinMaxOutOfOrder, textLogWatermarks)),
            "filter-highest-priority-post-action-joins");

    FromListToActionPath fromListToActionPath = new FromListToActionPath();
    SingleOutputStreamOperator<TinyActionPath> actionPaths =
        job.add(joinedActions.process(fromListToActionPath), "to-action-path");
    DataStream<TinyAction> dropped = actionPaths.getSideOutput(FromListToActionPath.DROPPED_TAG);

    // TODO - withDebugIds.

    return ActionPathAndDropped.create(actionPaths, dropped);
  }

  private SingleOutputStreamOperator<TinyJoinedAction> toTinyJoinedAction(
      Table table, StreamTableEnvironment tEnv, String label) {
    // TODO - plumb through job.
    return FlinkTableUtils.toProtoStreamWithRowtime(
        tEnv, table, TinyJoinedAction.class, "to-" + label + "-prioritized-tiny-joined-action");
  }

  public static class ContainsOtherContentId extends ScalarFunction {
    public final boolean eval(
        @DataTypeHint("ARRAY<ROW<key INT, value STRING> NOT NULL>") Row[] array, String value) {
      if (null == array || 0 == array.length) {
        return false;
      }
      return Arrays.stream(array).anyMatch(row -> value.equals(row.getFieldAs("value")));
    }
  }
}
