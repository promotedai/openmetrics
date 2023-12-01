package ai.promoted.metrics.logprocessor.job.join.impression;

import ai.promoted.metrics.logprocessor.common.flink.operator.KeyedProcessOperatorWithWatermarkAddition;
import ai.promoted.metrics.logprocessor.common.flink.operator.ProcessOperatorWithWatermarkAddition;
import ai.promoted.metrics.logprocessor.common.job.BaseFlinkJob;
import ai.promoted.metrics.logprocessor.common.job.FlinkSegment;
import ai.promoted.metrics.logprocessor.common.table.FlinkTableUtils;
import ai.promoted.metrics.logprocessor.job.join.common.FilterToHighestPriority;
import ai.promoted.metrics.logprocessor.job.join.common.IntervalSqlUtil;
import ai.promoted.metrics.logprocessor.job.join.common.JoinUtil;
import ai.promoted.proto.event.TinyImpression;
import ai.promoted.proto.event.TinyInsertion;
import ai.promoted.proto.event.TinyJoinedImpression;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.GeneratedMessageV3;
import java.time.Duration;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine.Option;

// TODO - support retractions.

/** A Segment to encapsulates the Insertion to Impression join logic. */
public class ImpressionJoinSegment implements FlinkSegment {
  private static final Logger LOGGER = LogManager.getLogger(ImpressionJoinSegment.class);

  // Use Class KeySelector to avoid `InvalidTypesException` with lambdas.
  private static final KeySelector<TinyJoinedImpression, Tuple2<Long, String>>
      prioritizedInsertionKey =
          new KeySelector<>() {
            @Override
            public Tuple2<Long, String> getKey(TinyJoinedImpression p) {
              return Tuple2.of(
                  p.getImpression().getCommon().getPlatformId(),
                  p.getImpression().getImpressionId());
            }
          };

  @Option(
      names = {"--insertionImpressionJoinMin"},
      defaultValue = "-PT30M",
      description =
          "Min range in Insertion Impression interval join. Default=-PT30M. Java8 Duration parse format.")
  public Duration insertionImpressionJoinMin = Duration.parse("-PT30M");

  @Option(
      names = {"--insertionImpressionJoinMaxOutOfOrder"},
      defaultValue = "PT0S",
      description =
          "Max out-of-order range in Insertion Impression interval join. Default=PT0S. Java8 Duration parse format.")
  public Duration insertionImpressionJoinMaxOutOfOrder = Duration.parse("PT0S");

  public ImpressionJoinSegment() {}

  @Override
  public void validateArgs() {
    Preconditions.checkArgument(
        insertionImpressionJoinMin.isNegative(), "--insertionImpressionJoinMin should be negative");
    Preconditions.checkArgument(
        !insertionImpressionJoinMaxOutOfOrder.isNegative(),
        "--insertionImpressionJoinMaxOutOfOrder should not be negative");
    Preconditions.checkArgument(
        insertionImpressionJoinMin.abs().compareTo(insertionImpressionJoinMaxOutOfOrder.abs()) >= 0,
        "abs(--insertionImpressionJoinMin) needs to be greater than or equal to "
            + "abs(--insertionImpressionJoinMaxOutOfOrder).  If this is changed, the code to change "
            + "the watermark after the interval join would need changed.");
  }

  @Override
  public Set<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    return ImmutableSet.of(TinyJoinedImpression.class);
  }

  /** Joins Insertions and Impressions. */
  public ImpressionJoinOutputs joinImpressions(
      BaseFlinkJob job,
      StreamTableEnvironment tEnv,
      SingleOutputStreamOperator<TinyInsertion> insertions,
      SingleOutputStreamOperator<TinyImpression> impressions,
      boolean textLogWatermarks) {
    insertions =
        job.addTextLogDebugIdsOperator(
            insertions, TinyInsertion.class, "join-impression-insertion-input");
    impressions =
        job.addTextLogDebugIdsOperator(
            impressions, TinyImpression.class, "join-impression-impression-input");

    // 1. Create separate join operators for each type of join.
    DataStream<TinyJoinedImpression> joinedImpressions =
        joinByKeys(job, tEnv, insertions, impressions, textLogWatermarks);

    // 2. Key by the impression primary key and pick the highest priority join.
    return filterImpressionJoinByPriority(joinedImpressions, job, textLogWatermarks);
  }

  private DataStream<TinyJoinedImpression> joinByKeys(
      BaseFlinkJob job,
      StreamTableEnvironment tEnv,
      SingleOutputStreamOperator<TinyInsertion> insertions,
      SingleOutputStreamOperator<TinyImpression> impressions,
      boolean textLogWatermarks) {
    FlinkTableUtils.registerProtoView(tEnv, insertions, "insertion");
    FlinkTableUtils.registerProtoView(tEnv, impressions, "impression");

    Schema tinyInsertionAvroSchema = FlinkTableUtils.protoToAvroSchema(TinyInsertion.class);
    DataType tinyInsertionRowType = FlinkTableUtils.avroSchemaToDataType(tinyInsertionAvroSchema);
    String insertionFieldSql = JoinUtil.toSelectFieldSql(tinyInsertionRowType, "insertion.");
    String insertionSchema = JoinUtil.getSchemaSql(tinyInsertionRowType);
    Schema tinyImpressionAvroSchema = FlinkTableUtils.protoToAvroSchema(TinyImpression.class);
    DataType tinyImpressionRowType = FlinkTableUtils.avroSchemaToDataType(tinyImpressionAvroSchema);
    String impressionFieldSql = JoinUtil.toSelectFieldSql(tinyImpressionRowType, "impression.");
    String impressionSchema = JoinUtil.getSchemaSql(tinyImpressionRowType);

    // TODO - a typo in `priority` is hard to debug.

    // 1. Join by (platform_id, insertion_id)
    // Uses an outer join so the un-joined impressions get sent to the Filter operation.
    String selectHasInsertionIdImpressionSql =
        String.format(
            "SELECT\n"
                + "CAST(ROW(%s) AS %s) AS insertion,\n"
                + "CAST(ROW(%s) AS %s) AS impression,\n"
                + "impression.rowtime\n"
                + "FROM insertion\n"
                + "RIGHT OUTER JOIN impression\n"
                + "ON insertion.platform_id = impression.platform_id\n"
                + "AND insertion.insertion_id = impression.insertion_id\n"
                + "AND (insertion.rowtime >= impression.rowtime - %s)\n"
                + "AND (insertion.rowtime <= impression.rowtime + %s)\n"
                + "AND impression.insertion_id <> ''"
                + "AND impression.insertion_id IS NOT NULL",
            insertionFieldSql,
            insertionSchema,
            impressionFieldSql,
            impressionSchema,
            IntervalSqlUtil.fromDurationToSQLInterval(insertionImpressionJoinMin.negated()),
            IntervalSqlUtil.fromDurationToSQLInterval(insertionImpressionJoinMaxOutOfOrder));
    LOGGER.info(selectHasInsertionIdImpressionSql);
    Table table1 = tEnv.sqlQuery(selectHasInsertionIdImpressionSql);
    SingleOutputStreamOperator<TinyJoinedImpression> outputStream =
        toTinyJoinedImpression(table1, job, tEnv, "has-insertion-id");

    // TODO - plumb textLogWatermarks.
    // The watermark needs shifted here.
    // The built-in interval join code delays the watermark by the min window.
    // The default implementation does not know that the event time from the right-side is used.
    // This operator can optimize it.  JoinMin is negative so it needs to be negated.
    outputStream =
        job.add(
            ProcessOperatorWithWatermarkAddition.addWatermarkDelay(
                outputStream,
                TypeInformation.of(TinyJoinedImpression.class),
                insertionImpressionJoinMin.negated(),
                "delay-has-insertion-id-join-impression-watermark",
                textLogWatermarks),
            "delay-has-insertion-id-join-impression-watermark");

    // 2. Join by (platform_id, anon_user_id, content_id)
    // Inner join.  It only outputs matches.
    String selectHasAnonUserIdImpressionSql =
        String.format(
            "SELECT\n"
                + "CAST(ROW(%s) AS %s) AS insertion,\n"
                + "CAST(ROW(%s) AS %s) AS impression,\n"
                + "impression.rowtime\n"
                + "FROM insertion\n"
                + "JOIN impression\n"
                + "ON insertion.platform_id = impression.platform_id\n"
                + "AND insertion.anon_user_id = impression.anon_user_id\n"
                + "AND insertion.content_id = impression.content_id\n"
                + "AND (insertion.rowtime >= impression.rowtime - %s)\n"
                + "AND (insertion.rowtime <= impression.rowtime + %s)\n"
                + "WHERE insertion.anon_user_id <> ''\n"
                + "AND insertion.anon_user_id IS NOT NULL\n"
                + "AND impression.anon_user_id <> ''\n"
                + "AND impression.anon_user_id IS NOT NULL\n"
                + "AND insertion.content_id <> ''\n"
                + "AND insertion.content_id IS NOT NULL\n"
                + "AND impression.content_id <> ''\n"
                + "AND impression.content_id IS NOT NULL\n",
            insertionFieldSql,
            insertionSchema,
            impressionFieldSql,
            impressionSchema,
            IntervalSqlUtil.fromDurationToSQLInterval(insertionImpressionJoinMin.negated()),
            IntervalSqlUtil.fromDurationToSQLInterval(insertionImpressionJoinMaxOutOfOrder));
    LOGGER.info(selectHasAnonUserIdImpressionSql);
    Table table2 = tEnv.sqlQuery(selectHasAnonUserIdImpressionSql);
    SingleOutputStreamOperator<TinyJoinedImpression> outputStream2 =
        toTinyJoinedImpression(table2, job, tEnv, "has-anon-user-id-content-id");

    // The watermark needs shifted here.
    // The built-in interval join code delays the watermark by the min window.
    // The default implementation does not know that the event time from the right-side is used.
    // This operator can optimize it.  JoinMin is negative so it needs to be negated.
    outputStream2 =
        job.add(
            ProcessOperatorWithWatermarkAddition.addWatermarkDelay(
                outputStream2,
                TypeInformation.of(TinyJoinedImpression.class),
                insertionImpressionJoinMin.negated(),
                "delay-has-anon-user-id-content-id-join-impression-watermark",
                textLogWatermarks),
            "delay-has-anon-user-id-content-id-join-impression-watermark");

    return outputStream.union(outputStream2);
  }

  /**
   * Keys by `impressionId` and filters the impression joins to the most important (lowest
   * priority).
   */
  private ImpressionJoinOutputs filterImpressionJoinByPriority(
      DataStream<TinyJoinedImpression> joinImpressions,
      BaseFlinkJob job,
      boolean textLogWatermarks) {
    FilterToHighestPriority<Tuple2<Long, String>, TinyJoinedImpression, TinyJoinedImpression>
        filter =
            new FilterToHighestPriority<>(
                TypeInformation.of(TinyJoinedImpression.class),
                p -> p,
                JoinedImpressionComparatorSupplier::hasMatchingInsertionId,
                new JoinedImpressionComparatorSupplier(),
                p -> p.getInsertion().getCore().getInsertionId(),
                p -> p.getImpression().getCommon().getEventApiTimestamp(),
                p -> p.getInsertion().getCommon().getEventApiTimestamp(),
                // Delay clean-up for late or duplicate events.
                insertionImpressionJoinMin.negated());

    // The watermark needs to be delayed by -MaxOutOfOrder.  MaxOutOfOrder is positive so it
    // needs to be negated.
    SingleOutputStreamOperator<TinyJoinedImpression> joinedImpressions =
        job.add(
            joinImpressions
                .keyBy(prioritizedInsertionKey)
                .transform(
                    "filter-insertion-impression-join",
                    TypeInformation.of(TinyJoinedImpression.class),
                    KeyedProcessOperatorWithWatermarkAddition.withDelay(
                        filter, insertionImpressionJoinMaxOutOfOrder, textLogWatermarks)),
            "filter-insertion-impression-join");

    SingleOutputStreamOperator<TinyImpression> dropped =
        job.add(
            joinedImpressions
                .getSideOutput(filter.getDroppedEventsTag())
                .map(TinyJoinedImpression::getImpression),
            "dropped-get-impression");

    joinedImpressions =
        job.addTextLogDebugIdsOperator(
            joinedImpressions,
            TinyJoinedImpression.class,
            "impression-join-joined-impression-output");
    dropped =
        job.addTextLogDebugIdsOperator(
            dropped, TinyImpression.class, "impression-join-dropped-impression-output");

    return ImpressionJoinOutputs.create(joinedImpressions, dropped);
  }

  private SingleOutputStreamOperator<TinyJoinedImpression> toTinyJoinedImpression(
      Table table, BaseFlinkJob job, StreamTableEnvironment tEnv, String label) {
    return FlinkTableUtils.toProtoStreamWithRowtime(
        tEnv, table, TinyJoinedImpression.class, "to-" + label + "-matched-tiny-joined-impression");
  }
}
