package ai.promoted.metrics.logprocessor.job.contentquerymetrics;

import ai.promoted.metrics.logprocessor.common.job.BaseFlinkJob;
import ai.promoted.metrics.logprocessor.common.job.DirectFlatOutputKafkaSource;
import ai.promoted.metrics.logprocessor.common.job.FlatOutputKafkaSegment;
import ai.promoted.metrics.logprocessor.common.job.FlatOutputKafkaSource;
import ai.promoted.metrics.logprocessor.common.job.FlinkSegment;
import ai.promoted.metrics.logprocessor.common.job.KafkaSegment;
import ai.promoted.metrics.logprocessor.common.job.KafkaSourceSegment;
import ai.promoted.metrics.logprocessor.common.job.KeepFirstSegment;
import ai.promoted.metrics.logprocessor.common.job.ResourceLoader;
import ai.promoted.metrics.logprocessor.common.job.S3FileOutput;
import ai.promoted.metrics.logprocessor.common.job.S3Segment;
import ai.promoted.metrics.logprocessor.common.table.JoinedEventTableSourceSegment;
import ai.promoted.metrics.logprocessor.common.table.Tables;
import ai.promoted.metrics.logprocessor.common.util.StringUtil;
import ai.promoted.proto.event.AttributedAction;
import ai.promoted.proto.event.JoinedImpression;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

@CommandLine.Command(
    name = "contentquerymetrics",
    mixinStandardHelpOptions = true,
    version = "contentquerymetrics 1.0.0",
    description =
        "Creates a Flink job that produces content query metrics tables by aggregating engagement records from Kafka and writes records to sinks.")
public class ContentQueryMetricsJob extends BaseFlinkJob {
  private static final Logger LOGGER = LogManager.getLogger(ContentQueryMetricsJob.class);

  @CommandLine.Mixin public final KafkaSegment kafkaSegment = new KafkaSegment();

  @CommandLine.Mixin
  public final KafkaSourceSegment kafkaSourceSegment = new KafkaSourceSegment(this, kafkaSegment);

  @CommandLine.Mixin
  public final FlatOutputKafkaSegment flatOutputKafkaSegment =
      new FlatOutputKafkaSegment(this, kafkaSegment);

  @CommandLine.Mixin
  public final DirectFlatOutputKafkaSource directFlatOutputKafkaSource =
      new DirectFlatOutputKafkaSource(kafkaSourceSegment, flatOutputKafkaSegment);

  @CommandLine.Mixin public final KeepFirstSegment keepFirstSegment = new KeepFirstSegment(this);

  private final FlatOutputKafkaSource flatOutputKafkaSource =
      new FlatOutputKafkaSource(
          directFlatOutputKafkaSource, flatOutputKafkaSegment, keepFirstSegment);

  @CommandLine.Mixin public final S3Segment s3 = new S3Segment(this);
  @CommandLine.Mixin public final S3FileOutput s3FileOutput = new S3FileOutput(this, s3);

  public final JoinedEventTableSourceSegment joinedEventTableSourceSegment =
      new JoinedEventTableSourceSegment(this);

  @CommandLine.Option(
      names = {"--daily"},
      negatable = true,
      description = "Whether to output daily files.  Default=false")
  public boolean daily = false;

  @CommandLine.Option(
      names = {"--weekly"},
      negatable = true,
      description = "Whether to output weekly files.  Default=false")
  public boolean weekly = false;

  @CommandLine.Option(
      names = {"--outputParquet"},
      negatable = true,
      description = "Whether to output parquet files.  Default=false")
  public boolean outputParquet = false;

  @CommandLine.Option(
      names = {"--outputCsv"},
      negatable = true,
      description = "Whether to output csv files.  Default=false")
  public boolean outputCsv = false;

  public static void main(String[] args) {
    executeMain(new ContentQueryMetricsJob(), args);
  }

  @Override
  public Set<FlinkSegment> getInnerFlinkSegments() {
    return ImmutableSet.of(
        kafkaSegment,
        kafkaSourceSegment,
        flatOutputKafkaSegment,
        directFlatOutputKafkaSource,
        flatOutputKafkaSource,
        s3,
        s3FileOutput,
        joinedEventTableSourceSegment);
  }

  @Override
  protected String getDefaultBaseJobName() {
    return "content-query-metrics";
  }

  @Override
  protected void startJob() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    configureExecutionEnvironment(env, parallelism, maxParallelism);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    tableEnv.getConfig().setLocalTimeZone(ZoneOffset.UTC);

    Tables.createCatalogAndDatabase(tableEnv);
    DataStream<JoinedImpression> joinedImpressions =
        flatOutputKafkaSource.getJoinedImpressionSource(
            env, toKafkaConsumerGroupId("content-query-metrics"));
    DataStream<AttributedAction> attributedActions =
        flatOutputKafkaSource.getAttributedActionSource(
            env, toKafkaConsumerGroupId("content-query-metrics"));
    createOperators(tableEnv, joinedImpressions, attributedActions);
    LOGGER.info("ContentQueryMetricsJob.executionPlan\n{}", env.getExecutionPlan());
  }

  @VisibleForTesting
  List<TableResult> createOperators(
      StreamTableEnvironment tableEnv,
      DataStream<JoinedImpression> joinedImpressions,
      DataStream<AttributedAction> attributedActions) {
    tableEnv.getConfig().getConfiguration().setString("pipeline.name", getJobName());

    joinedEventTableSourceSegment.createJoinedImpressionTable(tableEnv, joinedImpressions);
    joinedEventTableSourceSegment.createAttributedActionTable(tableEnv, attributedActions);

    StreamStatementSet statementSet = tableEnv.createStatementSet();
    executeSqlFromResource(tableEnv::executeSql, "1_create_unified_event_stream.sql");
    boolean hasOutput = false;

    if (daily) {
      Map<String, String> parameters =
          ImmutableMap.of(
              "periodLabel", "daily",
              "periodDays", "1",
              "periodOffsetDays", "0");
      var result = executeSqlForPeriod(tableEnv, statementSet, parameters);
      statementSet = result.f0;
      hasOutput |= result.f1;
    }
    if (weekly) {
      Map<String, String> parameters =
          ImmutableMap.of(
              "periodLabel", "weekly",
              "periodDays", "7",
              // To align to Sunday.
              "periodOffsetDays", "3");
      var result = executeSqlForPeriod(tableEnv, statementSet, parameters);
      statementSet = result.f0;
      hasOutput |= result.f1;
    }

    Preconditions.checkState(hasOutput, "At least one output flag needs to be specified");

    // TODO - switch query away from rowtime and to event api time.
    // TODO - include platform_id in the join.

    return ImmutableList.of(statementSet.execute());
  }

  private Tuple2<StreamStatementSet, Boolean> executeSqlForPeriod(
      StreamTableEnvironment tableEnv,
      StreamStatementSet statementSet,
      Map<String, String> parameters) {
    boolean hasOutput = false;
    if (outputParquet || outputCsv) {
      executeSqlFromResource(
          tableEnv::executeSql, "2_create_view_content_query_joined_metrics.sql", parameters);
    }
    if (outputParquet) {
      executeSqlFromResource(
          tableEnv::executeSql, "3_create_table_content_query_joined_metrics.sql", parameters);
      statementSet =
          executeSqlFromResource(
              statementSet::addInsertSql,
              "4_insert_into_content_query_joined_metrics.sql",
              parameters);
      hasOutput = true;
    }
    if (outputCsv) {
      executeSqlFromResource(
          tableEnv::executeSql, "5_create_table_content_query_joined_metrics_csv.sql", parameters);
      statementSet =
          executeSqlFromResource(
              statementSet::addInsertSql,
              "6_insert_into_content_query_joined_metrics_csv.sql",
              parameters);
      hasOutput = true;
    }
    return Tuple2.of(statementSet, hasOutput);
  }

  /** Returns the SQL string with templated parameters filled in. */
  private String getResolvedSqlString(String queryFile, Map<String, String> parameters) {
    String query = ResourceLoader.getQueryFromResource(ContentQueryMetricsJob.class, queryFile);

    Map<String, String> formatParameters =
        ImmutableMap.<String, String>builder()
            .put("jobName", getJobName())
            .put("rootPath", s3.getOutputDir().build().toString())
            .put("sinkParallelism", Integer.toString(getSinkParallelism("sink")))
            .put("kafkaBootstrapServers", kafkaSegment.bootstrapServers)
            .putAll(parameters)
            .build();
    try {
      return StringUtil.replace(query, formatParameters);
    } catch (Exception e) {
      throw new IllegalArgumentException("Error formatting SQL " + queryFile, e);
    }
  }

  private <R> R executeSqlFromResource(Function<String, R> executeSql, String queryFile) {
    return this.executeSqlFromResource(executeSql, queryFile, ImmutableMap.of());
  }

  private <R> R executeSqlFromResource(
      Function<String, R> executeSql, String queryFile, Map<String, String> parameters) {
    String sql = getResolvedSqlString(queryFile, parameters);
    try {
      return executeSql.apply(sql);
    } catch (Exception e) {
      throw new RuntimeException("executeSql error for " + queryFile + ", fullQuery=" + sql, e);
    }
  }
}
