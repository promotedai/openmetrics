package ai.promoted.metrics.logprocessor.job.contentmetrics;

import static org.apache.flink.table.api.Expressions.$;

import ai.promoted.metrics.logprocessor.common.job.BaseFlinkJob;
import ai.promoted.metrics.logprocessor.common.job.DirectFlatOutputKafkaSource;
import ai.promoted.metrics.logprocessor.common.job.DirectValidatedEventKafkaSource;
import ai.promoted.metrics.logprocessor.common.job.FlatOutputKafkaSegment;
import ai.promoted.metrics.logprocessor.common.job.FlatOutputKafkaSource;
import ai.promoted.metrics.logprocessor.common.job.FlatOutputKafkaSourceProvider;
import ai.promoted.metrics.logprocessor.common.job.FlinkSegment;
import ai.promoted.metrics.logprocessor.common.job.KafkaSegment;
import ai.promoted.metrics.logprocessor.common.job.KafkaSourceSegment;
import ai.promoted.metrics.logprocessor.common.job.KeepFirstSegment;
import ai.promoted.metrics.logprocessor.common.job.ResourceLoader;
import ai.promoted.metrics.logprocessor.common.job.S3FileOutput;
import ai.promoted.metrics.logprocessor.common.job.S3Segment;
import ai.promoted.metrics.logprocessor.common.job.ValidatedDataSourceProvider;
import ai.promoted.metrics.logprocessor.common.job.ValidatedEventKafkaSegment;
import ai.promoted.metrics.logprocessor.common.job.ValidatedEventKafkaSource;
import ai.promoted.metrics.logprocessor.common.table.MetricsTableCatalog;
import ai.promoted.metrics.logprocessor.common.table.Tables;
import ai.promoted.metrics.logprocessor.common.util.StringUtil;
import ai.promoted.proto.event.ActionType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

@CommandLine.Command(
    name = "contentmetrics",
    mixinStandardHelpOptions = true,
    version = "contentmetrics 1.0.0",
    description =
        "Creates a Flink job that produces content metrics tables by aggregating engagement records from Kafka and writes records to sinks.")
public class ContentMetricsJob extends BaseFlinkJob {
  private static final Logger LOGGER = LogManager.getLogger(ContentMetricsJob.class);

  @CommandLine.Mixin public final KafkaSegment kafkaSegment = new KafkaSegment();

  @CommandLine.Mixin
  public final KafkaSourceSegment kafkaSourceSegment = new KafkaSourceSegment(this, kafkaSegment);

  public final ValidatedEventKafkaSegment validatedEventKafkaSegment =
      new ValidatedEventKafkaSegment(this, kafkaSegment);

  @CommandLine.Mixin
  public final DirectValidatedEventKafkaSource directValidatedEventKafkaSource =
      new DirectValidatedEventKafkaSource(kafkaSourceSegment, validatedEventKafkaSegment);

  @CommandLine.Mixin public final KeepFirstSegment keepFirstSegment = new KeepFirstSegment(this);

  public final ValidatedEventKafkaSource validatedEventKafkaSource =
      new ValidatedEventKafkaSource(
          directValidatedEventKafkaSource, validatedEventKafkaSegment, keepFirstSegment);

  @CommandLine.Mixin
  public final FlatOutputKafkaSegment flatOutputKafkaSegment =
      new FlatOutputKafkaSegment(this, kafkaSegment);

  @CommandLine.Mixin
  public final DirectFlatOutputKafkaSource directFlatOutputKafkaSource =
      new DirectFlatOutputKafkaSource(kafkaSourceSegment, flatOutputKafkaSegment);

  private final FlatOutputKafkaSource flatOutputKafkaSource =
      new FlatOutputKafkaSource(
          directFlatOutputKafkaSource, flatOutputKafkaSegment, keepFirstSegment);

  @CommandLine.Mixin public final S3Segment s3 = new S3Segment(this);
  @CommandLine.Mixin public final S3FileOutput s3FileOutput = new S3FileOutput(this, s3);

  @CommandLine.Option(
      names = {"--region"},
      defaultValue = "",
      description = "AWS region.  Defaults empty.")
  public String region = "";

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

  @CommandLine.Option(
      names = {"--outputCumulatedFiles"},
      negatable = true,
      description =
          "Whether to output cumulated JSON files.  This is meant for debugging locally.  Default=false")
  public boolean outputCumulatedFiles = false;

  @CommandLine.Option(
      names = {"--outputCumulatedKafka"},
      negatable = true,
      description =
          "Whether to output cumulated Kafka.  This is meant for debugging locally.  Default=false")
  public boolean outputCumulatedKafka = false;

  @CommandLine.Option(
      names = {"--outputCumulatedKinesis"},
      negatable = true,
      description =
          "Whether to output cumulated Kinesis.  This is meant for debugging locally.  Default=false")
  public boolean outputCumulatedKinesis = false;

  @CommandLine.Option(
      names = {"--cumulatedWindowStep"},
      defaultValue = "INTERVAL '1' DAY",
      description =
          "Changes the cumulate window step size.  Must be an divide into 1 day with a remainder of zero.  Setting to a smaller value causes the rows to emitted more frequently.  Defaults to \"INTERVAL '1' DAY\" so the output is at the end of the day")
  public String cumulatedWindowStep = "INTERVAL '1' DAY";

  @CommandLine.Option(
      names = {"--kinesisStream"},
      defaultValue = "",
      description = "The kinesis stream.  Defaults empty.")
  public String kinesisStream = "";

  public static void main(String[] args) {
    executeMain(new ContentMetricsJob(), args);
  }

  @Override
  public Set<FlinkSegment> getInnerFlinkSegments() {
    return ImmutableSet.of(
        kafkaSegment,
        kafkaSourceSegment,
        validatedEventKafkaSegment,
        directValidatedEventKafkaSource,
        keepFirstSegment,
        validatedEventKafkaSource,
        flatOutputKafkaSegment,
        directFlatOutputKafkaSource,
        flatOutputKafkaSource,
        s3,
        s3FileOutput);
  }

  @Override
  public void validateArgs() {
    super.validateArgs();
    if (outputCumulatedKinesis) {
      Preconditions.checkArgument(
          !kinesisStream.isEmpty(), "--kinesisStream needs to be specified");
    }
  }

  @Override
  protected String getDefaultBaseJobName() {
    return "content-metrics";
  }

  @Override
  protected void startJob() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    configureExecutionEnvironment(env, parallelism, maxParallelism);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    tableEnv.getConfig().setLocalTimeZone(ZoneOffset.UTC);

    Tables.createCatalogAndDatabase(tableEnv);

    createOperators(env, tableEnv, validatedEventKafkaSource, flatOutputKafkaSource);
    LOGGER.info("ContentMetricsJob.executionPlan\n{}", env.getExecutionPlan());
  }

  private void prepareFunctions(StreamTableEnvironment tableEnv) {
    tableEnv.registerFunction("toActionCartContentRow", new ToActionCartContentRow());
    tableEnv.createTemporarySystemFunction("toActionTypeNum", ToActionTypeNum.class);
    tableEnv.createTemporarySystemFunction("toLocalTimestamp", ToLocalTimestamp.class);
  }

  private void prepareViews(StreamTableEnvironment tableEnv) {
    tableEnv.executeSql(
        "CREATE TEMPORARY VIEW view AS "
            + "(SELECT "
            + "rowtime, "
            + "platform_id, "
            + "toLocalTimestamp(timing.event_api_timestamp) AS event_api_timestamp, "
            + "view_id, "
            + "name, "
            + "content_id "
            + "FROM `metrics_default_"
            + getJobLabel()
            + "`.`validated_ro`.`view`)");

    tableEnv.executeSql(
        "CREATE TEMPORARY VIEW impression AS "
            + "(SELECT "
            + "rowtime, "
            + "platform_id, "
            + "toLocalTimestamp(timing.event_api_timestamp) AS event_api_timestamp, "
            + "impression_id, "
            + "insertion_id, "
            + "content_id "
            + "FROM `metrics_default_"
            + getJobLabel()
            + "`.`validated_ro`.`impression`)");

    tableEnv.executeSql(
        "CREATE TEMPORARY VIEW joined_impression AS "
            + "(SELECT "
            + "rowtime, "
            + "ids.platform_id AS platform_id, "
            + "toLocalTimestamp(timing.event_api_timestamp) AS event_api_timestamp, "
            + "ids.impression_id AS impression_id, "
            + "response_insertion.content_id as content_id, "
            + "response_insertion.`position` AS `position`, "
            + "request.search_query AS search_query "
            + "FROM `metrics_default_"
            + getJobLabel()
            + "`.`flat_ro`.`joined_impression`)");

    tableEnv.executeSql(
        "CREATE TEMPORARY VIEW action AS "
            + "(SELECT "
            + "rowtime, "
            + "platform_id, "
            + "toLocalTimestamp(timing.event_api_timestamp) AS event_api_timestamp, "
            + "action_id, "
            + "impression_id, "
            + "insertion_id, "
            + "content_id, "
            + "toActionTypeNum(action_type) AS action_type, "
            + "cart "
            + "FROM `metrics_default_"
            + getJobLabel()
            + "`.`validated_ro`.`action`)");

    Table cartContentTable =
        tableEnv
            .sqlQuery("SELECT * FROM action")
            .flatMap(
                Expressions.call(
                    "toActionCartContentRow",
                    $("platform_id"),
                    $("action_id"),
                    $("event_api_timestamp"),
                    $("impression_id"),
                    $("insertion_id"),
                    $("action_type"),
                    $("cart"),
                    $("rowtime")));
    tableEnv.createTemporaryView("action_cart_content", cartContentTable);
  }

  @VisibleForTesting
  List<TableResult> createOperators(
      StreamExecutionEnvironment env,
      StreamTableEnvironment tableEnv,
      ValidatedDataSourceProvider validatedDataSourceProvider,
      FlatOutputKafkaSourceProvider flatOutputKafkaSourceProvider) {

    MetricsTableCatalog metricsTableCatalog =
        new MetricsTableCatalog("", getJobLabel(), toKafkaConsumerGroupId("contentmetrics"));
    metricsTableCatalog.addValidatedSourceProvider(
        ValidatedEventKafkaSource.VALIDATED_RO_DB_PREFIX, validatedDataSourceProvider);
    metricsTableCatalog.addFlatOutputSourceProvider(
        FlatOutputKafkaSource.FLAT_RO_DB_PREFIX, flatOutputKafkaSourceProvider);
    List<String> tables = metricsTableCatalog.registerMetricsTables(env, tableEnv);
    LOGGER.debug("Registered tables {}", tables);
    tableEnv.getConfig().getConfiguration().setString(PipelineOptions.NAME, getJobName());

    prepareFunctions(tableEnv);
    prepareViews(tableEnv);

    StreamStatementSet statementSet = tableEnv.createStatementSet();
    executeSqlFromResource(tableEnv::executeSql, "1_create_unified_event_stream.sql");
    boolean hasOutput = false;
    if (outputParquet) {
      executeSqlFromResource(tableEnv::executeSql, "2_create_view_hourly_content_metrics.sql");
      executeSqlFromResource(tableEnv::executeSql, "3_create_table_hourly_content_metrics.sql");
      statementSet =
          executeSqlFromResource(
              statementSet::addInsertSql, "4_insert_into_hourly_content_metrics.sql");
      executeSqlFromResource(tableEnv::executeSql, "5_create_view_daily_content_metrics.sql");
      executeSqlFromResource(tableEnv::executeSql, "6_create_table_daily_content_metrics.sql");
      statementSet =
          executeSqlFromResource(
              statementSet::addInsertSql, "7_insert_into_daily_content_metrics.sql");
      hasOutput = true;
    }
    if (outputCsv) {
      executeSqlFromResource(tableEnv::executeSql, "8_create_table_daily_content_metrics_csv.sql");
      statementSet =
          executeSqlFromResource(
              statementSet::addInsertSql, "9_insert_into_daily_content_metrics_csv.sql");
      hasOutput = true;
    }
    if (outputCumulatedFiles || outputCumulatedKafka || outputCumulatedKinesis) {
      executeSqlFromResource(tableEnv::executeSql, "10_create_view_cumulated_content_metrics.sql");
      if (outputCumulatedFiles) {
        executeSqlFromResource(
            tableEnv::executeSql, "11_create_table_cumulated_content_metrics_file.sql");
        statementSet =
            executeSqlFromResource(
                statementSet::addInsertSql, "12_insert_into_cumulated_content_metrics_file.sql");
      }
      if (outputCumulatedKafka) {
        executeSqlFromResource(
            tableEnv::executeSql, "13_create_table_cumulated_content_metrics_kafka.sql");
        statementSet =
            executeSqlFromResource(
                statementSet::addInsertSql, "14_insert_into_cumulated_content_metrics_kafka.sql");
      }
      if (outputCumulatedKinesis) {
        executeSqlFromResource(
            tableEnv::executeSql, "15_create_table_cumulated_content_metrics_kinesis.sql");
        statementSet =
            executeSqlFromResource(
                statementSet::addInsertSql, "16_insert_into_cumulated_content_metrics_kinesis.sql");
      }
      hasOutput = true;
    }
    Preconditions.checkState(hasOutput, "At least one output flag needs to be specified");

    // TODO - switch query away from rowtime and to event api time.
    // TODO - include platform_id in the join.

    return ImmutableList.of(statementSet.execute());
  }

  /** Returns the SQL string with templated parameters filled in. */
  private String getResolvedSqlString(String queryFile) {
    String query = ResourceLoader.getQueryFromResource(ContentMetricsJob.class, queryFile);

    // TODO - make a separate way to support required and optional flags.
    if (query.contains("{region}")) {
      Preconditions.checkArgument(!region.isEmpty(), "region needs to be specified");
    }

    Map<String, String> formatParameters =
        ImmutableMap.<String, String>builder()
            .put("jobName", getJobName())
            .put("rootPath", s3.getOutputDir().build().toString())
            .put("sinkParallelism", Integer.toString(getSinkParallelism("sink")))
            .put("cumulatedWindowStep", cumulatedWindowStep)
            // TODO - parameterize these Kafka outputs parameters.
            // It's fine right now because the output Kafka topic is only used for local testing.
            .put("cumulatedKafkaTopic", "metrics.blue.default.cumulated-content-metrics")
            .put("kafkaBootstrapServers", kafkaSegment.bootstrapServers)
            .put("cumulatedKafkaGroupId", "blue.content-metrics")
            .put("region", region)
            .put("kinesisStream", kinesisStream)
            .build();
    try {
      return StringUtil.replace(query, formatParameters);
    } catch (Exception e) {
      throw new IllegalArgumentException("Error formatting SQL " + queryFile, e);
    }
  }

  private <R> R executeSqlFromResource(
      java.util.function.Function<String, R> executeSql, String queryFile) {
    String sql = getResolvedSqlString(queryFile);
    try {
      return executeSql.apply(sql);
    } catch (Exception e) {
      throw new RuntimeException("executeSql error for " + queryFile + ", fullQuery=" + sql, e);
    }
  }

  public static class ToLocalTimestamp extends ScalarFunction {
    public LocalDateTime eval(Long eventApiTimestamp) {
      return LocalDateTime.ofEpochSecond(
          eventApiTimestamp / 1000, (int) ((eventApiTimestamp % 1000) * 1000000), ZoneOffset.UTC);
    }
  }

  public static class ToActionTypeNum extends ScalarFunction {
    public int eval(String actionType) {
      return ActionType.valueOf(actionType).getNumber();
    }
  }

  public static class ToActionCartContentRow extends TableFunction<Row> {
    public void eval(
        Long platformId,
        String actionId,
        LocalDateTime eventApiTimestamp,
        String impressionId,
        String insertionId,
        int actionType,
        @Nullable Row cart,
        Timestamp rowtime) {
      if (null != cart) {
        Row[] cartContents = cart.getFieldAs("contents");
        for (Row cartContent : cartContents) {
          collect(
              Row.of(
                  platformId,
                  eventApiTimestamp,
                  actionId,
                  impressionId,
                  insertionId,
                  cartContent.<String>getFieldAs("content_id"),
                  actionType,
                  cartContent.<Long>getFieldAs("quantity"),
                  cartContent.<Row>getFieldAs("price_per_unit").<Long>getFieldAs("amount_micros"),
                  rowtime));
        }
      }
    }

    @Override
    public TypeInformation<Row> getResultType() {
      return Types.ROW_NAMED(
          new String[] {
            "platform_id",
            "event_api_timestamp",
            "action_id",
            "impression_id",
            "insertion_id",
            "content_id",
            "action_type",
            "quantity",
            "price_usd_micros_per_unit",
            "rowtime"
          },
          Types.LONG,
          Types.LOCAL_DATE_TIME,
          Types.STRING,
          Types.STRING,
          Types.STRING,
          Types.STRING,
          Types.INT,
          Types.LONG,
          Types.LONG,
          Types.SQL_TIMESTAMP);
    }
  }
}
