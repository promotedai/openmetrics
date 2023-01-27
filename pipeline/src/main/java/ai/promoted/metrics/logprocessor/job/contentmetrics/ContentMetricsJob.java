package ai.promoted.metrics.logprocessor.job.contentmetrics;

import ai.promoted.metrics.logprocessor.common.job.BaseFlinkJob;
import ai.promoted.metrics.logprocessor.common.job.FlatOutputKafka;
import ai.promoted.metrics.logprocessor.common.job.JoinedImpressionSegment;
import ai.promoted.metrics.logprocessor.common.job.KafkaSegment;
import ai.promoted.metrics.logprocessor.common.job.MetricsApiKafkaSource;
import ai.promoted.metrics.logprocessor.common.job.RawActionSegment;
import ai.promoted.metrics.logprocessor.common.job.RawImpressionSegment;
import ai.promoted.metrics.logprocessor.common.job.RawViewSegment;
import ai.promoted.metrics.logprocessor.common.job.S3FileOutput;
import ai.promoted.metrics.logprocessor.common.util.StringUtil;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.JoinedEvent;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.View;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@CommandLine.Command(name = "contentmetrics", mixinStandardHelpOptions = true, version = "contentmetrics 1.0.0",
        description = "Creates a Flink job that produces content metrics tables by aggregating engagement records from Kafka and writes records to sinks.")
public class ContentMetricsJob extends BaseFlinkJob {
    private static final Logger LOGGER = LogManager.getLogger(ContentMetricsJob.class);

    @CommandLine.Mixin
    public final KafkaSegment kafkaSegment = new KafkaSegment(this);
    @CommandLine.Mixin
    public final MetricsApiKafkaSource metricsApiKafkaSource = new MetricsApiKafkaSource(this, kafkaSegment);
    @CommandLine.Mixin public final FlatOutputKafka flatOutputKafka = new FlatOutputKafka(kafkaSegment);
    @CommandLine.Mixin
    public final S3FileOutput s3FileOutput = new S3FileOutput(this);
    @CommandLine.Mixin
    public final RawViewSegment rawViewSegment = new RawViewSegment(this);
    @CommandLine.Mixin
    public final RawImpressionSegment rawImpressionSegment = new RawImpressionSegment(this);
    @CommandLine.Mixin
    public final RawActionSegment rawActionSegment = new RawActionSegment(this);
    @CommandLine.Mixin
    public final JoinedImpressionSegment joinedImpressionSegment = new JoinedImpressionSegment(this);

    @CommandLine.Option(names = {"--jobName"}, defaultValue = "content-metrics", description = "The name of the job (excluding the label prefix).  This can be used to run multiple ContentMetrics jobs with the same label and different outputs.  Defaults to \"content-metrics\"")
    public String jobName = "content-metrics";

    @CommandLine.Option(names = {"--region"}, defaultValue = "", description = "AWS region.  Defaults empty.")
    public String region = "";

    @CommandLine.Option(names = {"--overrideInputLabel"}, defaultValue = "", description = "Overrides the Kafka input label (defaults to --jobLabel).  Can be used for cases like --jobLabel='blue.canary' and --overrideInputLabel='blue'.  Empty string means no override.  Cannot be used to override to empty string (not useful now).  Defaults to empty string (no override)")
    public String overrideInputLabel = "";

    @CommandLine.Option(names = {"--outputParquet"}, negatable = true, description = "Whether to output parquet files.  Default=false")
    public boolean outputParquet = false;

    @CommandLine.Option(names = {"--outputCsv"}, negatable = true, description = "Whether to output csv files.  Default=false")
    public boolean outputCsv = false;

    @CommandLine.Option(names = {"--outputCumulatedFiles"}, negatable = true, description = "Whether to output cumulated JSON files.  This is meant for debugging locally.  Default=false")
    public boolean outputCumulatedFiles = false;

    @CommandLine.Option(names = {"--outputCumulatedKafka"}, negatable = true, description = "Whether to output cumulated Kafka.  This is meant for debugging locally.  Default=false")
    public boolean outputCumulatedKafka = false;

    @CommandLine.Option(names = {"--outputCumulatedKinesis"}, negatable = true, description = "Whether to output cumulated Kinesis.  This is meant for debugging locally.  Default=false")
    public boolean outputCumulatedKinesis = false;

    @CommandLine.Option(names = {"--cumulatedWindowStep"}, defaultValue = "INTERVAL '1' DAY", description = "Changes the cumulate window step size.  Must be an divide into 1 day with a remainder of zero.  Setting to a smaller value causes the rows to emitted more frequently.  Defaults to \"INTERVAL '1' DAY\" so the output is at the end of the day")
    public String cumulatedWindowStep = "INTERVAL '1' DAY";

    @CommandLine.Option(names = {"--kinesisStream"}, defaultValue = "", description = "The kinesis stream.  Defaults empty.")
    public String kinesisStream = "";

    public static void main(String[] args) {
        int exitCode = new CommandLine(new ContentMetricsJob()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        validateArgs();
        startJob();
        return 0;
    }

    @Override
    public void validateArgs() {
        kafkaSegment.validateArgs();
        metricsApiKafkaSource.validateArgs();
        s3FileOutput.validateArgs();
        rawViewSegment.validateArgs();
        rawImpressionSegment.validateArgs();
        rawActionSegment.validateArgs();
        joinedImpressionSegment.validateArgs();

        if (outputCumulatedKinesis) {
            Preconditions.checkArgument(!kinesisStream.isEmpty(), "--kinesisStream needs to be specified");
        }
    }

    @Override
    public List<Class<? extends GeneratedMessageV3>> getProtoClasses() {
        return ImmutableList.<Class<? extends GeneratedMessageV3>>builder()
                .addAll(kafkaSegment.getProtoClasses())
                .addAll(metricsApiKafkaSource.getProtoClasses())
                .addAll(s3FileOutput.getProtoClasses())
                .addAll(rawViewSegment.getProtoClasses())
                .addAll(rawImpressionSegment.getProtoClasses())
                .addAll(rawActionSegment.getProtoClasses())
                .addAll(joinedImpressionSegment.getProtoClasses())
                .build();
    }

    public String getInputLabel() {
        return StringUtil.firstNotEmpty(overrideInputLabel, getJobLabel());
    }

    @Override
    public String getJobName() {
        return prefixJobLabel(jobName);
    }

    private void startJob() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureExecutionEnvironment(env, parallelism, maxParallelism);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setLocalTimeZone(ZoneOffset.UTC);

        // TODO - make sure we don't have overlapping kafkagroupids.
        String kafkaGroupId = toKafkaConsumerGroupId("contentmetrics-logrequest");
        MetricsApiKafkaSource.SplitSources splitLogRequest = metricsApiKafkaSource.splitSources(env, kafkaGroupId);

        Tables.createCatalogAndDatabase(tableEnv);
        DataStream<JoinedEvent> joinedEvents = flatOutputKafka.getJoinedEventSource(
                env,
                toKafkaConsumerGroupId("content-metrics"),
                flatOutputKafka.getJoinedEventTopic(getInputLabel()));
        createOperators(tableEnv, splitLogRequest, joinedEvents);
        LOGGER.info("ContentMetricsJob.executionPlan\n{}", env.getExecutionPlan());
    }

    @VisibleForTesting
    List<TableResult> createOperators(
            StreamTableEnvironment tableEnv,
            MetricsApiKafkaSource.SplitSources splitLogRequest,
            DataStream<JoinedEvent> joinedEvents
    ) {
        tableEnv.getConfig().getConfiguration().setString("pipeline.name", getJobName());

        createViewTable(tableEnv, splitLogRequest);
        createImpressionTable(tableEnv, splitLogRequest);
        createJoinedImpressionTable(tableEnv, joinedEvents);
        // TODO - try to move deduplication work to Flink SQL.  TBD because Flink SQL doesn't support the same keep first semantics.
        DataStream<Action> actions = rawActionSegment.getDeduplicatedAction(splitLogRequest.getRawActionSource());
        createActionTable(tableEnv, actions);
        createActionCartContentTable(tableEnv, actions);

        StreamStatementSet statementSet = tableEnv.createStatementSet();
        executeSqlFromResource(tableEnv::executeSql, "1_create_unified_event_stream.sql");
        boolean hasOutput = false;
        if (outputParquet) {
            executeSqlFromResource(tableEnv::executeSql, "2_create_view_hourly_content_metrics.sql");
            executeSqlFromResource(tableEnv::executeSql, "3_create_table_hourly_content_metrics.sql");
            statementSet = executeSqlFromResource(statementSet::addInsertSql, "4_insert_into_hourly_content_metrics.sql");
            executeSqlFromResource(tableEnv::executeSql, "5_create_view_daily_content_metrics.sql");
            executeSqlFromResource(tableEnv::executeSql, "6_create_table_daily_content_metrics.sql");
            statementSet = executeSqlFromResource(statementSet::addInsertSql, "7_insert_into_daily_content_metrics.sql");
            hasOutput = true;
        }
        if (outputCsv) {
            executeSqlFromResource(tableEnv::executeSql, "8_create_table_daily_content_metrics_csv.sql");
            statementSet = executeSqlFromResource(statementSet::addInsertSql, "9_insert_into_daily_content_metrics_csv.sql");
            hasOutput = true;
        }
        if (outputCumulatedFiles || outputCumulatedKafka || outputCumulatedKinesis) {
            executeSqlFromResource(tableEnv::executeSql, "10_create_view_cumulated_content_metrics.sql");
            if (outputCumulatedFiles) {
                executeSqlFromResource(tableEnv::executeSql, "11_create_table_cumulated_content_metrics_file.sql");
                statementSet = executeSqlFromResource(statementSet::addInsertSql, "12_insert_into_cumulated_content_metrics_file.sql");
            }
            if (outputCumulatedKafka) {
                executeSqlFromResource(tableEnv::executeSql, "13_create_table_cumulated_content_metrics_kafka.sql");
                statementSet = executeSqlFromResource(statementSet::addInsertSql, "14_insert_into_cumulated_content_metrics_kafka.sql");
            }
            if (outputCumulatedKinesis) {
                executeSqlFromResource(tableEnv::executeSql, "15_create_table_cumulated_content_metrics_kinesis.sql");
                statementSet = executeSqlFromResource(statementSet::addInsertSql, "16_insert_into_cumulated_content_metrics_kinesis.sql");
            }
            hasOutput = true;
        }
        Preconditions.checkState(hasOutput, "At least one output flag needs to be specified");

        // TODO - switch query away from rowtime and to event api time.
        // TODO - include platform_id in the join.
        // TODO - include deliverylog metrics.

        return ImmutableList.of(statementSet.execute());
    }

    private void createViewTable(StreamTableEnvironment tableEnv, MetricsApiKafkaSource.SplitSources splitLogRequest) {
        // TODO - try to move deduplication work to Flink SQL.  TBD because Flink SQL doesn't support the same keep first semantics.
        DataStream<View> protoStream = rawViewSegment.getDeduplicatedView(splitLogRequest.getRawViewSource());
        DataStream<Row> rows = add(protoStream.map(ViewTable::toRow, ViewTable.ROW_TYPE_INFORMATION), "to-view-row");
        Table table = tableEnv.fromDataStream(rows, ViewTable.SCHEMA);
        tableEnv.createTemporaryView("view", table);
    }

    private void createImpressionTable(StreamTableEnvironment tableEnv, MetricsApiKafkaSource.SplitSources splitLogRequest) {
        // TODO - try to move deduplication work to Flink SQL.  TBD because Flink SQL doesn't support the same keep first semantics.
        DataStream<Impression> impressions = rawImpressionSegment.getDeduplicatedImpression(splitLogRequest.getRawImpressionSource());
        DataStream<Row> impressionRows = add(impressions.map(ImpressionTable::toImpressionRow, ImpressionTable.IMPRESSION_ROW), "to-impression-row");
        Table table = tableEnv.fromDataStream(impressionRows, ImpressionTable.IMPRESSION_SCHEMA);
        tableEnv.createTemporaryView("impression", table);
    }

    private void createJoinedImpressionTable(StreamTableEnvironment tableEnv, DataStream<JoinedEvent> joinedEvents) {
        // TODO - try to move deduplication work to Flink SQL.  TBD because Flink SQL doesn't support the same keep first semantics.
        DataStream<JoinedEvent> joinedImpressions = add(joinedEvents.filter(new IsJoinedImpression()), "filter-joined-impression");
        joinedImpressions = joinedImpressionSegment.getDeduplicatedJoinedImpression(joinedImpressions);
        DataStream<Row> rows = add(joinedImpressions.map(new ToJoinedImpressionRows(), JoinedImpressionTable.ROW_TYPE_INFORMATION), "to-joined-impression-row");
        Table table = tableEnv.fromDataStream(rows, JoinedImpressionTable.SCHEMA);
        tableEnv.createTemporaryView("joined_impression", table);
    }

    private void createActionTable(StreamTableEnvironment tableEnv, DataStream<Action> actions) {
        DataStream<Row> actionRows = add(actions.map(ActionTable::toActionRow, ActionTable.ACTION_ROW), "to-action-row");
        Table table = tableEnv.fromDataStream(actionRows, ActionTable.ACTION_SCHEMA);
        tableEnv.createTemporaryView("action", table);
    }

    private void createActionCartContentTable(StreamTableEnvironment tableEnv, DataStream<Action> actions) {
        DataStream<Row> actionCartContentRows = add(
                actions.flatMap(new ToFlatActionCartContentRows(), ActionTable.ACTION_CART_CONTENT_ROW),
                "to-action-cart-content-row");
        Table table = tableEnv.fromDataStream(actionCartContentRows, ActionTable.ACTION_CART_CONTENT_SCHEMA);
        tableEnv.createTemporaryView("action_cart_content", table);
    }

    private static class ToFlatActionCartContentRows implements FlatMapFunction<Action, Row>, Serializable {
        @Override
        public void flatMap(Action action, Collector<Row> out) throws Exception {
            ActionTable.toActionCartContentRow(action).forEach(out::collect);
        }
    }

    public static class IsJoinedImpression implements FilterFunction<JoinedEvent>, Serializable {
        @Override
        public boolean filter(JoinedEvent joinedEvent) throws Exception {
            return !joinedEvent.hasAction();
        }
    }

    private static class ToJoinedImpressionRows implements MapFunction<JoinedEvent, Row>, Serializable {
        @Override
        public Row map(JoinedEvent event) throws Exception {
            return JoinedImpressionTable.toRow(event);
        }
    }

    private <R> R executeSqlFromResource(java.util.function.Function<String, R> executeSql, String queryFile) {
        String query = getQueryFromResource(queryFile);

        // TODO - make a separate way to support required and optional flags.
        if (query.contains("{region}")) {
            Preconditions.checkArgument(!region.isEmpty(), "region needs to be specified");
        }

        Map<String, String> formatParameters = ImmutableMap.<String, String>builder()
                .put("jobName", getJobName())
                .put("rootPath", s3FileOutput.getOutputS3Dir().build().toString())
                .put("sinkParallelism", Integer.toString(defaultSinkParallelism))
                .put("cumulatedWindowStep", cumulatedWindowStep)
                // TODO - parameterize.
                .put("cumulatedKafkaTopic", "metrics.blue.default.cumulated-content-metrics")
                .put("kafkaBootstrapServers", kafkaSegment.bootstrapServers)
                .put("cumulatedKafkaGroupId", "blue.content-metrics")
                .put("region", region)
                .put("kinesisStream", kinesisStream)
                .build();
        String fullQuery = "";
        try {
            fullQuery = StringUtil.replace(query, formatParameters);
            return executeSql.apply(fullQuery);
        } catch (Exception e) {
            throw new RuntimeException("executeSql error for " + queryFile + ", fullQuery=" + fullQuery, e);
        }
    }

    private String getQueryFromResource(String queryFile) {
        return getTextResource("resources/" + queryFile)
                .collect(Collectors.joining("\n"));
    }

    /** Gets resource from the jar. */
    private static Stream<String> getTextResource(String fileName) {
        InputStream inputStream = ContentMetricsJob.class.getResourceAsStream(fileName);
        if (inputStream == null) {
            throw new IllegalArgumentException("Resource filename does not exist, file=" + fileName);
        }
        return new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8))
                .lines();
    }
}
