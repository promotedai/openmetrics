package ai.promoted.metrics.logprocessor.common.table;

import static com.google.common.truth.Truth.assertThat;

import ai.promoted.metrics.logprocessor.common.job.BaseFlinkJob;
import ai.promoted.metrics.logprocessor.common.job.DirectFlatOutputKafkaSource;
import ai.promoted.metrics.logprocessor.common.job.DirectValidatedEventKafkaSource;
import ai.promoted.metrics.logprocessor.common.job.FlatOutputKafkaSegment;
import ai.promoted.metrics.logprocessor.common.job.FlinkSegment;
import ai.promoted.metrics.logprocessor.common.job.KafkaSegment;
import ai.promoted.metrics.logprocessor.common.job.KafkaSourceSegment;
import ai.promoted.metrics.logprocessor.common.job.ValidatedEventKafkaSegment;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableMap;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class MetricsTableCatalogTest {
  @TempDir protected File tempDir;

  @Test
  void testRegisterKafkaTables() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    BaseFlinkJob baseFlinkJob =
        new BaseFlinkJob() {
          @Override
          protected void startJob() throws Exception {}

          @Override
          protected String getDefaultBaseJobName() {
            return "catalog_kafka";
          }

          @Override
          public Set<FlinkSegment> getInnerFlinkSegments() {
            return null;
          }
        };
    KafkaSegment kafkaSegment = new KafkaSegment();
    kafkaSegment.bootstrapServers = "kafka:9092";
    MetricsTableCatalog tableCatalog =
        new MetricsTableCatalog("promoted_prod", "blue", "kafka-consumer-group");
    tableCatalog.addFlatOutputSourceProvider(
        DirectFlatOutputKafkaSource.DIRECT_FLAT_RO_DATABASE_PREFIX,
        new DirectFlatOutputKafkaSource(
            new KafkaSourceSegment(baseFlinkJob, kafkaSegment),
            new FlatOutputKafkaSegment(baseFlinkJob, kafkaSegment)));
    tableCatalog.addValidatedSourceProvider(
        DirectValidatedEventKafkaSource.DIRECT_VALIDATED_RO_DB_PREFIX,
        new DirectValidatedEventKafkaSource(
            new KafkaSourceSegment(baseFlinkJob, kafkaSegment),
            new ValidatedEventKafkaSegment(baseFlinkJob, kafkaSegment)));
    List<String> expectedTables =
        ImmutableList.of(
            "`promoted_prod_metrics_default_blue`.`direct_flat_ro`.`joined_impression`",
            "`promoted_prod_metrics_default_blue`.`direct_flat_ro`.`attributed_action`",
            "`promoted_prod_metrics_default_blue`.`direct_validated_ro`.`view`",
            "`promoted_prod_metrics_default_blue`.`direct_validated_ro`.`diagnostics`",
            "`promoted_prod_metrics_default_blue`.`direct_validated_ro`.`impression`",
            "`promoted_prod_metrics_default_blue`.`direct_validated_ro`.`cohort_membership`",
            "`promoted_prod_metrics_default_blue`.`direct_validated_ro`.`action`",
            "`promoted_prod_metrics_default_blue`.`direct_validated_ro`.`retained_user`",
            "`promoted_prod_metrics_default_blue`.`direct_validated_ro`.`log_user_user`",
            "`promoted_prod_metrics_default_blue`.`direct_validated_ro`.`delivery_log`");
    List<String> tables = tableCatalog.registerMetricsTables(env, tEnv);
    assertThat(tables).containsExactlyElementsIn(expectedTables);

    // Validate catalogs
    List<String> catalogs = new ArrayList<>();
    tEnv.executeSql("show catalogs")
        .collect()
        .forEachRemaining(row -> catalogs.add(Objects.requireNonNull(row.getField(0)).toString()));
    List<String> expectedCatalogs =
        ImmutableList.of("default_catalog", "promoted_prod_metrics_default_blue");
    assertThat(catalogs).containsExactlyElementsIn(expectedCatalogs);

    // Validate databases
    List<String> databases = new ArrayList<>();
    tEnv.executeSql("use catalog `promoted_prod_metrics_default_blue`");
    tEnv.executeSql("show databases")
        .collect()
        .forEachRemaining(row -> databases.add(Objects.requireNonNull(row.getField(0)).toString()));
    List<String> expectedDatabases =
        ImmutableList.of("default", "direct_flat_ro", "direct_validated_ro");
    assertThat(databases).containsExactlyElementsIn(expectedDatabases);

    // Validate raw tables
    tEnv.executeSql("use `direct_validated_ro`");
    List<String> rawTables = new ArrayList<>();
    tEnv.executeSql("show tables")
        .collect()
        .forEachRemaining(row -> rawTables.add(Objects.requireNonNull(row.getField(0)).toString()));
    List<String> expectedRawTables =
        ImmutableList.of(
            "action",
            "cohort_membership",
            "diagnostics",
            "impression",
            "view",
            "log_user_user",
            "retained_user",
            "delivery_log");
    assertThat(rawTables).containsExactlyElementsIn(expectedRawTables);

    // Validate flat tables
    tEnv.executeSql("use `direct_flat_ro`");
    List<String> flatTables = new ArrayList<>();
    tEnv.executeSql("show tables")
        .collect()
        .forEachRemaining(
            row -> flatTables.add(Objects.requireNonNull(row.getField(0)).toString()));
    List<String> expectedFlatTables = ImmutableList.of("attributed_action", "joined_impression");
    assertThat(flatTables).containsExactlyElementsIn(expectedFlatTables);
  }

  @Test
  void testRegisterFileSystemAvroTables() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    MetricsTableCatalog tableCatalog =
        new MetricsTableCatalog("promoted_prod", "blue", "kafka-consumer-group");
    tableCatalog.setFsAvroPathSchemas(
        ImmutableMap.of(
            "file:///path/to/log_user_user",
            "ai.promoted.metrics.common.LogUserUser", // Avro schema
            "file:///path/to/another/log_user_user",
            "ai.promoted.metrics.common.LogUserUser", // Duplicate schemas
            "file:///path/to/delivery_log",
            "ai.promoted.proto.event.TinyInsertion")); // Proto schema
    String path = tempDir.getPath();
    List<String> createdTable = tableCatalog.registerMetricsTables(env, tEnv);
    assertThat(createdTable)
        .containsExactlyElementsIn(
            ImmutableList.of(
                "`promoted_prod_metrics_default_blue`.`fs_avro`.`LogUserUser_0`",
                "`promoted_prod_metrics_default_blue`.`fs_avro`.`LogUserUser_1`",
                "`promoted_prod_metrics_default_blue`.`fs_avro`.`TinyInsertion_0`"));
  }
}
