package ai.promoted.metrics.logprocessor.job.sqlrunner;

import static com.google.common.truth.Truth.assertThat;

import ai.promoted.metrics.logprocessor.common.functions.base.SerializableConsumer;
import ai.promoted.metrics.logprocessor.common.job.DirectFlatOutputKafkaSource;
import ai.promoted.metrics.logprocessor.common.job.DirectValidatedEventKafkaSource;
import ai.promoted.metrics.logprocessor.common.job.FlatOutputKafkaSegment;
import ai.promoted.metrics.logprocessor.common.job.KafkaSegment;
import ai.promoted.metrics.logprocessor.common.job.KafkaSourceSegment;
import ai.promoted.metrics.logprocessor.common.job.ValidatedEventKafkaSegment;
import ai.promoted.metrics.logprocessor.common.job.paimon.PaimonSegment;
import ai.promoted.metrics.logprocessor.common.job.testing.BaseJobMiniclusterTest;
import ai.promoted.metrics.logprocessor.common.testing.MiniClusterExtension;
import ai.promoted.proto.delivery.DeliveryLog;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(MiniClusterExtension.class)
public class SqlRunnerJobMiniclusterTest extends BaseJobMiniclusterTest<SqlRunnerJob> {
  public static final Logger LOGGER = LogManager.getLogger(SqlRunnerJobMiniclusterTest.class);

  @Test
  void testFsAvro() {
    SqlRunnerJob job = createJob();
    String relativePath = "pipeline/src/test/java/resources/rawavro/deliverylog";
    String deliveryLogPath = new File(relativePath).getAbsolutePath();
    final List<String> result = new ArrayList<>();
    job.setResultConsumer((SerializableConsumer<Row>) row -> result.add(row.toString()));
    job.fsAvroPathSchemas =
        ImmutableMap.of(
            "file://" + deliveryLogPath,
            "ai.promoted.proto.delivery.DeliveryLog",
            "file:///tmp/view",
            "ai.promoted.proto.event.View",
            "file:///tmp/impression",
            "ai.promoted.proto.event.Impression");
    job.sqlStatements =
        ImmutableList.of(
            "SELECT request.user_info.log_user_id, response.insertion FROM `metrics_default_blue`.`fs_avro`.`DeliveryLog_0`");
    String expectedResult =
        "+I[00000000-0000-0000-0000-000000000001, [+I[1, null, +I[1601596151000, 1601596151000, 0, 0], null, 44444444-4444-4444-0000-000000000001, 33333333-3333-3333-0000-000000000001, , , , i-1-1, 0, null, null, null, null, null, 0, 0.0]]]";
    job.startSqlRunner(env, EnvironmentSettings.newInstance().build());
    assertThat(result).containsExactly(expectedResult);
  }

  @Test
  void testSqlRunnerWithPaimon() throws Exception {
    SqlRunnerJob job = createJob();
    preparePaimonCatalog(job);
    job.paimonCatalogPaths = List.of(tempDir.getPath());
    final List<String> result = new ArrayList<>();
    job.setResultConsumer((SerializableConsumer<Row>) row -> result.add(row.toString()));
    job.sqlStatements =
        ImmutableList.of(
            "show catalogs;",
            " -- this is a comment",
            "SELECT platform_id FROM `metrics_paimon_blue`.`test_db_blue`.`delivery_log`",
            "/* this is another comment */");
    Configuration configuration = new Configuration();
    configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
    EnvironmentSettings settings =
        EnvironmentSettings.newInstance().withConfiguration(configuration).build();
    job.startSqlRunner(env, settings);
    assertThat(result)
        .containsExactly(
            "+I[default_catalog]", "+I[metrics_default_blue]", "+I[metrics_paimon_blue]", "+I[1]");
  }

  @Override
  protected SqlRunnerJob createJob() {
    SqlRunnerJob job = new SqlRunnerJob();
    job.jobLabel = "blue";
    job.checkpointInterval = Duration.ofSeconds(1);
    KafkaSegment kafkaSegment = new KafkaSegment();
    KafkaSourceSegment kafkaSourceSegment = new KafkaSourceSegment(job, kafkaSegment);
    ValidatedEventKafkaSegment validatedEventKafkaSegment =
        new ValidatedEventKafkaSegment(job, kafkaSegment);
    FlatOutputKafkaSegment flatOutputKafkaSegment = new FlatOutputKafkaSegment(job, kafkaSegment);
    job.directValidatedEventKafkaSource =
        new DirectValidatedEventKafkaSource(kafkaSourceSegment, validatedEventKafkaSegment);
    job.directFlatOutputKafkaSource =
        new DirectFlatOutputKafkaSource(kafkaSourceSegment, flatOutputKafkaSegment);
    return job;
  }

  private void preparePaimonCatalog(SqlRunnerJob sqlRunnerJob)
      throws ExecutionException, InterruptedException {
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    PaimonSegment paimonSegment = new PaimonSegment(sqlRunnerJob, tEnv);
    paimonSegment.paimonCatalogName = "metrics_paimon";
    paimonSegment.paimonBasePath = tempDir.getPath();
    DataStreamSource<DeliveryLog> input =
        env.fromCollection(List.of(DeliveryLog.newBuilder().setPlatformId(1).build()));
    paimonSegment.writeProtoToPaimon(
        input.assignTimestampsAndWatermarks(
            WatermarkStrategy.<DeliveryLog>forMonotonousTimestamps()
                .withTimestampAssigner(
                    (TimestampAssignerSupplier<DeliveryLog>)
                        context ->
                            (TimestampAssigner<DeliveryLog>)
                                (deliveryLog, l) -> System.currentTimeMillis())),
        "test_db",
        "delivery_log",
        Collections.emptyList(),
        List.of("platform_id"),
        Collections.emptyList(),
        Collections.emptyMap());
    paimonSegment.getStatementSet().execute().await();
  }
}
