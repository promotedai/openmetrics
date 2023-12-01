package ai.promoted.metrics.logprocessor.job.sqlrunner;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class PromotedUdfsUnitTest {
  private static StreamTableEnvironment tableEnvironment;

  @BeforeAll
  public static void setupTableEnv() {
    Configuration configuration = new Configuration();
    configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    TypeInformation<List<Row>> attributedActionType =
        Types.LIST(
            Types.ROW_NAMED(
                new String[] {"attribution", "action"},
                Types.ROW_NAMED(new String[] {"model_id", "credit_millis"}, Types.LONG, Types.INT),
                Types.ROW_NAMED(new String[] {"action_type"}, Types.STRING)));
    TypeInformation<Row> featureType =
        Types.ROW_NAMED(
            new String[] {"features"},
            Types.LIST(Types.ROW_NAMED(new String[] {"key", "value"}, Types.LONG, Types.FLOAT)));
    DataStream<Row> input =
        env.fromCollection(
                List.of(
                    Row.of(
                        List.of(
                            Row.of(Row.of(1L, 10), Row.of("NAVIGATE")),
                            Row.of(Row.of(2L, 0), Row.of("CHECKOUT")),
                            Row.of(Row.of(2L, null), Row.of("CHECKOUT")),
                            Row.of(Row.of(2L, 20), Row.of("CHECKOUT")),
                            Row.of(Row.of(2L, 30), Row.of("CHECKOUT"))),
                        Row.of((Object) null),
                        Row.of(List.of(Row.of(1000L, 2.0F), Row.of(1001L, 2.0F))),
                        Row.of(List.of(Row.of(1002L, 3.0F), Row.of(1003L, 3.0F))))))
            .returns(
                Types.ROW_NAMED(
                    new String[] {
                      "attributed_action",
                      "insertion_feature_stage",
                      "request_feature_stage",
                      "user_feature_stage"
                    },
                    attributedActionType,
                    featureType,
                    featureType,
                    featureType));
    EnvironmentSettings settings =
        EnvironmentSettings.newInstance().inBatchMode().withConfiguration(configuration).build();
    tableEnvironment = StreamTableEnvironment.create(env, settings);
    tableEnvironment.createTemporaryView("flat_response_insertions", input);
    tableEnvironment.executeSql("LOAD MODULE promoted");
  }

  @Test
  public void testArrayConcat() {
    List<Row> result = new ArrayList<>();
    tableEnvironment
        .executeSql(
            "SELECT\n"
                + "ARRAY_CONCAT(OR_EMPTY_ARRAY(insertion_feature_stage.features), OR_EMPTY_ARRAY(request_feature_stage.features), OR_EMPTY_ARRAY(user_feature_stage.features))\n"
                + "FROM flat_response_insertions")
        .collect()
        .forEachRemaining(result::add);
    Assertions.assertEquals(
        "[+I[[+I[1000, 2.0], +I[1001, 2.0], +I[1002, 3.0], +I[1003, 3.0]]]]", result.toString());
  }

  @Test
  public void testFilterRowArray() {
    List<Row> result = new ArrayList<>();
    tableEnvironment
        .executeSql(
            "SELECT\n"
                + "FILTER_ROW_ARRAY(request_feature_stage.features, 'key', true, CAST(1001 AS BIGINT), CAST(1002 AS BIGINT)),\n"
                + "FILTER_ROW_ARRAY(user_feature_stage.features, 'key', false, CAST(1001 AS BIGINT), CAST(1002 AS BIGINT))\n"
                + "FROM flat_response_insertions")
        .collect()
        .forEachRemaining(result::add);
    Assertions.assertEquals("[+I[[+I[1001, 2.0]], [+I[1003, 3.0]]]]", result.toString());
  }

  @Test
  public void testFirstModelAndCredit() {
    List<Row> result = new ArrayList<>();
    tableEnvironment
        .executeSql(
            "SELECT\n"
                + "FIRST_MODEL_AND_CREDIT(attributed_action, CAST(2 AS BIGINT), 'CHECKOUT')"
                + "FROM flat_response_insertions")
        .collect()
        .forEachRemaining(result::add);
    Assertions.assertEquals("[+I[+I[true, 20]]]", result.toString());
  }

  @Test
  public void testFilterRowArrayByTwoFields() {
    List<Row> result = new ArrayList<>();
    tableEnvironment
        .executeSql(
            "SELECT\n"
                + "FILTER_ROW_ARRAY_BY_TWO_FIELDS(request_feature_stage.features, true, 'value', false, Array[CAST(3.0 AS FLOAT)], 'key', true, Array[CAST(1000 AS BIGINT), CAST(1001 AS BIGINT)]),"
                + "FILTER_ROW_ARRAY_BY_TWO_FIELDS(user_feature_stage.features, FALSE, 'value', true, Array[CAST(3.0 AS FLOAT)], 'key', false, Array[CAST(1000 AS BIGINT), CAST(1001 AS BIGINT), CAST(1002 AS BIGINT)])"
                + "FROM flat_response_insertions")
        .collect()
        .forEachRemaining(result::add);
    Assertions.assertEquals(
        "[+I[[+I[1000, 2.0], +I[1001, 2.0]], [+I[1002, 3.0], +I[1003, 3.0]]]]", result.toString());
  }

  @Test
  public void testToFeatureMap() {
    List<Row> result = new ArrayList<>();
    tableEnvironment
        .executeSql(
            "SELECT\n"
                + "TO_FEATURE_MAP(request_feature_stage.features, 'key', 'value')\n"
                + "FROM flat_response_insertions")
        .collect()
        .forEachRemaining(result::add);
    Assertions.assertEquals("[+I[{1000=2.0, 1001=2.0}]]", result.toString());
  }
}
