package ai.promoted.metrics.logprocessor.common.job;

import ai.promoted.metrics.logprocessor.common.util.StringUtil;
import ai.promoted.metrics.logprocessor.common.util.TableUtil;
import com.google.common.base.Preconditions;
import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.internal.AbstractStreamTableEnvironmentImpl;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import picocli.CommandLine;

public abstract class BaseFlinkTableJob extends BaseFlinkJob {
  @CommandLine.Option(
      names = {"--databaseName"},
      required = true,
      description = "The name for the main output database. Default = null")
  public String databaseName;

  @CommandLine.Option(
      names = {"--sideDatabaseName"},
      required = true,
      description = "The name for the side output database. Default = null")
  public String sideDatabaseName;

  @CommandLine.Option(
      names = {"--tablePartitionByHour"},
      description =
          "Table specific partition by hour option. The key should be {DATABASE}.{TABLE}. Default = empty")
  public Map<String, Boolean> tablePartitionByHour = Collections.emptyMap();

  @FeatureFlag
  @CommandLine.Option(
      names = {"--partitionByHour"},
      description = "Global partition by hour option.  Default=false")
  public boolean partitionByHour = false;

  protected Boolean isPartitionByHour(String database, String table) {
    String databaseAndTable = TableUtil.databaseAndTableName(database, table);
    return tablePartitionByHour.getOrDefault(databaseAndTable, partitionByHour);
  }

  protected StreamTableEnvironment tEnv;

  public static List<String> genPartitionFields(boolean partitionByHour) {
    if (partitionByHour) {
      return List.of("dt", "hr");
    } else {
      return List.of("dt");
    }
  }

  /** Generate a list of time extraction expressions based on a long format timestamp field. */
  public static List<String> genExtractedExtraFields(
      String longTimeField, List<String> extraFields) {
    List<String> result = new ArrayList<>(genExtractedExtraFields(longTimeField));
    result.addAll(extraFields);
    return result;
  }

  public static List<String> genExtractedExtraFields(String longField) {
    return List.of(
        "DATE_FORMAT(TO_TIMESTAMP_LTZ(" + longField + ", 3), 'yyyy-MM-dd') dt",
        "DATE_FORMAT(TO_TIMESTAMP_LTZ(" + longField + ", 3), 'HH') hr");
  }

  public abstract void tableOperationsToDataStream();

  public void prepareToExecute() throws Exception {
    tableOperationsToDataStream();
    // Set parallelisms
    Field field = AbstractStreamTableEnvironmentImpl.class.getDeclaredField("executionEnvironment");
    field.setAccessible(true);
    StreamExecutionEnvironment env = (StreamExecutionEnvironment) field.get(tEnv);
    Set<Integer> visited = new HashSet<>();
    ArrayDeque<Transformation<?>> deque = new ArrayDeque<>();
    env.getTransformations()
        .forEach(
            transformation -> {
              if (!visited.contains(transformation.getId())) {
                deque.add(transformation);
                visited.add(transformation.getId());
                while (!deque.isEmpty()) {
                  Transformation<?> t = deque.removeFirst();
                  // We use the transformation name for now since if the uid is set, it's always
                  // equal to the name.
                  getOperatorParallelism(t.getName()).ifPresent(t::setParallelism);
                  t.getInputs()
                      .forEach(
                          input -> {
                            if (!visited.contains(input.getId())) {
                              deque.add(input);
                              visited.add(input.getId());
                            }
                          });
                }
              }
            });
  }

  // TODO Xingcan: get rid of mutable env settings. Refactor the jobs to properly take
  //  StreamExecutionEnv and StreamTableEnv.
  public void setTableEnv(StreamTableEnvironment tEnv) {
    this.tEnv = tEnv;
  }

  @Override
  public void validateArgs() {
    super.validateArgs();
    Preconditions.checkArgument(
        !StringUtil.isBlank(databaseName), "--databaseName must be not empty.");
    Preconditions.checkArgument(
        !StringUtil.isBlank(sideDatabaseName), "--sideDatabaseName must be not empty.");
  }

  public StreamTableEnvironment getStreamTableEnvironment() {
    return tEnv;
  }
}
