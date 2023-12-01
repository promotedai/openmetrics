package ai.promoted.metrics.logprocessor.common.testing;

import com.google.common.collect.ImmutableMap;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Starts a Flink mini cluster as a resource and registers the respective ExecutionEnvironment and
 * StreamExecutionEnvironment.
 *
 * <p>This junit5 extension wraps a junit4 rule.
 */
public class MiniClusterExtension implements BeforeEachCallback, AfterEachCallback {

  private static final Configuration flinkConfig =
      Configuration.fromMap(
          ImmutableMap.<String, String>builder()
              .put("taskmanager.memory.network.min", "16mb")
              .put(TaskManagerOptions.MANAGED_MEMORY_FRACTION.key(), "0.4f")
              .put("taskmanager.memory.network.max", "16mb")
              .put("taskmanager.memory.segment-size", "4kb")
              .put("execution.checkpointing.checkpoints-after-tasks-finish.enabled", "true")
              .put("execution.checkpointing.tolerable-failed-checkpoints", "3")
              .build());
  private static final MiniClusterResourceConfiguration miniClusterResourceConfig =
      new MiniClusterResourceConfiguration.Builder()
          .setNumberSlotsPerTaskManager(2)
          .setNumberTaskManagers(1)
          .setConfiguration(flinkConfig)
          .build();
  public static MiniClusterWithClientResource flinkCluster =
      new MiniClusterWithClientResource(miniClusterResourceConfig);

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    flinkCluster.before();
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    flinkCluster.after();
  }
}
