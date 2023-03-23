package ai.promoted.metrics.logprocessor.common.testing;

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

  private static Configuration flinkConfig = new Configuration();
  private static MiniClusterResourceConfiguration miniClusterResourceConfig =
      new MiniClusterResourceConfiguration.Builder()
          .setNumberSlotsPerTaskManager(12)
          .setNumberTaskManagers(1)
          .setConfiguration(flinkConfig)
          .build();
  public static MiniClusterWithClientResource flinkCluster =
      new MiniClusterWithClientResource(miniClusterResourceConfig);

  static {
    flinkConfig.setFloat("taskmanager.memory.network.fraction", 0.4f);
    flinkConfig.setString("taskmanager.memory.network.min", "16mb");
    flinkConfig.set(TaskManagerOptions.MANAGED_MEMORY_FRACTION, 0.4f);
    flinkConfig.setString("taskmanager.memory.network.max", "16mb");
    //    flinkConfig.setString("state.backend", "memory");
    flinkConfig.setString("taskmanager.memory.segment-size", "4kb");
    flinkConfig.setBoolean("execution.checkpointing.checkpoints-after-tasks-finish.enabled", true);

    // Dan 2022-07-08 - testing with this off.
    // flinkConfig.setString("restart-strategy", "none");
    flinkConfig.setInteger("execution.checkpointing.tolerable-failed-checkpoints", 3);
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    flinkCluster.before();
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    flinkCluster.after();
  }
}
