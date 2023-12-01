package ai.promoted.metrics.logprocessor.common.job.testing;

import ai.promoted.metrics.logprocessor.common.job.BaseFlinkJob;
import java.io.File;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

/** ABC for a Flink job test. */
public abstract class BaseJobTest<JOB extends BaseFlinkJob> {
  public static final String KAFKA_CONTAINER_IMAGE =
      isArmArch() ? "confluentinc/cp-kafka:7.3.1.arm64" : "confluentinc/cp-kafka:7.3.1";

  public static final String REDIS_CONTAINER_IMAGE = "redis:7.0.10";

  @TempDir protected File tempDir;

  protected StreamExecutionEnvironment env;

  public static boolean isArmArch() {
    return System.getProperty("os.arch").equals("aarch64");
  }

  protected abstract JOB createJob();

  protected abstract Configuration getClientConfiguration();

  @BeforeEach
  public void setUp() {
    env = createTestStreamExecutionEnvironment();
  }

  protected RuntimeExecutionMode getRuntimeMode() {
    return RuntimeExecutionMode.STREAMING;
  }

  protected StreamExecutionEnvironment createTestStreamExecutionEnvironment() {
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment(getClientConfiguration());
    env.setStateBackend(new EmbeddedRocksDBStateBackend());
    env.getCheckpointConfig()
        .setCheckpointStorage("file://" + tempDir.getAbsolutePath() + "/checkpoint");
    env.setRuntimeMode(getRuntimeMode());
    env.getConfig().setAutoWatermarkInterval(50);
    env.getConfig().setRestartStrategy(RestartStrategies.noRestart());

    // Force the timeouts to be longer for tests.
    env.getCheckpointConfig().setCheckpointTimeout(30000);
    return env;
  }
}
