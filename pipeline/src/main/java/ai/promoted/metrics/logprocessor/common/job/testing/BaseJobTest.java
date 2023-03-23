package ai.promoted.metrics.logprocessor.common.job.testing;

import ai.promoted.metrics.logprocessor.common.job.BaseFlinkJob;
import java.io.File;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

/** ABC for a Flink job test. */
public abstract class BaseJobTest<JOB extends BaseFlinkJob> {
  public static final String KAFKA_CONTAINER_IMAGE =
      isArmArch() ? "confluentinc/cp-kafka:7.3.1.arm64" : "confluentinc/cp-kafka:7.3.1";

  @TempDir protected File tempDir;

  protected StreamExecutionEnvironment env;

  protected abstract JOB createJob();

  protected abstract Configuration getClientConfiguration();

  public static boolean isArmArch() {
    return System.getProperty("os.arch").equals("aarch64");
  }

  @BeforeEach
  public void setUp() {
    env = createTestStreamExecutionEnvironment();
  }

  protected RuntimeExecutionMode getRuntimeMode() {
    return RuntimeExecutionMode.STREAMING;
  }

  protected StreamExecutionEnvironment createTestStreamExecutionEnvironment() {
    StreamExecutionEnvironment env = new StreamExecutionEnvironment(getClientConfiguration());
    env.setStateBackend(new MemoryStateBackend(false));
    env.setRuntimeMode(getRuntimeMode());
    env.getConfig().setAutoWatermarkInterval(50);

    // Force the timeouts to be longer for tests.
    env.getCheckpointConfig().setCheckpointTimeout(30000);
    return env;
  }
}
