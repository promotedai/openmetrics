package ai.promoted.metrics.logprocessor.common.job.testing;

import ai.promoted.metrics.logprocessor.common.functions.base.SerializableFunction;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableToLongFunction;
import ai.promoted.metrics.logprocessor.common.job.BaseFlinkJob;
import ai.promoted.metrics.logprocessor.common.testing.MiniClusterExtension;
import ai.promoted.metrics.logprocessor.common.testing.MiniclusterUtils;
import ai.promoted.proto.common.Timing;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.client.deployment.executors.RemoteExecutor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;

/** ABC for a Flink job minicluster integration test. */
public abstract class BaseJobMiniclusterTest<JOB extends BaseFlinkJob> extends BaseJobTest<JOB> {
  private static final Logger LOGGER = LogManager.getLogger(BaseJobMiniclusterTest.class);

  // TODO - change the test watermark strategy after updating production.
  // I don't know how to properly implement watermarks and timestamps in tests when faking out a
  // Kafka source.
  // For now, just override how timestamps are assigned.
  protected static <T> WatermarkStrategy<T> getTestWatermarkStrategy(
      SerializableToLongFunction<T> getTimestamp) {
    return ((WatermarkStrategy<T>)
            context ->
                new WatermarkGenerator<>() {
                  @Override
                  public void onEvent(T t, long l, WatermarkOutput watermarkOutput) {
                    Long timestamp = getTimestamp.applyAsLong(t);
                    watermarkOutput.emitWatermark(
                        new org.apache.flink.api.common.eventtime.Watermark(timestamp - 1));
                  }

                  @Override
                  public void onPeriodicEmit(WatermarkOutput watermarkOutput) {}
                })
        .withTimestampAssigner(
            (SerializableTimestampAssigner<T>) (t, l) -> getTimestamp.applyAsLong(t));
  }

  protected static void waitForDone(JobExecutionResult result)
      throws InterruptedException, ExecutionException {
    waitForDone(result.getJobID());
  }

  protected static void waitForDone(JobID jobID) throws InterruptedException, ExecutionException {
    // You can uncomment these line if you sleep on all waitForDones and get the REST API.
    //
    // LOGGER.info("MiniCluster Flink REST API=" +
    // MiniClusterExtension.flinkCluster.getClusterClient().getWebInterfaceURL());
    // Thread.sleep(2 * 60 * 1000); // Sleep while interacting with the REST API.
    //
    // Also change:
    // 1. `MiniclusterUtils.shouldWait`.
    // 2. `java_junit5_test`'s `timeout` field in BUILD files.
    // 3. `BaseJobTest`'s `setCheckpointTimeout` duration to slightly shorter than the timeout in
    // step 2.
    MiniclusterUtils.waitForDone(
        MiniClusterExtension.flinkCluster.getClusterClient(), jobID, Duration.ofMinutes(5));
  }

  protected <T> SingleOutputStreamOperator<T> fromItems(
      StreamExecutionEnvironment env,
      String name,
      List<T> data,
      SerializableFunction<T, Timing> getTiming) {
    return fromItems(env, name, record -> getTiming.apply(record).getEventApiTimestamp(), data);
  }

  protected <T> SingleOutputStreamOperator<T> fromItems(
      StreamExecutionEnvironment env,
      String name,
      SerializableToLongFunction<T> getEventTimeMillis,
      List<T> data) {
    return fromItems(env, name, data, getTestWatermarkStrategy(getEventTimeMillis));
  }

  private <T> SingleOutputStreamOperator<T> fromItems(
      StreamExecutionEnvironment env,
      String name,
      List<T> data,
      WatermarkStrategy<T> watermarkStrategy) {
    if (data.isEmpty()) {
      throw new IllegalArgumentException(
          "data must be non-empty.  If you want empty, look at the other fromItems");
    }
    return fromItems(
        env, name, data, TypeInformation.of((Class<T>) data.get(0).getClass()), watermarkStrategy);
  }

  protected <T> SingleOutputStreamOperator<T> fromItems(
      StreamExecutionEnvironment env,
      String name,
      List<T> data,
      SerializableToLongFunction<T> getEventTimeMillis,
      TypeInformation<T> type) {
    return fromItems(env, name, data, type, getTestWatermarkStrategy(getEventTimeMillis));
  }

  protected <T> SingleOutputStreamOperator<T> fromItems(
      StreamExecutionEnvironment env,
      String name,
      List<T> data,
      TypeInformation<T> type,
      SerializableFunction<T, Timing> getTiming) {
    return fromItems(
        env, name, data, record -> getTiming.apply(record).getEventApiTimestamp(), type);
  }

  protected <T> SingleOutputStreamOperator<T> fromItems(
      StreamExecutionEnvironment env,
      String name,
      List<T> data,
      TypeInformation<T> type,
      WatermarkStrategy<T> watermarkStrategy) {
    return env.fromCollection(data, type)
        .uid("source-" + name)
        .name("source-" + name)
        .assignTimestampsAndWatermarks(watermarkStrategy)
        .uid("assign-timestamps-and-watermarks-" + name)
        .name("assign-timestamps-and-watermarks-" + name);
  }

  protected <T> SingleOutputStreamOperator<T> fromCollectionSource(
      StreamExecutionEnvironment env,
      String name,
      SourceFunction<T> input,
      TypeInformation<T> type,
      SerializableFunction<T, Timing> getTiming) {
    return fromCollectionSource(
        env,
        name,
        input,
        type,
        getTestWatermarkStrategy(record -> getTiming.apply(record).getEventApiTimestamp()));
  }

  private <T> SingleOutputStreamOperator<T> fromCollectionSource(
      StreamExecutionEnvironment env,
      String name,
      SourceFunction<T> input,
      TypeInformation<T> type,
      WatermarkStrategy<T> watermarkStrategy) {
    return env.addSource(input)
        .returns(type)
        .uid("source-" + name)
        .name("source-" + name)
        .assignTimestampsAndWatermarks(watermarkStrategy)
        .uid("assign-timestamps-and-watermarks-" + name)
        .name("assign-timestamps-and-watermarks-" + name);
  }

  @BeforeEach
  public void setUp() {
    env = createTestStreamExecutionEnvironment();
  }

  @Override
  protected Configuration getClientConfiguration() {
    Configuration result =
        new Configuration(MiniClusterExtension.flinkCluster.getClientConfiguration());
    result.setString("fs.s3a.connection.maximum", "100");
    result.set(DeploymentOptions.TARGET, RemoteExecutor.NAME);
    // Forces the final checkpoint. This is important to getting file outputs in minicluster tests.
    result.setBoolean(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
    return result;
  }

  /** SourceFunction used to provide source streams for test input. */
  public static class CollectionSource<T> extends RichSourceFunction<T> {
    private final Collection<T> input;
    private final SerializableFunction<T, Long> timeGetter;
    private long lastTimestamp = -1L;

    public CollectionSource(Collection<T> input, SerializableFunction<T, Long> timeGetter) {
      this.input = input;
      this.timeGetter = timeGetter;
    }

    @Override
    public void run(SourceContext<T> ctx) {
      // Stream with punctuated watermark behavior.
      for (T i : input) {
        lastTimestamp = timeGetter.apply(i);
        ctx.collectWithTimestamp(i, lastTimestamp);
        ctx.emitWatermark(new Watermark(lastTimestamp));
      }
      // One last watermark to trigger any remaining event timers.
      lastTimestamp += Duration.ofDays(3650).toMillis();
      ctx.emitWatermark(new Watermark(lastTimestamp));
    }

    @Override
    public void cancel() {}
  }
}
