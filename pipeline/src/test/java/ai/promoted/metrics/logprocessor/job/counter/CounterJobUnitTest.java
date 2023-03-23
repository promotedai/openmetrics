package ai.promoted.metrics.logprocessor.job.counter;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import ai.promoted.metrics.logprocessor.common.functions.sink.RedisSink;
import ai.promoted.metrics.logprocessor.common.job.testing.BaseJobUnitTest;
import com.google.common.collect.FluentIterable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

/** Unit tests (non-minicluster tests). */
public class CounterJobUnitTest extends BaseJobUnitTest<CounterJob> {
  @Override
  protected CounterJob createJob() {
    CounterJob job = new CounterJob();
    job.counterOutputStartTimestamp = 0;
    job.s3.rootPath = tempDir.getAbsolutePath();
    // Checkpoint more frequently so we don't hit part files.
    job.configureExecutionEnvironment(env, 1, 0);
    return job;
  }

  @Test
  void getInputLabel() {
    CounterJob job = createJob();
    job.jobLabel = "blue";
    assertEquals("blue", job.getInputLabel());
    job.jobLabel = "blue-canary";
    assertEquals("blue-canary", job.getInputLabel());
    job.overrideInputLabel = "blue";
    assertEquals("blue", job.getInputLabel());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void prepareSink(boolean doWipe) throws InterruptedException {
    CounterJob job = createJob();
    job.wipe = doWipe;

    ArgumentCaptor<RedisSink.Command> commandCaptor =
        ArgumentCaptor.forClass(RedisSink.Command.class);

    RedisSink mockSink = mock(RedisSink.class);
    job.prepareSink(mockSink);

    verify(mockSink).initConnection();
    verify(mockSink, times(doWipe ? 22 : 21)).sendCommand(commandCaptor.capture());
    verify(mockSink).closeConnection(true);
    verifyNoMoreInteractions(mockSink);

    FluentIterable<RedisSink.Command> actual = FluentIterable.from(commandCaptor.getAllValues());
    if (doWipe) {
      assertThat(actual).contains(RedisSink.flush());
    }

    // Just spot check a few values.
    assertThat(actual)
        .containsAtLeast(
            RedisSink.ping(),
            RedisSink.hset(
                CounterKeys.ROW_FORMAT_KEY, "platform-device", "os,user_agent,fid:value", -1),
            RedisSink.hset(
                CounterKeys.FEATURE_IDS_KEY,
                "platform-device",
                CounterJob.CSV.join(CounterKeys.GLOBAL_EVENT_DEVICE_KEY.getFeatureIds()),
                -1),
            RedisSink.hset(
                CounterKeys.ROW_FORMAT_KEY, "content-device", "os,user_agent,fid:value", -1),
            RedisSink.hset(
                CounterKeys.FEATURE_IDS_KEY,
                "content-device",
                CounterJob.CSV.join(CounterKeys.CONTENT_EVENT_DEVICE_KEY.getFeatureIds()),
                -1),
            RedisSink.hset(CounterKeys.ROW_FORMAT_KEY, "log-user", "fid:value", -1),
            RedisSink.hset(
                CounterKeys.FEATURE_IDS_KEY,
                "user",
                CounterJob.CSV.join(CounterKeys.USER_EVENT_KEY.getFeatureIds()),
                -1),
            RedisSink.hset(CounterKeys.ROW_FORMAT_KEY, "last-time-log-user-event", "fid:value", -1),
            RedisSink.hset(
                CounterKeys.FEATURE_IDS_KEY,
                "last-time-user-event",
                CounterJob.CSV.join(CounterKeys.LAST_USER_CONTENT_KEY.getFeatureIds()),
                -1),
            RedisSink.hset(CounterKeys.ROW_FORMAT_KEY, "query", "fid:value", -1),
            RedisSink.hset(
                CounterKeys.FEATURE_IDS_KEY,
                "content-query",
                CounterJob.CSV.join(CounterKeys.CONTENT_QUERY_EVENT_KEY.getFeatureIds()),
                -1),
            RedisSink.hset(
                CounterKeys.FEATURE_IDS_KEY,
                "last-time-log-user-query",
                CounterJob.CSV.join(CounterKeys.LAST_LOG_USER_QUERY_KEY.getFeatureIds()),
                -1),
            RedisSink.hset(CounterKeys.ROW_FORMAT_KEY, "last-time-user-query", "fid:value", -1))
        .inOrder();
  }
}
