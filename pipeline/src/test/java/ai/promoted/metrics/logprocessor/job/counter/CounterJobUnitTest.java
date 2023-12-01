package ai.promoted.metrics.logprocessor.job.counter;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import ai.promoted.metrics.logprocessor.common.functions.sink.RedisSink;
import ai.promoted.metrics.logprocessor.common.functions.sink.RedisSink.RedisSinkCommand;
import ai.promoted.metrics.logprocessor.common.job.testing.BaseJobUnitTest;
import com.google.common.collect.FluentIterable;
import java.util.Objects;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

/** Unit tests (non-minicluster tests). */
public class CounterJobUnitTest extends BaseJobUnitTest<CounterJob> {
  @Override
  protected CounterJob createJob() {
    CounterJob job = new CounterJob();
    job.jobLabel = "blue";
    job.counterOutputStartTimestamp = 0;
    job.s3.rootPath = tempDir.getAbsolutePath();
    env.setDefaultSavepointDirectory("file://" + tempDir.getAbsolutePath() + "/savepoint");
    // Checkpoint more frequently so we don't hit part files.
    job.configureExecutionEnvironment(env, 1, 0);
    return job;
  }

  @Test
  @Disabled("https://issues.apache.org/jira/browse/FLINK-23515")
  void testCheckpointPath() {
    createJob();
    String expectedCheckpointPath =
        "file://" + tempDir.getAbsolutePath() + "/checkpoint/" + "blue.counter";
    String expectedDefaultSavepointPath =
        "file://" + tempDir.getAbsolutePath() + "/savepoint/" + "blue.counter";
    assertEquals(
        expectedCheckpointPath,
        ((FileSystemCheckpointStorage)
                Objects.requireNonNull(env.getCheckpointConfig().getCheckpointStorage()))
            .getCheckpointPath()
            .toUri()
            .toString());
    assertEquals(
        expectedDefaultSavepointPath,
        Objects.requireNonNull(env.getDefaultSavepointDirectory()).toUri().toString());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void prepareSink(boolean doWipe) throws Exception {
    CounterJob job = createJob();
    job.wipe = doWipe;

    ArgumentCaptor<RedisSinkCommand> commandCaptor =
        ArgumentCaptor.forClass(RedisSinkCommand.class);

    RedisSink mockSink = mock(RedisSink.class);
    job.prepareSink(mockSink);

    verify(mockSink).initConnection();
    verify(mockSink, times(doWipe ? 2 : 1)).sendCommand(commandCaptor.capture());
    verify(mockSink).closeConnection(true);
    verifyNoMoreInteractions(mockSink);

    FluentIterable<RedisSinkCommand> actual = FluentIterable.from(commandCaptor.getAllValues());
    if (doWipe) {
      assertThat(actual).contains(RedisSink.flush());
    }

    assertThat(actual).contains(RedisSink.ping());
  }
}
