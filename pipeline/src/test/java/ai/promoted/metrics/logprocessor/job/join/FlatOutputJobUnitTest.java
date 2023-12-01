package ai.promoted.metrics.logprocessor.job.join;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.promoted.metrics.logprocessor.common.job.testing.BaseJobUnitTest;
import org.junit.jupiter.api.Test;

/** Unit tests (non-minicluster tests). */
public class FlatOutputJobUnitTest extends BaseJobUnitTest<FlatOutputJob> {
  // TODO - add other tests for very little data.  The test code requires lists to be not empty.

  @Override
  protected FlatOutputJob createJob() {
    FlatOutputJob job = new FlatOutputJob();
    job.maxParallelism = 1;
    job.s3.rootPath = tempDir.getAbsolutePath();
    job.writeJoinedEventsToKafka = false;
    job.configureExecutionEnvironment(env, 1, 0);
    return job;
  }

  @Test
  public void toJoinConsumerGroupId() {
    FlatOutputJob job = createJob();
    assertEquals("joinimpression", job.toKafkaConsumerGroupId("joinimpression"));
    job.jobLabel = "qa-prod";
    assertEquals("qa-prod.joinimpression", job.toKafkaConsumerGroupId("joinimpression"));
  }
}
