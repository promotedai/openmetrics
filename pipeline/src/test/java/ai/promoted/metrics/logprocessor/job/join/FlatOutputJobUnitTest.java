package ai.promoted.metrics.logprocessor.job.join;

import ai.promoted.metrics.logprocessor.common.job.testing.BaseJobUnitTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests (non-minicluster tests).
 */
public class FlatOutputJobUnitTest extends BaseJobUnitTest<FlatOutputJob> {
  // TODO - add other tests for very little data.  The test code requires lists to be not empty.

  @Override
  protected FlatOutputJob createJob() {
    FlatOutputJob job = new FlatOutputJob();
    job.maxParallelism = 1;
    job.s3FileOutput.s3OutputDirectory = tempDir.getAbsolutePath();
    job.writeJoinedEventsToKafka = false;
    job.configureExecutionEnvironment(env, 1, 0);
    return job;
  }

  @Test
  public void getJoinS3OuputDirectory() {
    FlatOutputJob job = createJob();
    assertEquals(tempDir + "/", job.s3FileOutput.getOutputS3Dir().build().toString());
    assertEquals(tempDir + "/foo/", job.s3FileOutput.getOutputS3Dir("foo").build().toString());
    assertEquals(tempDir + "/foo/bar/", job.s3FileOutput.getOutputS3Dir("foo", "bar").build().toString());

    job.jobLabel = "qa-prod";
    assertEquals(tempDir + "/qa-prod/", job.s3FileOutput.getOutputS3Dir().build().toString());
    assertEquals(tempDir + "/qa-prod/foo/", job.s3FileOutput.getOutputS3Dir("foo").build().toString());
    assertEquals(tempDir + "/qa-prod/foo/bar/", job.s3FileOutput.getOutputS3Dir("foo", "bar").build().toString());
  }

  @Test
  public void getJoinJobName() {
    FlatOutputJob job = createJob();
    assertEquals("join-event", job.getJobName());
    job.jobLabel = "qa-prod";
    assertEquals("qa-prod.join-event", job.getJobName());
  }

  @Test
  public void toJoinConsumerGroupId() {
    FlatOutputJob job = createJob();
    assertEquals("joinimpression", job.toKafkaConsumerGroupId("joinimpression"));
    job.jobLabel = "qa-prod";
    assertEquals("qa-prod.joinimpression", job.toKafkaConsumerGroupId("joinimpression"));
  }
}