package ai.promoted.metrics.logprocessor.job.raw;

import ai.promoted.metrics.logprocessor.common.job.testing.BaseJobUnitTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests (non-minicluster tests).
 */
public class RawOutputJobUnitTest extends BaseJobUnitTest<RawOutputJob> {
  @Override
  protected RawOutputJob createJob() {
    RawOutputJob job = new RawOutputJob();
    job.s3FileOutput.s3OutputDirectory = tempDir.getAbsolutePath();
    job.configureExecutionEnvironment(env, 1, 0);
    return job;
  }

  @Test
  public void getRawS3OuputDirectory() {
    RawOutputJob job = createJob();
    assertEquals(tempDir + "/", job.s3FileOutput.getOutputS3Dir().build().toString());
    assertEquals(tempDir + "/foo/", job.s3FileOutput.getOutputS3Dir("foo").build().toString());
    assertEquals(tempDir + "/foo/bar/", job.s3FileOutput.getOutputS3Dir("foo", "bar").build().toString());
  }
}
