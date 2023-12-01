package ai.promoted.metrics.logprocessor.common.job;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class S3SegmentUnitTest {
  @TempDir protected File tempDir;

  @Test
  public void getRawS3OuputDirectory() {
    FakeFlinkJob job = new FakeFlinkJob();
    S3Segment s3 = new S3Segment(job);
    s3.rootPath = tempDir.getAbsolutePath();
    assertEquals(tempDir + "/", s3.getOutputDir().build().toString());
    assertEquals(tempDir + "/foo/", s3.getOutputDir("foo").build().toString());
    assertEquals(tempDir + "/foo/bar/", s3.getOutputDir("foo", "bar").build().toString());

    job.jobLabel = "green";
    assertEquals(tempDir + "/green/", s3.getOutputDir().build().toString());
    assertEquals(tempDir + "/green/foo/", s3.getOutputDir("foo").build().toString());
    assertEquals(tempDir + "/green/foo/bar/", s3.getOutputDir("foo", "bar").build().toString());
  }

  private static class FakeFlinkJob extends BaseFlinkJob {

    @Override
    protected String getDefaultBaseJobName() {
      return "fake-job";
    }

    @Override
    public Set<FlinkSegment> getInnerFlinkSegments() {
      return ImmutableSet.of();
    }

    @Override
    protected void startJob() throws Exception {}
  }
}
