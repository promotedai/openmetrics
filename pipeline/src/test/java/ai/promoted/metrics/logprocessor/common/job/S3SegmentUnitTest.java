package ai.promoted.metrics.logprocessor.common.job;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.GeneratedMessageV3;
import java.io.File;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class S3SegmentUnitTest {
  @TempDir protected File tempDir;

  @Test
  public void getRawS3OuputDirectory() {
    FakeFlinkJob job = new FakeFlinkJob();
    S3Segment s3 = new S3Segment(job);
    s3.rootPath = tempDir.getAbsolutePath();
    assertEquals(tempDir + "/", s3.getDir().build().toString());
    assertEquals(tempDir + "/foo/", s3.getDir("foo").build().toString());
    assertEquals(tempDir + "/foo/bar/", s3.getDir("foo", "bar").build().toString());

    job.jobLabel = "green";
    assertEquals(tempDir + "/green/", s3.getDir().build().toString());
    assertEquals(tempDir + "/green/foo/", s3.getDir("foo").build().toString());
    assertEquals(tempDir + "/green/foo/bar/", s3.getDir("foo", "bar").build().toString());
  }

  private static class FakeFlinkJob extends BaseFlinkJob {

    @Override
    protected String getJobName() {
      return null;
    }

    @Override
    public List<Class<? extends GeneratedMessageV3>> getProtoClasses() {
      return null;
    }

    @Override
    public void validateArgs() {}

    @Override
    public Integer call() throws Exception {
      return null;
    }
  }
}
