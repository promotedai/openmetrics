package ai.promoted.metrics.logprocessor.common.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import ai.promoted.metrics.logprocessor.common.job.testing.BaseJobUnitTest;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine.ExecutionException;

/** Unit tests (non-minicluster tests). */
public class BaseFlinkJobUnitTest extends BaseJobUnitTest<BaseFlinkJob> {

  BaseFlinkJob job;

  @BeforeEach
  public void setUp() {
    job = createJob();
  }

  @Override
  protected BaseFlinkJob createJob() {
    BaseFlinkJob job =
        new BaseFlinkJob() {
          @Override
          protected String getDefaultBaseJobName() {
            return "fake-job";
          }

          @Override
          public Set<FlinkSegment> getInnerFlinkSegments() {
            // Do not register Segments passed into the constructor.
            return ImmutableSet.of();
          }

          @Override
          protected void startJob() throws Exception {}
        };
    job.parallelism = 4;
    job.maxParallelism = 24;
    return job;
  }

  @Test
  void getOperatorParallelism_default() {
    assertFalse(job.getOperatorParallelism("uid1").isPresent());
  }

  @Test
  void getOperatorParallelism_multiplier() {
    job.operatorParallelismMultiplier = ImmutableMap.of("uid1", 2.0f);
    assertEquals(8, job.getOperatorParallelism("uid1").get());
    job.operatorParallelismMultiplier = ImmutableMap.of("uid1", 10.0f);
    assertEquals(24, job.getOperatorParallelism("uid1").get());
    job.operatorParallelismMultiplier = ImmutableMap.of("uid1", 0.1f);
    assertEquals(1, job.getOperatorParallelism("uid1").get());
  }

  @Test
  void getSourceParallelism_default() {
    assertEquals(4, job.getSourceParallelism("uid1"));
  }

  @Test
  void getSourceParallelism_value() {
    assertEquals(4, job.getSourceParallelism("uid1"));
    job.operatorParallelism = ImmutableMap.of("uid1", 2);
    assertEquals(2, job.getSourceParallelism("uid1"));
    job.operatorParallelism = ImmutableMap.of("uid1", 10);
    assertEquals(10, job.getSourceParallelism("uid1"));
    job.operatorParallelism = ImmutableMap.of("uid1", 0);
    assertEquals(1, job.getSourceParallelism("uid1"));
  }

  @Test
  void getSourceParallelism_multiplier() {
    assertEquals(4, job.getSourceParallelism("uid1"));
    job.operatorParallelismMultiplier = ImmutableMap.of("uid1", 2.0f);
    assertEquals(8, job.getSourceParallelism("uid1"));
    job.operatorParallelismMultiplier = ImmutableMap.of("uid1", 10.0f);
    assertEquals(24, job.getSourceParallelism("uid1"));
    job.operatorParallelismMultiplier = ImmutableMap.of("uid1", 0.1f);
    assertEquals(1, job.getSourceParallelism("uid1"));
  }

  @Test
  void getSourceParallelism_sinkOverride() {
    assertEquals(4, job.getSourceParallelism("uid1"));
    job.defaultSourceParallelism = 8;
    assertEquals(8, job.getSourceParallelism("uid1"));
    job.defaultSourceParallelism = 50;
    assertEquals(24, job.getSourceParallelism("uid1"));
    job.defaultSourceParallelism = 0;
    assertEquals(4, job.getSourceParallelism("uid1"));
  }

  @Test
  void getSinkParallelism_default() {
    assertEquals(4, job.getSinkParallelism("uid1"));
  }

  @Test
  void getSinkParallelism_multiplier() {
    assertEquals(4, job.getSinkParallelism("uid1"));
    job.operatorParallelismMultiplier = ImmutableMap.of("uid1", 2.0f);
    assertEquals(8, job.getSinkParallelism("uid1"));
    job.operatorParallelismMultiplier = ImmutableMap.of("uid1", 10.0f);
    assertEquals(24, job.getSinkParallelism("uid1"));
    job.operatorParallelismMultiplier = ImmutableMap.of("uid1", 0.1f);
    assertEquals(1, job.getSinkParallelism("uid1"));
  }

  @Test
  void getSinkParallelism_sinkOverride() {
    assertEquals(4, job.getSinkParallelism("uid1"));
    job.defaultSinkParallelism = 8;
    assertEquals(8, job.getSinkParallelism("uid1"));
    job.defaultSinkParallelism = 50;
    assertEquals(24, job.getSinkParallelism("uid1"));
    job.defaultSinkParallelism = 0;
    assertEquals(4, job.getSinkParallelism("uid1"));
  }

  @Test
  void getJobName() {
    assertEquals("fake-job", job.getJobName());
    job.jobName = "other-name";
    assertEquals("other-name", job.getJobName());
    job.jobLabel = "green";
    assertEquals("green.other-name", job.getJobName());
    job.jobName = "";
    assertEquals("green.fake-job", job.getJobName());
  }

  @Test
  void testFailWithMissingParameters() {
    ExecutionException exception =
        Assertions.assertThrows(
            ExecutionException.class, () -> BaseFlinkJob.executeMain(job, new String[] {}));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains("picocli.CommandLine$MissingParameterException: Missing required option:"));
  }

  @Test
  void getInputLabel() {
    job.jobLabel = "blue";
    assertEquals("blue", job.getInputLabel("log-user-user"));
    job.jobLabel = "blue-canary";
    assertEquals("blue-canary", job.getInputLabel("log-user-user"));
    job.inputLabel = "blue";
    assertEquals("blue", job.getInputLabel("log-user-user"));
    job.topicInputLabel = ImmutableMap.of("log-user-user", "fix-blue");
    assertEquals("fix-blue", job.getInputLabel("log-user-user"));
  }
}
