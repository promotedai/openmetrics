package ai.promoted.metrics.logprocessor.common.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import ai.promoted.metrics.logprocessor.common.job.testing.BaseJobUnitTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.GeneratedMessageV3;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
          protected String getJobName() {
            return null;
          }

          @Override
          public List<Class<? extends GeneratedMessageV3>> getProtoClasses() {
            return ImmutableList.of();
          }

          @Override
          public void validateArgs() {}

          @Override
          public Integer call() throws Exception {
            return null;
          }
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
  void getSinkParallelism_default() {
    assertEquals(1, job.getSinkParallelism("uid1"));
  }

  @Test
  void getSinkParallelism_multiplier() {
    job.operatorParallelismMultiplier = ImmutableMap.of("uid1", 2.0f);
    assertEquals(8, job.getSinkParallelism("uid1"));
    job.operatorParallelismMultiplier = ImmutableMap.of("uid1", 10.0f);
    assertEquals(24, job.getSinkParallelism("uid1"));
    job.operatorParallelismMultiplier = ImmutableMap.of("uid1", 0.1f);
    assertEquals(1, job.getSinkParallelism("uid1"));
  }

  @Test
  void getSinkParallelism_sinkMultiplier() {
    job.defaultSinkParallelismMultiplier = 2.0f;
    assertEquals(8, job.getSinkParallelism("uid1"));
    job.defaultSinkParallelismMultiplier = 10.0f;
    assertEquals(24, job.getSinkParallelism("uid1"));
    job.defaultSinkParallelismMultiplier = 0.1f;
    assertEquals(1, job.getSinkParallelism("uid1"));
  }

  @Test
  void getSinkParallelism_sinkOverride() {
    job.defaultSinkParallelism = 8;
    assertEquals(8, job.getSinkParallelism("uid1"));
    job.defaultSinkParallelism = 50;
    assertEquals(24, job.getSinkParallelism("uid1"));
    job.defaultSinkParallelism = 0;
    assertEquals(1, job.getSinkParallelism("uid1"));
  }
}
