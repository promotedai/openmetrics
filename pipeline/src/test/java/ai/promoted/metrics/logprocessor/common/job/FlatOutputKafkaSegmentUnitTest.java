package ai.promoted.metrics.logprocessor.common.job;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests (non-minicluster tests). */
public class FlatOutputKafkaSegmentUnitTest {

  BaseFlinkJob job;
  FlatOutputKafkaSegment segment;

  @BeforeEach
  public void setUp() {
    job =
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
    segment = new FlatOutputKafkaSegment(job, new KafkaSegment());
  }

  @Test
  public void getJoinedImpressionSourceTopic() {
    job.jobLabel = "";
    assertEquals("metrics.default.joined-impression", segment.getJoinedImpressionSourceTopic());
    job.inputLabel = "blue";
    assertEquals(
        "metrics.blue.default.joined-impression", segment.getJoinedImpressionSourceTopic());
  }

  @Test
  public void getJoinedImpressionSinkTopic() {
    job.jobLabel = "";
    assertEquals("metrics.default.joined-impression", segment.getJoinedImpressionSourceTopic());
    job.jobLabel = "blue";
    assertEquals(
        "metrics.blue.default.joined-impression", segment.getJoinedImpressionSourceTopic());
  }

  @Test
  public void getAttributedActionSourceTopic() {
    job.jobLabel = "";
    assertEquals("metrics.default.attributed-action", segment.getAttributedActionSourceTopic());
    job.inputLabel = "blue";
    assertEquals(
        "metrics.blue.default.attributed-action", segment.getAttributedActionSourceTopic());
  }

  @Test
  public void getAttributedActionSinkTopic() {
    job.jobLabel = "";
    assertEquals("metrics.default.attributed-action", segment.getAttributedActionSourceTopic());
    job.jobLabel = "blue";
    assertEquals(
        "metrics.blue.default.attributed-action", segment.getAttributedActionSourceTopic());
  }
}
