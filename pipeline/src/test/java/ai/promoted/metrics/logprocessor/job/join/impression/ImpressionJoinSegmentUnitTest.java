package ai.promoted.metrics.logprocessor.job.join.impression;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.junit.jupiter.api.Test;

public class ImpressionJoinSegmentUnitTest {

  @Test
  public void validate() throws Exception {
    ImpressionJoinSegment segment = new ImpressionJoinSegment();
    segment.validateArgs();
    segment.insertionImpressionJoinMin = Duration.parse("PT-10M");
    segment.validateArgs();
    segment.insertionImpressionJoinMaxOutOfOrder = Duration.parse("PT1M");
    segment.validateArgs();
  }

  @Test
  public void invalid_insertionImpressionJoinMin() throws Exception {
    ImpressionJoinSegment segment = new ImpressionJoinSegment();
    segment.insertionImpressionJoinMin = Duration.parse("PT0M");
    assertThrows(IllegalArgumentException.class, () -> segment.validateArgs());
    segment.insertionImpressionJoinMin = Duration.parse("PT30M");
    assertThrows(IllegalArgumentException.class, () -> segment.validateArgs());
  }

  @Test
  public void invalid_insertionImpressionJoinMaxOutOfOrder() throws Exception {
    ImpressionJoinSegment segment = new ImpressionJoinSegment();
    segment.insertionImpressionJoinMaxOutOfOrder = Duration.parse("-PT30M");
    assertThrows(IllegalArgumentException.class, () -> segment.validateArgs());
  }
}
