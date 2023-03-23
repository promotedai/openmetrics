package ai.promoted.metrics.logprocessor.common.job;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests (non-minicluster tests). */
public class KafkaSegmentUnitTest {

  KafkaSegment segment;

  @BeforeEach
  public void setUp() {
    segment = new KafkaSegment(null);
  }

  // Most of the accessors on OffsetsInitializer are package private or private.  Just make sure the
  // calls pass.
  @Test
  public void getOffsetsInitializer_noStartFrom() {
    assertNotNull(segment.getOffsetsInitializer(0));
  }

  @Test
  public void getOffsetsInitializer_startFromEarliest() {
    segment.startFromEarliest = true;
    assertNotNull(segment.getOffsetsInitializer(0));
  }

  @Test
  public void getOffsetsInitializer_startFromTimestamp() {
    segment.startFromTimestamp = 1000;
    assertNotNull(segment.getOffsetsInitializer(0));
  }

  @Test
  public void getOffsetsInitializer_startFromTimestampOverride() {
    assertNotNull(segment.getOffsetsInitializer(1000));
  }

  @Test
  public void getOffsetsInitializer_startFromTimestampOverride_override() {
    segment.startFromTimestamp = 1000;
    assertNotNull(segment.getOffsetsInitializer(2000));
  }

  // Test in a bunch of bad configurations.
  @Test
  public void getOffsetsInitializer_throwsBadArgs() {
    segment.startFromEarliest = true;
    segment.startFromTimestamp = 1000;
    assertThrows(IllegalArgumentException.class, () -> segment.getOffsetsInitializer(1000));
    segment.startFromTimestamp = 0;
    assertThrows(IllegalArgumentException.class, () -> segment.getOffsetsInitializer(1000));
  }
}
