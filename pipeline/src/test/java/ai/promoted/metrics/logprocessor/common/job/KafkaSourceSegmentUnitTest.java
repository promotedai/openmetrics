package ai.promoted.metrics.logprocessor.common.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.internal.matchers.apachecommons.ReflectionEquals;

/** Unit tests (non-minicluster tests). */
public class KafkaSourceSegmentUnitTest {

  KafkaSourceSegment segment;

  @BeforeEach
  public void setUp() {
    segment = new KafkaSourceSegment(null, null);
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

  @Test
  void testValidatingEndAt() {
    segment.validateArgs();
    segment.endAt = "hahahaha";
    assertThrows(IllegalArgumentException.class, () -> segment.validateArgs());
    segment.endAt = "timestamp: 11111111111111111111"; // the timestamp is too long
    assertThrows(IllegalArgumentException.class, () -> segment.validateArgs());
    segment.endAt = "committed_offset";
    assertThrows(IllegalArgumentException.class, () -> segment.validateArgs());
  }

  @Test
  void testSettingEndAtEarliest() {
    segment.endAt = "earliest  ";
    segment.validateArgs();
    assertTrue(
        new ReflectionEquals(OffsetsInitializer.earliest())
            .matches(segment.getEndAtOffsetsInitializer()));
  }

  @Test
  void testSettingEndAtLatest() {
    segment.endAt = "  LATEst  ";
    segment.validateArgs();
    assertTrue(
        new ReflectionEquals(OffsetsInitializer.latest())
            .matches(segment.getEndAtOffsetsInitializer()));
  }

  @Test
  void testSettingEndAtCommittedOffsets() {
    segment.endAt = "COMMITTED_OFFSETS";
    segment.validateArgs();
    assertTrue(
        new ReflectionEquals(OffsetsInitializer.committedOffsets())
            .matches(segment.getEndAtOffsetsInitializer()));
  }

  @Test
  void testSettingEndAtTimestamp() {
    segment.endAt = " TIMESTAMP: 1234   ";
    segment.validateArgs();
    assertTrue(
        new ReflectionEquals(OffsetsInitializer.timestamp(1234))
            .matches(segment.getEndAtOffsetsInitializer()));
  }

  @Disabled("Temporarily disabling test while we roll back the idleness implementation")
  @Test
  void testGeneratingWatermarks() {
    WatermarkStrategy<Integer> strategy =
        segment.getWatermarkStrategy("", Duration.ofMinutes(1), (o -> o + 10L));
    assertEquals(20, strategy.createTimestampAssigner(null).extractTimestamp(10, 0));
    assertEquals(
        "org.apache.flink.api.common.eventtime.WatermarkStrategyWithTimestampAssigner",
        strategy.getClass().getName());

    segment.watermarkIdleness = Duration.ofSeconds(100);
    assertEquals(
        "org.apache.flink.api.common.eventtime.WatermarkStrategyWithIdleness",
        segment
            .<Integer>getWatermarkStrategy("A", Duration.ofMinutes(1), null)
            .getClass()
            .getName());

    segment.watermarkIdleness = null;
    segment.watermarkIdlenessForTopic.put("A", Duration.ofSeconds(10));
    assertEquals(
        "org.apache.flink.api.common.eventtime.WatermarkStrategyWithIdleness",
        segment
            .<Integer>getWatermarkStrategy("A", Duration.ofMinutes(1), null)
            .getClass()
            .getName());

    segment.watermarkAlignment = true;
    assertEquals(
        "org.apache.flink.api.common.eventtime.WatermarksWithWatermarkAlignment",
        segment
            .<Integer>getWatermarkStrategy("", Duration.ofMinutes(1), null)
            .getClass()
            .getName());
  }
}
