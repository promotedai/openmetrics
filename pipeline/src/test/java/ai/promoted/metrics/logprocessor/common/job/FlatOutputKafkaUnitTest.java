package ai.promoted.metrics.logprocessor.common.job;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests (non-minicluster tests). */
public class FlatOutputKafkaUnitTest {

  FlatOutputKafka segment;

  @BeforeEach
  public void setUp() {
    segment = new FlatOutputKafka(new KafkaSegment(null));
  }

  @Test
  public void getJoinedEventTopic() {
    assertEquals("metrics.default.joined-event", segment.getJoinedEventTopic(""));
    assertEquals("metrics.blue.default.joined-event", segment.getJoinedEventTopic("blue"));
  }

  @Test
  public void getFlatResponseInsertionTopic() {
    assertEquals(
        "metrics.default.flat-response-insertion", segment.getFlatResponseInsertionTopic(""));
    assertEquals(
        "metrics.blue.default.flat-response-insertion",
        segment.getFlatResponseInsertionTopic("blue"));
  }
}
