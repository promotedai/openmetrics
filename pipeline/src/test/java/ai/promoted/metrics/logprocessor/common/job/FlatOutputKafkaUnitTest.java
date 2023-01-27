package ai.promoted.metrics.logprocessor.common.job;

import ai.promoted.metrics.logprocessor.common.job.FlatOutputKafka;
import ai.promoted.metrics.logprocessor.common.job.KafkaSegment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests (non-minicluster tests).
 */
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
  public void getFlatUserEventTopic() {
    assertEquals("metrics.default.joined-user-event", segment.getJoinedUserEventTopic(""));
    assertEquals("metrics.blue.default.joined-user-event", segment.getJoinedUserEventTopic("blue"));
  }

  @Test
  public void getFlatResponseInsertionTopic() {
    assertEquals("metrics.default.flat-response-insertion", segment.getFlatResponseInsertionTopic(""));
    assertEquals("metrics.blue.default.flat-response-insertion", segment.getFlatResponseInsertionTopic("blue"));
  }

  @Test
  public void getFlatUserResponseInsertionTopic() {
    assertEquals("metrics.default.flat-user-response-insertion", segment.getFlatUserResponseInsertionTopic(""));
    assertEquals("metrics.blue.default.flat-user-response-insertion", segment.getFlatUserResponseInsertionTopic("blue"));
  }
}
