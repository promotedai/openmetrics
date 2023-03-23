package ai.promoted.metrics.logprocessor.common.job;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests (non-minicluster tests). */
public class MetricsApiKafkaSourceUnitTest {

  MetricsApiKafkaSource segment;

  @BeforeEach
  public void setUp() {
    segment = new MetricsApiKafkaSource(null, new KafkaSegment(null));
  }

  @Test
  public void getInputLogRequestTopic() {
    assertEquals("tracking.event.log-request", segment.getInputLogRequestTopic());
  }
}
