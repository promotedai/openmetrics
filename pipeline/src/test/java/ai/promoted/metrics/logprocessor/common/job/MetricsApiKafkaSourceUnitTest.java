package ai.promoted.metrics.logprocessor.common.job;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.promoted.proto.event.LogRequest;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests (non-minicluster tests). */
public class MetricsApiKafkaSourceUnitTest {

  MetricsApiKafkaSource segment;

  @BeforeEach
  public void setUp() {
    segment = new MetricsApiKafkaSource(null, new KafkaSegment(), null);
  }

  @Test
  public void getInputLogRequestTopic() {
    assertEquals("tracking.event.log-request", segment.getInputLogRequestTopic());
  }

  // This isn't an exact test.  It's just made to be easier to update.
  @Test
  public void catchNewLogRequestTimingFields() {
    // If you need to update this test, check MetricsApiKafkaSource.getTiming
    assertEquals(
        17,
        LogRequest.getDescriptor().getFields().stream()
            .map(field -> field.getNumber())
            .collect(Collectors.toSet())
            .size());
  }
}
