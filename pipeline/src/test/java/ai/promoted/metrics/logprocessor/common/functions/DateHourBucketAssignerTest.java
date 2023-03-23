package ai.promoted.metrics.logprocessor.common.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.promoted.proto.common.Timing;
import ai.promoted.proto.event.Impression;
import org.junit.jupiter.api.Test;

public class DateHourBucketAssignerTest {

  @Test
  public void simpleCase() throws Exception {
    Impression message =
        Impression.newBuilder()
            .setTiming(Timing.newBuilder().setEventApiTimestamp(1612815980000L))
            .build();
    assertEquals(
        "/dt=2021-02-08/hour=20",
        new DateHourBucketAssigner<Impression>(
                (impression) -> impression.getTiming().getEventApiTimestamp())
            .getBucketId(message, null));
  }
}
