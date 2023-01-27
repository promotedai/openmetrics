package ai.promoted.metrics.logprocessor.job.contentmetrics;

import ai.promoted.proto.common.Timing;
import ai.promoted.proto.event.Impression;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class impressionTableTest {

  @Test
  public void toImpressionRow() {
    LocalDateTime eventApiTimestamp = LocalDateTime.of(2022, 11, 6, 13, 40, 10, 4000000);
    Impression impression = Impression.newBuilder()
            .setPlatformId(1)
            .setTiming(Timing.newBuilder()
                    .setEventApiTimestamp(eventApiTimestamp.toInstant(ZoneOffset.UTC).toEpochMilli()))
            .setImpressionId("imp1")
            .setInsertionId("ins1")
            .setContentId("cont1")
            .build();
    assertEquals(
            Row.of(1L, eventApiTimestamp, "imp1", "ins1", "cont1"),
            ImpressionTable.toImpressionRow(impression));
  }
}
