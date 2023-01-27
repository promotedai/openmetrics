package ai.promoted.metrics.logprocessor.common.records;

import ai.promoted.metrics.logprocessor.common.records.ParseFromProtoDeserializer;
import ai.promoted.proto.common.Timing;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ParseFromProtoDeserializerTest {

  private Timing createTiming(long timestamp) {
    return Timing.newBuilder()
            .setClientLogTimestamp(timestamp)
            .build();
  }

  @Test
  public void parseFrom() {
    assertEquals(
            createTiming(1000),
            new ParseFromProtoDeserializer(Timing::parseFrom).deserialize(
                    createTiming(1000).toByteArray()));
  }
}
