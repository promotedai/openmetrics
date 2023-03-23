package ai.promoted.metrics.logprocessor.common.records;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.promoted.proto.common.Timing;
import org.junit.jupiter.api.Test;

public class ParseFromProtoDeserializerTest {

  private Timing createTiming(long timestamp) {
    return Timing.newBuilder().setClientLogTimestamp(timestamp).build();
  }

  @Test
  public void parseFrom() {
    assertEquals(
        createTiming(1000),
        new ParseFromProtoDeserializer(Timing::parseFrom)
            .deserialize(createTiming(1000).toByteArray()));
  }
}
