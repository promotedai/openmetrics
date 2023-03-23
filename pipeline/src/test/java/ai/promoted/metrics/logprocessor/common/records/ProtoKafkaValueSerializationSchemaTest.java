package ai.promoted.metrics.logprocessor.common.records;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.LogRequest;
import org.junit.jupiter.api.Test;

public class ProtoKafkaValueSerializationSchemaTest {

  // The test serializes an arbitrary but different proto message (LogRequest).
  // We don't expect this proto to be used this way.
  @Test
  public void serialize() throws Exception {
    ProtoKafkaValueSerializationSchema<LogRequest> schema =
        new ProtoKafkaValueSerializationSchema();
    LogRequest request =
        LogRequest.newBuilder()
            .setPlatformId(1)
            .setUserInfo(UserInfo.newBuilder().setLogUserId("userId").build())
            .build();
    byte[] bytes = schema.serialize(request);
    LogRequest serDeRequest = LogRequest.parseFrom(bytes);
    assertEquals(request, serDeRequest);
    assertNotSame(request, serDeRequest);
  }
}
