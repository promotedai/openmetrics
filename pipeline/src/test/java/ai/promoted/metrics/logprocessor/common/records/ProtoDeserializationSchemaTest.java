package ai.promoted.metrics.logprocessor.common.records;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.LogRequest;
import java.io.IOException;
import java.util.Base64;
import org.junit.jupiter.api.Test;

public class ProtoDeserializationSchemaTest {

  @Test
  public void serDe() throws Exception {
    ProtoDeserializationSchema<LogRequest> schema =
        new ProtoDeserializationSchema(LogRequest::parseFrom);
    LogRequest request =
        LogRequest.newBuilder()
            .setPlatformId(1)
            .setUserInfo(UserInfo.newBuilder().setUserId("userId").build())
            .build();
    LogRequest serDeRequest = schema.deserialize(schema.serialize(request));
    assertEquals(request, serDeRequest);
    assertNotSame(request, serDeRequest);
  }

  @Test
  public void deserialize_badProto() throws Exception {
    ProtoDeserializationSchema<LogRequest> schema =
        new ProtoDeserializationSchema(LogRequest::parseFrom);
    assertThrows(
        IOException.class,
        () -> {
          schema.deserialize(Base64.getDecoder().decode("aaaa"));
        });
  }
}
