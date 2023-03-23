package ai.promoted.metrics.logprocessor.common.records;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.LogRequest;
import org.junit.jupiter.api.Test;

public class PlatformLogUserKeySerializationSchemaTest {

  // The test serializes an arbitrary but different proto message (LogRequest).
  // We don't expect this proto to be used this way.
  @Test
  public void serialize() throws Exception {
    PlatformLogUserKeySerializationSchema<LogRequest> schema =
        new PlatformLogUserKeySerializationSchema<>(
            LogRequest::getPlatformId, (logRequest) -> logRequest.getUserInfo().getLogUserId());
    LogRequest request =
        LogRequest.newBuilder()
            .setPlatformId(1)
            .setUserInfo(UserInfo.newBuilder().setLogUserId("userId").build())
            .build();
    final long millis = 123456789;
    byte[] actualKey = schema.serialize(request);
    byte[] expectedKey = new byte[] {1, 0, 0, 0, 0, 0, 0, 0, 117, 115, 101, 114, 73, 100};
    assertEquals(expectedKey.length, actualKey.length);
    for (int i = 0; i < expectedKey.length; i++) {
      assertEquals(expectedKey[i], actualKey[i]);
    }
  }
}
