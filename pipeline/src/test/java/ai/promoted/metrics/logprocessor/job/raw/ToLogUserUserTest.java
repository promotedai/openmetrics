package ai.promoted.metrics.logprocessor.job.raw;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import ai.promoted.metrics.common.LogUserUser;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.LogRequest;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ToLogUserUserTest {
  protected Collector<LogUserUser> mockOut;
  protected ToLogUserUser operator;

  @BeforeEach
  public void setUp() {
    mockOut = Mockito.mock(Collector.class);
    operator = new ToLogUserUser();
  }

  // Just doing some of the flatMap paths.

  @Test
  public void flatMap_deliveryLog() throws Exception {
    operator.flatMap(
        LogRequest.newBuilder()
            .setPlatformId(1L)
            .setTiming(Timing.newBuilder().setEventApiTimestamp(1234))
            .addDeliveryLog(
                DeliveryLog.newBuilder()
                    .setPlatformId(2L)
                    .setRequest(
                        Request.newBuilder()
                            .setPlatformId(2L)
                            .setTiming(Timing.newBuilder().setEventApiTimestamp(1235))
                            .setUserInfo(
                                UserInfo.newBuilder()
                                    .setLogUserId("logUserId1")
                                    .setUserId("userId1"))))
            .build(),
        mockOut);
    verify(mockOut)
        .collect(
            LogUserUser.newBuilder()
                .setPlatformId(2L)
                .setEventApiTimestamp(1235)
                .setLogUserId("logUserId1")
                .setUserId("userId1")
                .build());
  }

  @Test
  public void flatMap_deliveryLog_noLog() throws Exception {
    operator.flatMap(
        LogRequest.newBuilder()
            .setPlatformId(1L)
            .addDeliveryLog(
                DeliveryLog.newBuilder()
                    .setPlatformId(1L)
                    .setRequest(
                        Request.newBuilder()
                            .setPlatformId(1L)
                            .setTiming(Timing.newBuilder().setEventApiTimestamp(1234))
                            .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1"))))
            .build(),
        mockOut);
    verifyNoInteractions(mockOut);
  }

  @Test
  public void flatMap_impression() throws Exception {
    operator.flatMap(
        LogRequest.newBuilder()
            .setPlatformId(1L)
            .setTiming(Timing.newBuilder().setEventApiTimestamp(1234))
            .addImpression(
                Impression.newBuilder()
                    .setPlatformId(2L)
                    .setTiming(Timing.newBuilder().setEventApiTimestamp(1235))
                    .setUserInfo(
                        UserInfo.newBuilder().setLogUserId("logUserId1").setUserId("userId1")))
            .build(),
        mockOut);
    verify(mockOut)
        .collect(
            LogUserUser.newBuilder()
                .setPlatformId(2L)
                .setEventApiTimestamp(1235)
                .setLogUserId("logUserId1")
                .setUserId("userId1")
                .build());
  }

  @Test
  public void flatMap_impression_noLog() throws Exception {
    operator.flatMap(
        LogRequest.newBuilder()
            .setPlatformId(1L)
            .setTiming(Timing.newBuilder().setEventApiTimestamp(1234))
            .addImpression(
                Impression.newBuilder()
                    .setPlatformId(2L)
                    .setTiming(Timing.newBuilder().setEventApiTimestamp(1235))
                    .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1")))
            .build(),
        mockOut);
    verifyNoInteractions(mockOut);
  }
}
