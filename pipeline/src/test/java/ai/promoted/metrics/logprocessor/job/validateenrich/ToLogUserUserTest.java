package ai.promoted.metrics.logprocessor.job.validateenrich;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import ai.promoted.metrics.common.LogUserUser;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.EnrichmentUnion;
import ai.promoted.proto.event.Impression;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ToLogUserUserTest {

  private static final Impression IMPRESSION =
      newImpression(UserInfo.newBuilder().setLogUserId("anonUserId4").setUserId("userId4"));

  private static final Impression newImpression(UserInfo.Builder userInfoBuilder) {
    return Impression.newBuilder()
        .setPlatformId(4L)
        .setTiming(Timing.newBuilder().setEventApiTimestamp(4))
        .setUserInfo(userInfoBuilder)
        .build();
  }

  protected ToLogUserUser operator;
  protected ProcessFunction<EnrichmentUnion, LogUserUser>.Context mockContext;
  protected Collector<LogUserUser> mockOut;

  @BeforeEach
  public void setUp() {
    operator = new ToLogUserUser();
    mockOut = Mockito.mock(Collector.class);
    mockContext = Mockito.mock(ProcessFunction.Context.class);
  }

  @Test
  public void canMap() {
    UserInfo.Builder userInfoBuilder = UserInfo.newBuilder();
    assertFalse(canMap(userInfoBuilder.clone()));
    userInfoBuilder.setLogUserId("logUserId1");
    assertFalse(canMap(userInfoBuilder.clone()));
    userInfoBuilder.setUserId("userId1");
    assertTrue(canMap(userInfoBuilder.clone()));
    userInfoBuilder.clearLogUserId();
    assertFalse(canMap(userInfoBuilder.clone()));
  }

  @Test
  public void processElement() throws Exception {
    when(mockContext.timestamp()).thenReturn(123L);
    operator.processElement(
        EnrichmentUnionUtil.toImpressionUnion(IMPRESSION), mockContext, mockOut);
    verify(mockOut)
        .collect(
            LogUserUser.newBuilder()
                .setPlatformId(4L)
                .setEventTimeMillis(123L)
                .setLogUserId("anonUserId4")
                .setUserId("userId4")
                .build());
  }

  @Test
  public void processElement_noOutput() throws Exception {
    when(mockContext.timestamp()).thenReturn(123L);
    operator.processElement(
        EnrichmentUnionUtil.toImpressionUnion(newImpression(UserInfo.newBuilder())),
        mockContext,
        mockOut);
    verifyNoInteractions(mockOut);
  }

  private static boolean canMap(UserInfo.Builder userInfoBuilder) {
    return ToLogUserUser.canMap(userInfoBuilder.build());
  }
}
