package ai.promoted.metrics.logprocessor.job.validateenrich;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import ai.promoted.metrics.logprocessor.common.util.TrackingUtil;
import ai.promoted.proto.common.AnonUserRetainedUser;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.EnrichmentUnion;
import ai.promoted.proto.event.Impression;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ToAnonUserRetainedUserTest {

  private static final Impression IMPRESSION =
      newImpression(
          UserInfo.newBuilder().setAnonUserId("anonUserId4").setRetainedUserId("retainedUserId4"));

  private static final Impression newImpression(UserInfo.Builder userInfoBuilder) {
    return Impression.newBuilder()
        .setPlatformId(4L)
        .setTiming(Timing.newBuilder().setEventApiTimestamp(4))
        .setUserInfo(userInfoBuilder)
        .build();
  }

  protected ToAnonUserRetainedUser operator;
  protected ProcessFunction<EnrichmentUnion, AnonUserRetainedUser>.Context mockContext;
  protected Collector<AnonUserRetainedUser> mockOut;

  @BeforeEach
  public void setUp() {
    operator = new ToAnonUserRetainedUser();
    mockOut = Mockito.mock(Collector.class);
    mockContext = Mockito.mock(ProcessFunction.Context.class);
    TrackingUtil.processingTimeSupplier = () -> 0L;
  }

  @Test
  public void canMap() {
    UserInfo.Builder userInfoBuilder = UserInfo.newBuilder();
    assertFalse(canMap(userInfoBuilder.clone()));
    userInfoBuilder.setAnonUserId("anonUserId1");
    assertFalse(canMap(userInfoBuilder.clone()));
    userInfoBuilder.setRetainedUserId("retainedUserId1");
    assertTrue(canMap(userInfoBuilder.clone()));
    userInfoBuilder.clearAnonUserId();
    assertFalse(canMap(userInfoBuilder.clone()));
  }

  @Test
  public void processElement() throws Exception {
    when(mockContext.timestamp()).thenReturn(123L);
    operator.processElement(
        EnrichmentUnionUtil.toImpressionUnion(IMPRESSION), mockContext, mockOut);
    verify(mockOut)
        .collect(
            AnonUserRetainedUser.newBuilder()
                .setPlatformId(4L)
                .setEventTimeMillis(123L)
                .setAnonUserId("anonUserId4")
                .setRetainedUserId("retainedUserId4")
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
    return ToAnonUserRetainedUser.canMap(userInfoBuilder.build());
  }
}
