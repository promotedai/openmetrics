package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.event.LogRequest;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;

public class PushDownAndFlatMapDeliveryLogTest extends BasePushDownAndFlatMapTest<DeliveryLog> {

  @Test
  public void pushAll() {
    new PushDownAndFlatMapDeliveryLog().flatMap(
            setBaseFields(LogRequest.newBuilder())
                    .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                    .setClientInfo(CLIENT_INFO)
                    .addDeliveryLog(DeliveryLog.newBuilder())
                    .build(),
            collector::add);
    assertThat(collector).containsExactly(
            DeliveryLog.newBuilder()
                    .setPlatformId(PLATFORM_ID)
                    .setRequest(Request.newBuilder()
                            .setPlatformId(PLATFORM_ID)
                            .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                            .setTiming(createTiming(TIME_EPOCH_MILLIS))
                            .setClientInfo(CLIENT_INFO))
                    .build());
  }

  @Test
  public void pushNone() {
    new PushDownAndFlatMapDeliveryLog().flatMap(
            setBaseFields(LogRequest.newBuilder())
                    .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                    .addDeliveryLog(DeliveryLog.newBuilder()
                            .setPlatformId(PLATFORM_ID)
                            .setRequest(Request.newBuilder()
                                    .setPlatformId(PLATFORM_ID)
                                    .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                                    .setTiming(createTiming(TIME_EPOCH_MILLIS))))
                    .build(),
            collector::add);
    assertThat(collector).containsExactly(
            DeliveryLog.newBuilder()
                    .setPlatformId(PLATFORM_ID)
                    .setRequest(Request.newBuilder()
                            .setPlatformId(PLATFORM_ID)
                            .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                            .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                    .build());
  }

  @Test
  public void pushDifferent() {
    new PushDownAndFlatMapDeliveryLog().flatMap(
            setBaseFields(LogRequest.newBuilder())
                    .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                    .addDeliveryLog(DeliveryLog.newBuilder()
                            .setPlatformId(OTHER_PLATFORM_ID)
                            .setRequest(Request.newBuilder()
                                    .setUserInfo(UserInfo.newBuilder().setLogUserId(OTHER_LOG_USER_ID))
                                    .setTiming(createTiming(OTHER_TIME_EPOCH_MILLIS))))
                    .build(),
            collector::add);
    assertThat(collector).containsExactly(
            DeliveryLog.newBuilder()
                    .setPlatformId(OTHER_PLATFORM_ID)
                    .setRequest(Request.newBuilder()
                            .setPlatformId(OTHER_PLATFORM_ID)
                            .setUserInfo(UserInfo.newBuilder().setLogUserId(OTHER_LOG_USER_ID))
                            .setTiming(createTiming(OTHER_TIME_EPOCH_MILLIS)))
                    .build());
  }

  @Test
  public void lowerCaseLogDeliveryLogId() {
    new PushDownAndFlatMapDeliveryLog().flatMap(
            LogRequest.newBuilder()
                    .setPlatformId(PLATFORM_ID)
                    .setUserInfo(UserInfo.newBuilder()
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .setTiming(createTiming(TIME_EPOCH_MILLIS))
                    .addDeliveryLog(DeliveryLog.newBuilder()
                            .setPlatformId(OTHER_PLATFORM_ID)
                            .setRequest(Request.newBuilder()
                                    .setUserInfo(UserInfo.newBuilder().setLogUserId(UPPERCASE_LOG_USER_ID))
                                    .setTiming(createTiming(TIME_EPOCH_MILLIS))))
                    .build(),
            collector::add);
    assertThat(collector).containsExactly(
            DeliveryLog.newBuilder()
                    .setPlatformId(OTHER_PLATFORM_ID)
                    .setRequest(Request.newBuilder()
                            .setPlatformId(OTHER_PLATFORM_ID)
                            .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                            .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                    .build());
  }

  @Test
  public void ifNoTopLevelUserInfo() {
    new PushDownAndFlatMapDeliveryLog().flatMap(
            LogRequest.newBuilder()
                    .setPlatformId(PLATFORM_ID)
                    .setTiming(createTiming(TIME_EPOCH_MILLIS))
                    .addDeliveryLog(DeliveryLog.newBuilder()
                            .setPlatformId(OTHER_PLATFORM_ID)
                            .setRequest(Request.newBuilder()
                                    .setUserInfo(UserInfo.newBuilder().setLogUserId(UPPERCASE_LOG_USER_ID))
                                    .setTiming(createTiming(TIME_EPOCH_MILLIS))))
                    .build(),
            collector::add);
    assertThat(collector).containsExactly(
            DeliveryLog.newBuilder()
                    .setPlatformId(OTHER_PLATFORM_ID)
                    .setRequest(Request.newBuilder()
                            .setPlatformId(OTHER_PLATFORM_ID)
                            .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                            .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                    .build());
  }
}