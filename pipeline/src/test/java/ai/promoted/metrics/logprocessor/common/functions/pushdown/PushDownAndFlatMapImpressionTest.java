package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.LogRequest;
import ai.promoted.proto.event.Impression;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;

public class PushDownAndFlatMapImpressionTest extends BasePushDownAndFlatMapTest<Impression> {

  @Test
  public void pushAll() {
    new PushDownAndFlatMapImpression().flatMap(
            setBaseFields(LogRequest.newBuilder())
                    .setUserInfo(UserInfo.newBuilder()
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .setClientInfo(CLIENT_INFO)
                    .addImpression(Impression.newBuilder())
                    .build(),
            collector::add);
    assertThat(collector).containsExactly(
            Impression.newBuilder()
                    .setPlatformId(PLATFORM_ID)
                    .setUserInfo(UserInfo.newBuilder()
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .setTiming(createTiming(TIME_EPOCH_MILLIS))
                    .setClientInfo(CLIENT_INFO)
                    .build());
  }

  @Test
  public void pushNone() {
    new PushDownAndFlatMapImpression().flatMap(
            setBaseFields(LogRequest.newBuilder())
                    .setUserInfo(UserInfo.newBuilder()
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .addImpression(Impression.newBuilder()
                            .setPlatformId(PLATFORM_ID)
                            .setUserInfo(UserInfo.newBuilder()
                                    .setLogUserId(LOWERCASE_LOG_USER_ID))
                            .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                    .build(),
            collector::add);
    assertThat(collector).containsExactly(
            Impression.newBuilder()
                    .setPlatformId(PLATFORM_ID)
                    .setUserInfo(UserInfo.newBuilder()
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .setTiming(createTiming(TIME_EPOCH_MILLIS))
                    .build());
  }

  @Test
  public void pushDifferent() {
    new PushDownAndFlatMapImpression().flatMap(
            setBaseFields(LogRequest.newBuilder())
                    .setUserInfo(UserInfo.newBuilder()
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .addImpression(Impression.newBuilder()
                            .setPlatformId(OTHER_PLATFORM_ID)
                            .setUserInfo(UserInfo.newBuilder()
                                    .setLogUserId(OTHER_LOG_USER_ID))
                            .setTiming(createTiming(OTHER_TIME_EPOCH_MILLIS)))
                    .build(),
            collector::add);
    assertThat(collector).containsExactly(
            Impression.newBuilder()
                    .setPlatformId(OTHER_PLATFORM_ID)
                    .setUserInfo(UserInfo.newBuilder()
                            .setLogUserId(OTHER_LOG_USER_ID))
                    .setTiming(createTiming(OTHER_TIME_EPOCH_MILLIS))
                    .build());
  }


  @Test
  public void lowerCaseLogImpressionId() {
    new PushDownAndFlatMapImpression().flatMap(
            LogRequest.newBuilder()
                    .setPlatformId(PLATFORM_ID)
                    .setUserInfo(UserInfo.newBuilder()
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .setTiming(createTiming(TIME_EPOCH_MILLIS))
                    .addImpression(Impression.newBuilder()
                            .setPlatformId(PLATFORM_ID)
                            .setUserInfo(UserInfo.newBuilder()
                                    .setLogUserId(UPPERCASE_LOG_USER_ID))
                            .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                    .build(),
            collector::add);
    assertThat(collector).containsExactly(
            Impression.newBuilder()
                    .setPlatformId(PLATFORM_ID)
                    .setUserInfo(UserInfo.newBuilder()
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .setTiming(createTiming(TIME_EPOCH_MILLIS))
                    .build());
  }

  @Test
  public void ifNoTopLevelUserInfo() {
    new PushDownAndFlatMapImpression().flatMap(
            LogRequest.newBuilder()
                    .setPlatformId(PLATFORM_ID)
                    .setTiming(createTiming(TIME_EPOCH_MILLIS))
                    .addImpression(Impression.newBuilder()
                            .setPlatformId(PLATFORM_ID)
                            .setUserInfo(UserInfo.newBuilder()
                                    .setLogUserId(UPPERCASE_LOG_USER_ID))
                            .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                    .build(),
            collector::add);
    assertThat(collector).containsExactly(
            Impression.newBuilder()
                    .setPlatformId(PLATFORM_ID)
                    .setUserInfo(UserInfo.newBuilder()
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .setTiming(createTiming(TIME_EPOCH_MILLIS))
                    .build());
  }
}