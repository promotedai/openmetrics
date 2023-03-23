package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import static com.google.common.truth.Truth.assertThat;

import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.LogRequest;
import ai.promoted.proto.event.SessionProfile;
import org.junit.jupiter.api.Test;

public class PushDownAndFlatMapSessionProfileTest
    extends BasePushDownAndFlatMapTest<SessionProfile> {

  @Test
  public void pushAll() {
    new PushDownAndFlatMapSessionProfile()
        .flatMap(
            setBaseFields(LogRequest.newBuilder())
                .setUserInfo(UserInfo.newBuilder().setUserId(USER_ID))
                .setClientInfo(CLIENT_INFO)
                .addSessionProfile(SessionProfile.newBuilder())
                .build(),
            collector::add);
    assertThat(collector)
        .containsExactly(
            SessionProfile.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setUserId(USER_ID))
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .setClientInfo(CLIENT_INFO)
                .build());
  }

  @Test
  public void pushNone() {
    new PushDownAndFlatMapSessionProfile()
        .flatMap(
            setBaseFields(LogRequest.newBuilder())
                .setUserInfo(UserInfo.newBuilder().setUserId(USER_ID))
                .addSessionProfile(
                    SessionProfile.newBuilder()
                        .setPlatformId(PLATFORM_ID)
                        .setUserInfo(UserInfo.newBuilder().setUserId(USER_ID))
                        .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                .build(),
            collector::add);
    assertThat(collector)
        .containsExactly(
            SessionProfile.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setUserId(USER_ID))
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .build());
  }

  @Test
  public void pushDifferent() {
    new PushDownAndFlatMapSessionProfile()
        .flatMap(
            setBaseFields(LogRequest.newBuilder())
                .setUserInfo(UserInfo.newBuilder().setUserId(USER_ID))
                .addSessionProfile(
                    SessionProfile.newBuilder()
                        .setPlatformId(OTHER_PLATFORM_ID)
                        .setUserInfo(UserInfo.newBuilder().setUserId(OTHER_USER_ID))
                        .setTiming(createTiming(OTHER_TIME_EPOCH_MILLIS)))
                .build(),
            collector::add);
    assertThat(collector)
        .containsExactly(
            SessionProfile.newBuilder()
                .setPlatformId(OTHER_PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setUserId(OTHER_USER_ID))
                .setTiming(createTiming(OTHER_TIME_EPOCH_MILLIS))
                .build());
  }

  @Test
  public void lowerCaseLogUserId() {
    new PushDownAndFlatMapSessionProfile()
        .flatMap(
            LogRequest.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setUserId(USER_ID))
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .addSessionProfile(
                    SessionProfile.newBuilder()
                        .setPlatformId(PLATFORM_ID)
                        .setUserInfo(UserInfo.newBuilder().setUserId(USER_ID))
                        .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                .build(),
            collector::add);
    assertThat(collector)
        .containsExactly(
            SessionProfile.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setUserId(USER_ID))
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .build());
  }

  @Test
  public void ifNoTopLevelUserInfo() {
    new PushDownAndFlatMapSessionProfile()
        .flatMap(
            LogRequest.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .addSessionProfile(
                    SessionProfile.newBuilder()
                        .setPlatformId(PLATFORM_ID)
                        .setUserInfo(UserInfo.newBuilder().setUserId(USER_ID))
                        .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                .build(),
            collector::add);
    assertThat(collector)
        .containsExactly(
            SessionProfile.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setUserId(USER_ID))
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .build());
  }
}
