package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import static com.google.common.truth.Truth.assertThat;

import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.LogRequest;
import ai.promoted.proto.event.Session;
import org.junit.jupiter.api.Test;

public class PushDownAndFlatMapSessionTest extends BasePushDownAndFlatMapTest<Session> {

  @Test
  public void pushAll() {
    new PushDownAndFlatMapSession()
        .flatMap(
            setBaseFields(LogRequest.newBuilder())
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .setClientInfo(CLIENT_INFO)
                .addSession(Session.newBuilder())
                .build(),
            collector::add);
    assertThat(collector)
        .containsExactly(
            Session.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .setClientInfo(CLIENT_INFO)
                .build());
  }

  @Test
  public void pushNone() {
    new PushDownAndFlatMapSession()
        .flatMap(
            setBaseFields(LogRequest.newBuilder())
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .addSession(
                    Session.newBuilder()
                        .setPlatformId(PLATFORM_ID)
                        .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                        .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                .build(),
            collector::add);
    assertThat(collector)
        .containsExactly(
            Session.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .build());
  }

  @Test
  public void pushDifferent() {
    new PushDownAndFlatMapSession()
        .flatMap(
            setBaseFields(LogRequest.newBuilder())
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .addSession(
                    Session.newBuilder()
                        .setPlatformId(OTHER_PLATFORM_ID)
                        .setUserInfo(UserInfo.newBuilder().setLogUserId(OTHER_LOG_USER_ID))
                        .setTiming(createTiming(OTHER_TIME_EPOCH_MILLIS)))
                .build(),
            collector::add);
    assertThat(collector)
        .containsExactly(
            Session.newBuilder()
                .setPlatformId(OTHER_PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(OTHER_LOG_USER_ID))
                .setTiming(createTiming(OTHER_TIME_EPOCH_MILLIS))
                .build());
  }

  @Test
  public void lowerCaseLogSessionId() {
    new PushDownAndFlatMapSession()
        .flatMap(
            LogRequest.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .addSession(
                    Session.newBuilder()
                        .setPlatformId(PLATFORM_ID)
                        .setUserInfo(UserInfo.newBuilder().setLogUserId(UPPERCASE_LOG_USER_ID))
                        .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                .build(),
            collector::add);
    assertThat(collector)
        .containsExactly(
            Session.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .build());
  }

  @Test
  public void ifNoTopLevelUserInfo() {
    new PushDownAndFlatMapSession()
        .flatMap(
            LogRequest.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .addSession(
                    Session.newBuilder()
                        .setPlatformId(PLATFORM_ID)
                        .setUserInfo(UserInfo.newBuilder().setLogUserId(UPPERCASE_LOG_USER_ID))
                        .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                .build(),
            collector::add);
    assertThat(collector)
        .containsExactly(
            Session.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .build());
  }
}
