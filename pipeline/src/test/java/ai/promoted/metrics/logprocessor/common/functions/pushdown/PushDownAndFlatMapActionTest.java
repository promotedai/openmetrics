package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import static com.google.common.truth.Truth.assertThat;

import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.LogRequest;
import org.junit.jupiter.api.Test;

public class PushDownAndFlatMapActionTest extends BasePushDownAndFlatMapTest<Action> {

  @Test
  public void pushAll() {
    new PushDownAndFlatMapAction()
        .flatMap(
            setBaseFields(LogRequest.newBuilder())
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .setClientInfo(CLIENT_INFO)
                .addAction(Action.newBuilder())
                .build(),
            collector::add);
    assertThat(collector)
        .containsExactly(
            Action.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .setClientInfo(CLIENT_INFO)
                .build());
  }

  @Test
  public void pushNone() {
    new PushDownAndFlatMapAction()
        .flatMap(
            setBaseFields(LogRequest.newBuilder())
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .addAction(
                    Action.newBuilder()
                        .setPlatformId(PLATFORM_ID)
                        .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                        .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                .build(),
            collector::add);
    assertThat(collector)
        .containsExactly(
            Action.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .build());
  }

  @Test
  public void pushDifferent() {
    new PushDownAndFlatMapAction()
        .flatMap(
            setBaseFields(LogRequest.newBuilder())
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .addAction(
                    Action.newBuilder()
                        .setPlatformId(OTHER_PLATFORM_ID)
                        .setUserInfo(UserInfo.newBuilder().setLogUserId(OTHER_LOG_USER_ID))
                        .setTiming(createTiming(OTHER_TIME_EPOCH_MILLIS)))
                .build(),
            collector::add);
    assertThat(collector)
        .containsExactly(
            Action.newBuilder()
                .setPlatformId(OTHER_PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(OTHER_LOG_USER_ID))
                .setTiming(createTiming(OTHER_TIME_EPOCH_MILLIS))
                .build());
  }

  @Test
  public void lowerCaseLogActionId() {
    new PushDownAndFlatMapAction()
        .flatMap(
            LogRequest.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .addAction(
                    Action.newBuilder()
                        .setPlatformId(PLATFORM_ID)
                        .setUserInfo(UserInfo.newBuilder().setLogUserId(UPPERCASE_LOG_USER_ID))
                        .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                .build(),
            collector::add);
    assertThat(collector)
        .containsExactly(
            Action.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .build());
  }

  @Test
  public void ifNoTopLevelUserInfo() {
    new PushDownAndFlatMapAction()
        .flatMap(
            LogRequest.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .addAction(
                    Action.newBuilder()
                        .setPlatformId(PLATFORM_ID)
                        .setUserInfo(UserInfo.newBuilder().setLogUserId(UPPERCASE_LOG_USER_ID))
                        .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                .build(),
            collector::add);
    assertThat(collector)
        .containsExactly(
            Action.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .build());
  }
}
