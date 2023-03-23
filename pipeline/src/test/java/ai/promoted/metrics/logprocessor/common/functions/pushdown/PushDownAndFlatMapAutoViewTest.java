package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import static com.google.common.truth.Truth.assertThat;

import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.AutoView;
import ai.promoted.proto.event.LogRequest;
import org.junit.jupiter.api.Test;

public class PushDownAndFlatMapAutoViewTest extends BasePushDownAndFlatMapTest<AutoView> {

  @Test
  public void pushAll() {
    new PushDownAndFlatMapAutoView()
        .flatMap(
            setBaseFields(LogRequest.newBuilder())
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .setClientInfo(CLIENT_INFO)
                .addAutoView(AutoView.newBuilder())
                .build(),
            collector::add);
    assertThat(collector)
        .containsExactly(
            AutoView.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .setClientInfo(CLIENT_INFO)
                .build());
  }

  @Test
  public void pushNone() {
    new PushDownAndFlatMapAutoView()
        .flatMap(
            setBaseFields(LogRequest.newBuilder())
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .addAutoView(
                    AutoView.newBuilder()
                        .setPlatformId(PLATFORM_ID)
                        .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                        .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                .build(),
            collector::add);
    assertThat(collector)
        .containsExactly(
            AutoView.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .build());
  }

  @Test
  public void pushDifferent() {
    new PushDownAndFlatMapAutoView()
        .flatMap(
            setBaseFields(LogRequest.newBuilder())
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .addAutoView(
                    AutoView.newBuilder()
                        .setPlatformId(OTHER_PLATFORM_ID)
                        .setUserInfo(UserInfo.newBuilder().setLogUserId(OTHER_LOG_USER_ID))
                        .setTiming(createTiming(OTHER_TIME_EPOCH_MILLIS)))
                .build(),
            collector::add);
    assertThat(collector)
        .containsExactly(
            AutoView.newBuilder()
                .setPlatformId(OTHER_PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(OTHER_LOG_USER_ID))
                .setTiming(createTiming(OTHER_TIME_EPOCH_MILLIS))
                .build());
  }

  @Test
  public void lowerCaseLogAutoViewId() {
    new PushDownAndFlatMapAutoView()
        .flatMap(
            LogRequest.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .addAutoView(
                    AutoView.newBuilder()
                        .setPlatformId(PLATFORM_ID)
                        .setUserInfo(UserInfo.newBuilder().setLogUserId(UPPERCASE_LOG_USER_ID))
                        .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                .build(),
            collector::add);
    assertThat(collector)
        .containsExactly(
            AutoView.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .build());
  }

  @Test
  public void ifNoTopLevelUserInfo() {
    new PushDownAndFlatMapAutoView()
        .flatMap(
            LogRequest.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .addAutoView(
                    AutoView.newBuilder()
                        .setPlatformId(PLATFORM_ID)
                        .setUserInfo(UserInfo.newBuilder().setLogUserId(UPPERCASE_LOG_USER_ID))
                        .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                .build(),
            collector::add);
    assertThat(collector)
        .containsExactly(
            AutoView.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .build());
  }
}
