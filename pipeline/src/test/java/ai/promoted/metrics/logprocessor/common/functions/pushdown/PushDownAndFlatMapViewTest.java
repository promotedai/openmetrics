package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import static com.google.common.truth.Truth.assertThat;

import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.LogRequest;
import ai.promoted.proto.event.View;
import org.junit.jupiter.api.Test;

public class PushDownAndFlatMapViewTest extends BasePushDownAndFlatMapTest<View> {

  @Test
  public void pushAll() {
    new PushDownAndFlatMapView()
        .flatMap(
            setBaseFields(LogRequest.newBuilder())
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .setClientInfo(CLIENT_INFO)
                .addView(View.newBuilder())
                .build(),
            collector::add);
    assertThat(collector)
        .containsExactly(
            View.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .setClientInfo(CLIENT_INFO)
                .build());
  }

  @Test
  public void pushNone() {
    new PushDownAndFlatMapView()
        .flatMap(
            setBaseFields(LogRequest.newBuilder())
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .addView(
                    View.newBuilder()
                        .setPlatformId(PLATFORM_ID)
                        .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                        .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                .build(),
            collector::add);
    assertThat(collector)
        .containsExactly(
            View.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .build());
  }

  @Test
  public void pushDifferent() {
    new PushDownAndFlatMapView()
        .flatMap(
            setBaseFields(LogRequest.newBuilder())
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .addView(
                    View.newBuilder()
                        .setPlatformId(OTHER_PLATFORM_ID)
                        .setUserInfo(UserInfo.newBuilder().setLogUserId(OTHER_LOG_USER_ID))
                        .setTiming(createTiming(OTHER_TIME_EPOCH_MILLIS)))
                .build(),
            collector::add);
    assertThat(collector)
        .containsExactly(
            View.newBuilder()
                .setPlatformId(OTHER_PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(OTHER_LOG_USER_ID))
                .setTiming(createTiming(OTHER_TIME_EPOCH_MILLIS))
                .build());
  }

  @Test
  public void lowerCaseLogViewId() {
    new PushDownAndFlatMapView()
        .flatMap(
            LogRequest.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .addView(
                    View.newBuilder()
                        .setPlatformId(PLATFORM_ID)
                        .setUserInfo(UserInfo.newBuilder().setLogUserId(UPPERCASE_LOG_USER_ID))
                        .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                .build(),
            collector::add);
    assertThat(collector)
        .containsExactly(
            View.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .build());
  }

  @Test
  public void ifNoTopLevelUserInfo() {
    new PushDownAndFlatMapView()
        .flatMap(
            LogRequest.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .addView(
                    View.newBuilder()
                        .setPlatformId(PLATFORM_ID)
                        .setUserInfo(UserInfo.newBuilder().setLogUserId(UPPERCASE_LOG_USER_ID))
                        .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                .build(),
            collector::add);
    assertThat(collector)
        .containsExactly(
            View.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOWERCASE_LOG_USER_ID))
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .build());
  }
}
