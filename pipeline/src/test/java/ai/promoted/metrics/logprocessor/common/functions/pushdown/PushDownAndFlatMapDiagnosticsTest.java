package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.Diagnostics;
import ai.promoted.proto.event.LogRequest;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;

public class PushDownAndFlatMapDiagnosticsTest extends BasePushDownAndFlatMapTest<Diagnostics> {

  @Test
  public void pushAll() {
    new PushDownAndFlatMapDiagnostics().flatMap(
            setBaseFields(LogRequest.newBuilder())
                    .setUserInfo(UserInfo.newBuilder()
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .setClientInfo(CLIENT_INFO)
                    .addDiagnostics(Diagnostics.newBuilder())
                    .build(),
            collector::add);
    assertThat(collector).containsExactly(
            Diagnostics.newBuilder()
                    .setPlatformId(PLATFORM_ID)
                    .setUserInfo(UserInfo.newBuilder()
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .setTiming(createTiming(TIME_EPOCH_MILLIS))
                    .setClientInfo(CLIENT_INFO)
                    .build());
  }

  @Test
  public void pushNone() {
    new PushDownAndFlatMapDiagnostics().flatMap(
            setBaseFields(LogRequest.newBuilder())
                    .setUserInfo(UserInfo.newBuilder()
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .addDiagnostics(Diagnostics.newBuilder()
                            .setPlatformId(PLATFORM_ID)
                            .setUserInfo(UserInfo.newBuilder()
                                    .setLogUserId(LOWERCASE_LOG_USER_ID))
                            .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                    .build(),
            collector::add);
    assertThat(collector).containsExactly(
            Diagnostics.newBuilder()
                    .setPlatformId(PLATFORM_ID)
                    .setUserInfo(UserInfo.newBuilder()
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .setTiming(createTiming(TIME_EPOCH_MILLIS))
                    .build());
  }

  @Test
  public void pushDifferent() {
    new PushDownAndFlatMapDiagnostics().flatMap(
            setBaseFields(LogRequest.newBuilder())
                    .setUserInfo(UserInfo.newBuilder()
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .addDiagnostics(Diagnostics.newBuilder()
                            .setPlatformId(OTHER_PLATFORM_ID)
                            .setUserInfo(UserInfo.newBuilder()
                                    .setLogUserId(OTHER_LOG_USER_ID))
                            .setTiming(createTiming(OTHER_TIME_EPOCH_MILLIS)))
                    .build(),
            collector::add);
    assertThat(collector).containsExactly(
            Diagnostics.newBuilder()
                    .setPlatformId(OTHER_PLATFORM_ID)
                    .setUserInfo(UserInfo.newBuilder()
                            .setLogUserId(OTHER_LOG_USER_ID))
                    .setTiming(createTiming(OTHER_TIME_EPOCH_MILLIS))
                    .build());
  }


  @Test
  public void lowerCaseLogDiagnosticsId() {
    new PushDownAndFlatMapDiagnostics().flatMap(
            LogRequest.newBuilder()
                    .setPlatformId(PLATFORM_ID)
                    .setUserInfo(UserInfo.newBuilder()
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .setTiming(createTiming(TIME_EPOCH_MILLIS))
                    .addDiagnostics(Diagnostics.newBuilder()
                            .setPlatformId(PLATFORM_ID)
                            .setUserInfo(UserInfo.newBuilder()
                                    .setLogUserId(UPPERCASE_LOG_USER_ID))
                            .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                    .build(),
            collector::add);
    assertThat(collector).containsExactly(
            Diagnostics.newBuilder()
                    .setPlatformId(PLATFORM_ID)
                    .setUserInfo(UserInfo.newBuilder()
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .setTiming(createTiming(TIME_EPOCH_MILLIS))
                    .build());
  }

  @Test
  public void ifNoTopLevelUserInfo() {
    new PushDownAndFlatMapDiagnostics().flatMap(
            LogRequest.newBuilder()
                    .setPlatformId(PLATFORM_ID)
                    .setTiming(createTiming(TIME_EPOCH_MILLIS))
                    .addDiagnostics(Diagnostics.newBuilder()
                            .setPlatformId(PLATFORM_ID)
                            .setUserInfo(UserInfo.newBuilder()
                                    .setLogUserId(UPPERCASE_LOG_USER_ID))
                            .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                    .build(),
            collector::add);
    assertThat(collector).containsExactly(
            Diagnostics.newBuilder()
                    .setPlatformId(PLATFORM_ID)
                    .setUserInfo(UserInfo.newBuilder()
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .setTiming(createTiming(TIME_EPOCH_MILLIS))
                    .build());
  }
}