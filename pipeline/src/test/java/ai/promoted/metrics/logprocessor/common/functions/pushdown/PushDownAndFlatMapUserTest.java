package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.LogRequest;
import ai.promoted.proto.event.User;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;

public class PushDownAndFlatMapUserTest extends BasePushDownAndFlatMapTest<User> {

  @Test
  public void pushAll() {
    new PushDownAndFlatMapUser().flatMap(
            setBaseFields(LogRequest.newBuilder())
                    .setUserInfo(UserInfo.newBuilder()
                            .setUserId(USER_ID)
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .setClientInfo(CLIENT_INFO)
                    .addUser(User.newBuilder())
                    .build(),
            collector::add);
    assertThat(collector).containsExactly(
            User.newBuilder()
                    .setPlatformId(PLATFORM_ID)
                    .setUserInfo(UserInfo.newBuilder()
                            .setUserId(USER_ID)
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .setTiming(createTiming(TIME_EPOCH_MILLIS))
                    .setClientInfo(CLIENT_INFO)
                    .build());
  }

  @Test
  public void pushNone() {
    new PushDownAndFlatMapUser().flatMap(
            setBaseFields(LogRequest.newBuilder())
                    .setUserInfo(UserInfo.newBuilder()
                            .setUserId(USER_ID)
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .addUser(User.newBuilder()
                            .setPlatformId(PLATFORM_ID)
                            .setUserInfo(UserInfo.newBuilder()
                                    .setUserId(USER_ID)
                                    .setLogUserId(LOWERCASE_LOG_USER_ID))
                            .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                    .build(),
            collector::add);
    assertThat(collector).containsExactly(
            User.newBuilder()
                    .setPlatformId(PLATFORM_ID)
                    .setUserInfo(UserInfo.newBuilder()
                            .setUserId(USER_ID)
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .setTiming(createTiming(TIME_EPOCH_MILLIS))
                    .build());
  }

  @Test
  public void pushDifferent() {
    new PushDownAndFlatMapUser().flatMap(
            setBaseFields(LogRequest.newBuilder())
                    .setUserInfo(UserInfo.newBuilder()
                            .setUserId(USER_ID)
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .addUser(User.newBuilder()
                            .setPlatformId(OTHER_PLATFORM_ID)
                            .setUserInfo(UserInfo.newBuilder()
                                    .setUserId(OTHER_USER_ID)
                                    .setLogUserId(OTHER_LOG_USER_ID))
                            .setTiming(createTiming(OTHER_TIME_EPOCH_MILLIS)))
                    .build(),
            collector::add);
    assertThat(collector).containsExactly(
            User.newBuilder()
                    .setPlatformId(OTHER_PLATFORM_ID)
                    .setUserInfo(UserInfo.newBuilder()
                            .setUserId(OTHER_USER_ID)
                            .setLogUserId(OTHER_LOG_USER_ID))
                    .setTiming(createTiming(OTHER_TIME_EPOCH_MILLIS))
                    .build());
  }


  @Test
  public void lowerCaseLogUserId() {
    new PushDownAndFlatMapUser().flatMap(
            LogRequest.newBuilder()
                    .setPlatformId(PLATFORM_ID)
                    .setUserInfo(UserInfo.newBuilder()
                            .setUserId(USER_ID)
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .setTiming(createTiming(TIME_EPOCH_MILLIS))
                    .addUser(User.newBuilder()
                            .setPlatformId(PLATFORM_ID)
                            .setUserInfo(UserInfo.newBuilder()
                                    .setUserId(USER_ID)
                                    .setLogUserId(UPPERCASE_LOG_USER_ID))
                            .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                    .build(),
            collector::add);
    assertThat(collector).containsExactly(
            User.newBuilder()
                    .setPlatformId(PLATFORM_ID)
                    .setUserInfo(UserInfo.newBuilder()
                            .setUserId(USER_ID)
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .setTiming(createTiming(TIME_EPOCH_MILLIS))
                    .build());
  }

  @Test
  public void ifNoTopLevelUserInfo() {
    new PushDownAndFlatMapUser().flatMap(
            LogRequest.newBuilder()
                    .setPlatformId(PLATFORM_ID)
                    .setTiming(createTiming(TIME_EPOCH_MILLIS))
                    .addUser(User.newBuilder()
                            .setPlatformId(PLATFORM_ID)
                            .setUserInfo(UserInfo.newBuilder()
                                    .setUserId(USER_ID)
                                    .setLogUserId(UPPERCASE_LOG_USER_ID))
                            .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                    .build(),
            collector::add);
    assertThat(collector).containsExactly(
            User.newBuilder()
                    .setPlatformId(PLATFORM_ID)
                    .setUserInfo(UserInfo.newBuilder()
                            .setUserId(USER_ID)
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .setTiming(createTiming(TIME_EPOCH_MILLIS))
                    .build());
  }
}