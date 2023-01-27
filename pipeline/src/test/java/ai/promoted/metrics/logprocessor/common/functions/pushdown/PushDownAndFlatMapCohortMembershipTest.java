package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.CohortMembership;
import ai.promoted.proto.event.LogRequest;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;

public class PushDownAndFlatMapCohortMembershipTest extends BasePushDownAndFlatMapTest<CohortMembership> {

  @Test
  public void pushAll() {
    new PushDownAndFlatMapCohortMembership().flatMap(
            setBaseFields(LogRequest.newBuilder())
                    .setUserInfo(UserInfo.newBuilder()
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .setClientInfo(CLIENT_INFO)
                    .addCohortMembership(CohortMembership.newBuilder())
                    .build(),
            collector::add);
    assertThat(collector).containsExactly(
            CohortMembership.newBuilder()
                    .setPlatformId(PLATFORM_ID)
                    .setUserInfo(UserInfo.newBuilder()
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .setTiming(createTiming(TIME_EPOCH_MILLIS))
                    .setClientInfo(CLIENT_INFO)
                    .build());
  }

  @Test
  public void pushNone() {
    new PushDownAndFlatMapCohortMembership().flatMap(
            setBaseFields(LogRequest.newBuilder())
                    .setUserInfo(UserInfo.newBuilder()
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .addCohortMembership(CohortMembership.newBuilder()
                            .setPlatformId(PLATFORM_ID)
                            .setUserInfo(UserInfo.newBuilder()
                                    .setLogUserId(LOWERCASE_LOG_USER_ID))
                            .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                    .build(),
            collector::add);
    assertThat(collector).containsExactly(
            CohortMembership.newBuilder()
                    .setPlatformId(PLATFORM_ID)
                    .setUserInfo(UserInfo.newBuilder()
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .setTiming(createTiming(TIME_EPOCH_MILLIS))
                    .build());
  }

  @Test
  public void pushDifferent() {
    new PushDownAndFlatMapCohortMembership().flatMap(
            setBaseFields(LogRequest.newBuilder())
                    .setUserInfo(UserInfo.newBuilder()
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .addCohortMembership(CohortMembership.newBuilder()
                            .setPlatformId(OTHER_PLATFORM_ID)
                            .setUserInfo(UserInfo.newBuilder()
                                    .setLogUserId(OTHER_LOG_USER_ID))
                            .setTiming(createTiming(OTHER_TIME_EPOCH_MILLIS)))
                    .build(),
            collector::add);
    assertThat(collector).containsExactly(
            CohortMembership.newBuilder()
                    .setPlatformId(OTHER_PLATFORM_ID)
                    .setUserInfo(UserInfo.newBuilder()
                            .setLogUserId(OTHER_LOG_USER_ID))
                    .setTiming(createTiming(OTHER_TIME_EPOCH_MILLIS))
                    .build());
  }


  @Test
  public void lowerCaseLogCohortMembershipId() {
    new PushDownAndFlatMapCohortMembership().flatMap(
            LogRequest.newBuilder()
                    .setPlatformId(PLATFORM_ID)
                    .setUserInfo(UserInfo.newBuilder()
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .setTiming(createTiming(TIME_EPOCH_MILLIS))
                    .addCohortMembership(CohortMembership.newBuilder()
                            .setPlatformId(PLATFORM_ID)
                            .setUserInfo(UserInfo.newBuilder()
                                    .setLogUserId(UPPERCASE_LOG_USER_ID))
                            .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                    .build(),
            collector::add);
    assertThat(collector).containsExactly(
            CohortMembership.newBuilder()
                    .setPlatformId(PLATFORM_ID)
                    .setUserInfo(UserInfo.newBuilder()
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .setTiming(createTiming(TIME_EPOCH_MILLIS))
                    .build());
  }

  @Test
  public void ifNoTopLevelUserInfo() {
    new PushDownAndFlatMapCohortMembership().flatMap(
            LogRequest.newBuilder()
                    .setPlatformId(PLATFORM_ID)
                    .setTiming(createTiming(TIME_EPOCH_MILLIS))
                    .addCohortMembership(CohortMembership.newBuilder()
                            .setPlatformId(PLATFORM_ID)
                            .setUserInfo(UserInfo.newBuilder()
                                    .setLogUserId(UPPERCASE_LOG_USER_ID))
                            .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                    .build(),
            collector::add);
    assertThat(collector).containsExactly(
            CohortMembership.newBuilder()
                    .setPlatformId(PLATFORM_ID)
                    .setUserInfo(UserInfo.newBuilder()
                            .setLogUserId(LOWERCASE_LOG_USER_ID))
                    .setTiming(createTiming(TIME_EPOCH_MILLIS))
                    .build());
  }
}