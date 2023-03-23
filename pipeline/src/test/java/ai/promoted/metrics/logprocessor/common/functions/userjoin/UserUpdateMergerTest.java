package ai.promoted.metrics.logprocessor.common.functions.userjoin;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.promoted.proto.common.Properties;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.User;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Test;

public class UserUpdateMergerTest {

  @Test
  public void noUsers() {
    UserUpdateMerger builder = new UserUpdateMerger(ImmutableList.of(), true);
    assertThat(builder.getEffectiveUser()).isNull();
    assertThat(builder.getRemainingUserUpdatesCount()).isEqualTo(0);
    assertThat(builder.getRemainingUserUpdates()).isEmpty();
    builder.advanceTo(100L);
    assertThat(builder.getEffectiveUser()).isNull();
    assertThat(builder.getRemainingUserUpdatesCount()).isEqualTo(0);
    assertThat(builder.getRemainingUserUpdates()).isEmpty();
  }

  @Test
  public void oneUser() {
    User userUpdate1 = createUser(10L);
    UserUpdateMerger builder = new UserUpdateMerger(ImmutableList.of(userUpdate1), true);
    assertThat(builder.getEffectiveUser()).isNull();
    assertThat(builder.getRemainingUserUpdatesCount()).isEqualTo(1);
    assertThat(builder.getRemainingUserUpdates()).containsExactly(userUpdate1);

    builder.advanceTo(9L);
    assertThat(builder.getEffectiveUser()).isNull();
    assertThat(builder.getRemainingUserUpdatesCount()).isEqualTo(1);
    assertThat(builder.getRemainingUserUpdates()).containsExactly(userUpdate1);

    builder.advanceTo(10L);
    assertThat(builder.getEffectiveUser()).isEqualTo(userUpdate1);
    assertThat(builder.getRemainingUserUpdatesCount()).isEqualTo(0);
    assertThat(builder.getRemainingUserUpdates()).isEmpty();
  }

  @Test
  public void multipleUserUpdates() {
    User userUpdate1 = createUser(10L);
    User userUpdate2 =
        User.newBuilder()
            .setPlatformId(1L)
            .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1").setIsInternalUser(true))
            .setTiming(Timing.newBuilder().setLogTimestamp(20).setEventApiTimestamp(18))
            .setProperties(
                Properties.newBuilder()
                    .setStruct(
                        Struct.newBuilder()
                            .putFields("key1", Value.newBuilder().setNumberValue(1).build())))
            .build();
    User userUpdate3 =
        createUserBuilder(30L)
            .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1").setIgnoreUsage(true))
            .setProperties(
                Properties.newBuilder()
                    .setStruct(
                        Struct.newBuilder()
                            .putFields("key2", Value.newBuilder().setNumberValue(2).build())))
            .build();
    User userUpdate4 =
        createUserBuilder(40L)
            .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1"))
            .build();
    UserUpdateMerger builder =
        new UserUpdateMerger(
            ImmutableList.of(userUpdate1, userUpdate2, userUpdate3, userUpdate4), true);

    assertThat(builder.getEffectiveUser()).isNull();
    assertThat(builder.getRemainingUserUpdatesCount()).isEqualTo(4);
    assertThat(builder.getRemainingUserUpdates())
        .containsExactly(userUpdate1, userUpdate2, userUpdate3, userUpdate4);

    builder.advanceTo(9L);
    assertThat(builder.getEffectiveUser()).isNull();
    assertThat(builder.getRemainingUserUpdatesCount()).isEqualTo(4);
    assertThat(builder.getRemainingUserUpdates())
        .containsExactly(userUpdate1, userUpdate2, userUpdate3, userUpdate4);

    builder.advanceTo(10L);
    assertThat(builder.getEffectiveUser()).isEqualTo(createUser(10L));
    assertThat(builder.getRemainingUserUpdatesCount()).isEqualTo(3);
    assertThat(builder.getRemainingUserUpdates())
        .containsExactly(userUpdate2, userUpdate3, userUpdate4);

    builder.advanceTo(20L);
    assertThat(builder.getEffectiveUser()).isEqualTo(userUpdate2);
    assertThat(builder.getRemainingUserUpdatesCount()).isEqualTo(2);
    assertThat(builder.getRemainingUserUpdates()).containsExactly(userUpdate3, userUpdate4);

    builder.advanceTo(30L);
    assertThat(builder.getEffectiveUser())
        .isEqualTo(
            createUserBuilder(30L)
                .setUserInfo(
                    UserInfo.newBuilder()
                        .setLogUserId("logUserId1")
                        .setIsInternalUser(true)
                        .setIgnoreUsage(true))
                .setProperties(
                    Properties.newBuilder()
                        .setStruct(
                            Struct.newBuilder()
                                .putFields("key2", Value.newBuilder().setNumberValue(2).build())))
                .build());
    assertThat(builder.getRemainingUserUpdatesCount()).isEqualTo(1);
    assertThat(builder.getRemainingUserUpdates()).containsExactly(userUpdate4);

    builder.advanceTo(40L);
    assertThat(builder.getEffectiveUser())
        .isEqualTo(
            createUserBuilder(40L)
                .setUserInfo(
                    UserInfo.newBuilder().setLogUserId("logUserId1").setIsInternalUser(true))
                .setProperties(
                    Properties.newBuilder()
                        .setStruct(
                            Struct.newBuilder()
                                .putFields("key2", Value.newBuilder().setNumberValue(2).build())))
                .build());
    assertThat(builder.getRemainingUserUpdatesCount()).isEqualTo(0);
    assertThat(builder.getRemainingUserUpdates()).isEmpty();
  }

  @Test
  public void assertSameLogUserId() {
    User userUpdate1 =
        createUser(10L).toBuilder()
            .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1"))
            .build();
    User userUpdate2 =
        createUser(20L).toBuilder()
            .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId2"))
            .build();
    UserUpdateMerger builder =
        new UserUpdateMerger(ImmutableList.of(userUpdate1, userUpdate2), true);
    assertThrows(IllegalStateException.class, () -> builder.advanceTo(30L));
  }

  @Test
  public void assertSameUserId() {
    User userUpdate1 =
        createUser(10L).toBuilder().setUserInfo(UserInfo.newBuilder().setUserId("userId1")).build();
    User userUpdate2 =
        createUser(20L).toBuilder().setUserInfo(UserInfo.newBuilder().setUserId("userId2")).build();
    UserUpdateMerger builder =
        new UserUpdateMerger(ImmutableList.of(userUpdate1, userUpdate2), true);
    assertThrows(IllegalStateException.class, () -> builder.advanceTo(30L));
  }

  private static User createUser(long logTimestamp) {
    return createUserBuilder(logTimestamp).build();
  }

  private static User.Builder createUserBuilder(long logTimestamp) {
    return User.newBuilder()
        .setPlatformId(1L)
        .setTiming(Timing.newBuilder().setLogTimestamp(logTimestamp));
  }
}
