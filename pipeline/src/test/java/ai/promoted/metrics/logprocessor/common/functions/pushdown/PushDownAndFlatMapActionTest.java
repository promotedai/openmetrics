package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import static com.google.common.truth.Truth.assertThat;

import ai.promoted.proto.common.Browser;
import ai.promoted.proto.common.ClientHintBrand;
import ai.promoted.proto.common.ClientHints;
import ai.promoted.proto.common.Device;
import ai.promoted.proto.common.DeviceType;
import ai.promoted.proto.common.Timing;
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
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOG_USER_ID))
                .setClientInfo(CLIENT_INFO)
                .addAction(Action.newBuilder())
                .build(),
            collector::add);
    assertThat(collector)
        .containsExactly(
            Action.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOG_USER_ID))
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .setClientInfo(CLIENT_INFO)
                .build());
  }

  @Test
  public void pushNone() {
    new PushDownAndFlatMapAction()
        .flatMap(
            setBaseFields(LogRequest.newBuilder())
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOG_USER_ID))
                .addAction(
                    Action.newBuilder()
                        .setPlatformId(PLATFORM_ID)
                        .setUserInfo(UserInfo.newBuilder().setLogUserId(LOG_USER_ID))
                        .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                .build(),
            collector::add);
    assertThat(collector)
        .containsExactly(
            Action.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOG_USER_ID))
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .build());
  }

  @Test
  public void pushDifferent_eventApiTimestampNotSet() {
    new PushDownAndFlatMapAction()
        .flatMap(
            setBaseFields(LogRequest.newBuilder())
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOG_USER_ID))
                .setTiming(
                    Timing.newBuilder()
                        .setClientLogTimestamp(TIME_EPOCH_MILLIS)
                        .setEventApiTimestamp(1L))
                .addAction(
                    Action.newBuilder()
                        .setPlatformId(OTHER_PLATFORM_ID)
                        .setUserInfo(UserInfo.newBuilder().setLogUserId(OTHER_LOG_USER_ID))
                        .setTiming(
                            Timing.newBuilder().setClientLogTimestamp(OTHER_TIME_EPOCH_MILLIS)))
                .build(),
            collector::add);
    assertThat(collector)
        .containsExactly(
            Action.newBuilder()
                .setPlatformId(OTHER_PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(OTHER_LOG_USER_ID))
                .setTiming(
                    Timing.newBuilder()
                        .setClientLogTimestamp(OTHER_TIME_EPOCH_MILLIS)
                        .setEventApiTimestamp(1L))
                .build());
  }

  @Test
  public void pushDifferent_eventApiTimestampOnLeaf() {
    new PushDownAndFlatMapAction()
        .flatMap(
            setBaseFields(LogRequest.newBuilder())
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOG_USER_ID))
                .setTiming(
                    Timing.newBuilder()
                        .setClientLogTimestamp(TIME_EPOCH_MILLIS)
                        .setEventApiTimestamp(1L))
                .addAction(
                    Action.newBuilder()
                        .setPlatformId(OTHER_PLATFORM_ID)
                        .setUserInfo(UserInfo.newBuilder().setLogUserId(OTHER_LOG_USER_ID))
                        .setTiming(
                            Timing.newBuilder()
                                .setClientLogTimestamp(OTHER_TIME_EPOCH_MILLIS)
                                .setEventApiTimestamp(2L)))
                .build(),
            collector::add);
    assertThat(collector)
        .containsExactly(
            Action.newBuilder()
                .setPlatformId(OTHER_PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(OTHER_LOG_USER_ID))
                .setTiming(
                    Timing.newBuilder()
                        .setClientLogTimestamp(OTHER_TIME_EPOCH_MILLIS)
                        .setEventApiTimestamp(2L))
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
                        .setUserInfo(UserInfo.newBuilder().setLogUserId(LOG_USER_ID))
                        .setTiming(createTiming(TIME_EPOCH_MILLIS)))
                .build(),
            collector::add);
    assertThat(collector)
        .containsExactly(
            Action.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOG_USER_ID))
                .setTiming(createTiming(TIME_EPOCH_MILLIS))
                .build());
  }

  // This checks to make sure we don't do a full proto merge.
  @Test
  public void pushRepeatedField() {
    new PushDownAndFlatMapAction()
        .flatMap(
            LogRequest.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOG_USER_ID))
                .setDevice(
                    Device.newBuilder()
                        .setDeviceType(DeviceType.DESKTOP)
                        .setBrowser(
                            Browser.newBuilder()
                                .setClientHints(
                                    ClientHints.newBuilder()
                                        .addBrand(
                                            ClientHintBrand.newBuilder()
                                                .setBrand("Chrome")
                                                .build()))))
                .addAction(
                    Action.newBuilder()
                        .setDevice(
                            Device.newBuilder()
                                .setDeviceType(DeviceType.DESKTOP)
                                .setBrowser(
                                    Browser.newBuilder()
                                        .setClientHints(
                                            ClientHints.newBuilder()
                                                .addBrand(
                                                    ClientHintBrand.newBuilder()
                                                        .setBrand("Mozilla")
                                                        .build())))))
                .build(),
            collector::add);
    assertThat(collector)
        .containsExactly(
            Action.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOG_USER_ID))
                .setDevice(
                    Device.newBuilder()
                        .setDeviceType(DeviceType.DESKTOP)
                        .setBrowser(
                            Browser.newBuilder()
                                .setClientHints(
                                    ClientHints.newBuilder()
                                        .addBrand(
                                            ClientHintBrand.newBuilder().setBrand("Mozilla")))))
                .build());
  }
}
