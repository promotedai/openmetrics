package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import ai.promoted.metrics.logprocessor.common.functions.base.SerializableBiConsumer;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableFunction;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableToLongFunction;
import ai.promoted.proto.common.ClientInfo;
import ai.promoted.proto.common.Device;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.LogRequest;
import java.util.List;

/**
 * A {@code BasePushDownAndFlatMap} with DeviceInfo.
 *
 * @param <T> the type of the log record on {@link LogRequest}
 * @param <B> builder for T.
 */
public class BaseDevicePushDownAndFlatMap<T, B> extends BasePushDownAndFlatMap<T, B> {
  private final SerializableFunction<B, Boolean> hasDevice;
  private final SerializableFunction<B, Device> getDevice;
  private final SerializableBiConsumer<B, Device> setDevice;

  protected BaseDevicePushDownAndFlatMap(
      SerializableFunction<LogRequest, List<T>> getRecords,
      SerializableFunction<T, B> toBuilder,
      SerializableFunction<B, T> build,
      SerializableToLongFunction<B> getPlatformId,
      SerializableBiConsumer<B, Long> setPlatformId,
      SerializableFunction<B, Boolean> hasUserInfo,
      SerializableFunction<B, UserInfo> getUserInfo,
      SerializableBiConsumer<B, UserInfo> setUserInfo,
      SerializableFunction<B, Boolean> hasTiming,
      SerializableFunction<B, Timing> getTiming,
      SerializableBiConsumer<B, Timing> setTiming,
      SerializableFunction<B, Boolean> hasClientInfo,
      SerializableFunction<B, ClientInfo> getClientInfo,
      SerializableBiConsumer<B, ClientInfo> setClientInfo,
      SerializableFunction<B, Boolean> hasDevice,
      SerializableFunction<B, Device> getDevice,
      SerializableBiConsumer<B, Device> setDevice) {
    super(
        getRecords,
        toBuilder,
        build,
        getPlatformId,
        setPlatformId,
        hasUserInfo,
        getUserInfo,
        setUserInfo,
        hasTiming,
        getTiming,
        setTiming,
        hasClientInfo,
        getClientInfo,
        setClientInfo);
    this.hasDevice = hasDevice;
    this.getDevice = getDevice;
    this.setDevice = setDevice;
  }

  @Override
  protected void pushDownFieldsOnBuilder(B builder, LogRequest batchValue) {
    super.pushDownFieldsOnBuilder(builder, batchValue);
    pushDownDevice(builder, batchValue);
  }

  private void pushDownDevice(B builder, LogRequest batchValue) {
    if (!hasDevice.apply(builder) && batchValue.hasDevice()) {
      setDevice.accept(
          builder, batchValue.getDevice().toBuilder().mergeFrom(getDevice.apply(builder)).build());
    }
  }
}
