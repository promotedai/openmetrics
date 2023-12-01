package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import ai.promoted.metrics.logprocessor.common.functions.base.SerializableBiConsumer;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableFunction;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableToLongFunction;
import ai.promoted.proto.common.ClientInfo;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.LogRequest;
import java.io.Serializable;
import java.util.List;
import java.util.function.Consumer;

/**
 * Uses fields from {@link LogRequest} as default common fields when the records do not set them.
 * This allows sharing common fields across multiple records. E.g. Timing, ClientInfo, Device.
 *
 * <p>This is done very early in our Flink jobs so we can convert incoming LogRequests to specific
 * records.
 *
 * @param <T> the type of the log record on {@link LogRequest}
 */
public class BasePushDownAndFlatMap<T, B> implements Serializable {
  private static final long serialVersionUID = 1L;

  // TODO(PRO-2420) - In Metrics API, set a primary key on LogRequest.  Change auto-generated UUIDs
  // to be based on the LogRequest.log_request_id.

  private final SerializableFunction<LogRequest, List<T>> getRecords;
  private final SerializableFunction<T, B> toBuilder;
  private final SerializableFunction<B, T> build;
  private final SerializableToLongFunction<B> getPlatformId;
  private final SerializableBiConsumer<B, Long> setPlatformId;
  private final SerializableFunction<B, Boolean> hasUserInfo;
  private final SerializableFunction<B, UserInfo> getUserInfo;
  private final SerializableBiConsumer<B, UserInfo> setUserInfo;
  private final SerializableFunction<B, Boolean> hasTiming;
  private final SerializableFunction<B, Timing> getTiming;
  private final SerializableBiConsumer<B, Timing> setTiming;
  private final SerializableFunction<B, Boolean> hasClientInfo;
  private final SerializableFunction<B, ClientInfo> getClientInfo;
  private final SerializableBiConsumer<B, ClientInfo> setClientInfo;

  protected BasePushDownAndFlatMap(
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
      SerializableBiConsumer<B, ClientInfo> setClientInfo) {
    this.getRecords = getRecords;
    this.toBuilder = toBuilder;
    this.build = build;
    this.getPlatformId = getPlatformId;
    this.setPlatformId = setPlatformId;
    this.hasUserInfo = hasUserInfo;
    this.getUserInfo = getUserInfo;
    this.setUserInfo = setUserInfo;
    this.hasTiming = hasTiming;
    this.getTiming = getTiming;
    this.setTiming = setTiming;
    this.hasClientInfo = hasClientInfo;
    this.getClientInfo = getClientInfo;
    this.setClientInfo = setClientInfo;
  }

  public void flatMap(LogRequest logRequest, Consumer<T> out) {
    // Do this across all logUserIds.
    getRecords.apply(logRequest).stream()
        .map(record -> pushDownFields(record, logRequest))
        .forEach(out);
  }

  public T pushDownFields(T record, LogRequest batchValue) {
    B builder = toBuilder.apply(record);
    pushDownFieldsOnBuilder(builder, batchValue);
    return build.apply(builder);
  }

  protected void pushDownFieldsOnBuilder(B builder, LogRequest batchValue) {
    pushDownPlatformId(builder, batchValue);
    pushDownUserInfo(builder, batchValue);
    pushDownTiming(builder, batchValue);
    pushDownClientInfo(builder, batchValue);
  }

  protected void pushDownPlatformId(B builder, LogRequest batchValue) {
    if (getPlatformId.applyAsLong(builder) == 0) {
      setPlatformId.accept(builder, batchValue.getPlatformId());
    }
  }

  private void pushDownUserInfo(B builder, LogRequest batchValue) {
    if (!hasUserInfo.apply(builder) && batchValue.hasUserInfo()) {
      setUserInfo.accept(
          builder,
          batchValue.getUserInfo().toBuilder().mergeFrom(getUserInfo.apply(builder)).build());
    }
  }

  /**
   * Push down Timing field. Timing is a little more unique because eventApiTimestamp is only set on
   * the {@code LogRequest.timing} and we want to push it down even if a Timing block exists on the
   * individual log records.
   */
  private void pushDownTiming(B builder, LogRequest batchValue) {
    if (batchValue.hasTiming()) {
      if (!hasTiming.apply(builder)) {
        setTiming.accept(
            builder,
            batchValue.getTiming().toBuilder().mergeFrom(getTiming.apply(builder)).build());
      } else {
        Timing timing = getTiming.apply(builder);
        if (timing.getEventApiTimestamp() == 0L
            && batchValue.getTiming().getEventApiTimestamp() != 0L) {
          setTiming.accept(
              builder,
              timing.toBuilder()
                  .setEventApiTimestamp(batchValue.getTiming().getEventApiTimestamp())
                  .build());
        }
      }
    }
  }

  private void pushDownClientInfo(B builder, LogRequest batchValue) {
    if (!hasClientInfo.apply(builder) && batchValue.hasClientInfo()) {
      setClientInfo.accept(
          builder,
          batchValue.getClientInfo().toBuilder().mergeFrom(getClientInfo.apply(builder)).build());
    }
  }
}
