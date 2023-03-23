package ai.promoted.metrics.logprocessor.common.counter;

import ai.promoted.proto.delivery.internal.features.AggMetric;
import java.time.temporal.ChronoUnit;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

/** platformId, aggMetric, OS, userAgent */
public class GlobalEventDevice extends Tuple4<Long, String, String, String>
    implements JoinedEventRedisHashSupplier {

  public static final String NAME = "platform-device";

  public GlobalEventDevice() {}

  public GlobalEventDevice(Long platformId, String countAggValue, String os, String userAgent) {
    super(platformId, countAggValue, os, userAgent);
  }

  public Long getPlatformId() {
    return f0;
  }

  public String getCountAggValue() {
    return f1;
  }

  public String getOs() {
    return f2;
  }

  public String getUserAgent() {
    return f3;
  }

  /** (platformId) */
  @Override
  public Tuple getHashKey() {
    return Tuple1.of(getPlatformId());
  }

  /** (os, userAgent, featureId) */
  @Override
  public Tuple getHashField(int windowSize, ChronoUnit windowUnit) {
    return Tuple3.of(
        getOs(),
        getUserAgent(),
        FeatureId.itemDeviceCount(AggMetric.valueOf(getCountAggValue()), windowSize, windowUnit));
  }
}
