package ai.promoted.metrics.logprocessor.common.counter;

import ai.promoted.proto.delivery.internal.features.AggMetric;
import java.time.temporal.ChronoUnit;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/** platformId, device, aggMetric */
public class PlatformDeviceEvent extends Tuple3<Long, String, String>
    implements JoinedEventRedisHashSupplier {

  public static final String NAME = "plat_dvc-$evt";
  public static final String DEPRECATED_NAME = "platform-dvc";

  public PlatformDeviceEvent() {}

  public PlatformDeviceEvent(Long platformId, String device, String countAggValue) {
    super(platformId, device, countAggValue);
  }

  public Long getPlatformId() {
    return f0;
  }

  public String getDevice() {
    return f1;
  }

  public String getCountAggValue() {
    return f2;
  }

  /** (platformId, "dvc") */
  @Override
  public Tuple toRedisHashKey() {
    return Tuple2.of(getPlatformId(), "dvc");
  }

  /** (device, featureId) */
  @Override
  public Tuple toRedisHashField(int windowSize, ChronoUnit windowUnit) {
    return Tuple2.of(
        getDevice(),
        FeatureId.itemDeviceCount(AggMetric.valueOf(getCountAggValue()), windowSize, windowUnit));
  }
}
