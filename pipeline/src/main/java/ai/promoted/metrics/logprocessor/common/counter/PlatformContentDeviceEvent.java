package ai.promoted.metrics.logprocessor.common.counter;

import ai.promoted.proto.delivery.internal.features.AggMetric;
import java.time.temporal.ChronoUnit;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

/** platformId, contentId, device, aggMetric */
public class PlatformContentDeviceEvent extends Tuple4<Long, String, String, String>
    implements JoinedEventRedisHashSupplier {
  public static final String NAME = "plat-cont_dvc-$evt";
  public static final String DEPRECATED_NAME = "content-dvc";

  public PlatformContentDeviceEvent() {}

  public PlatformContentDeviceEvent(
      Long platformId, String contentId, String device, String aggMetrics) {
    super(platformId, contentId, device, aggMetrics);
  }

  public Long getPlatformId() {
    return f0;
  }

  public String getContentId() {
    return f1;
  }

  public String getDevice() {
    return f2;
  }

  public String getAggMetrics() {
    return f3;
  }

  /** (platformId, contentId, "dvc") */
  @Override
  public Tuple toRedisHashKey() {
    return Tuple3.of(getPlatformId(), getContentId(), "dvc");
  }

  /** (device, featId) */
  @Override
  public Tuple toRedisHashField(int windowSize, ChronoUnit windowUnit) {
    return Tuple2.of(
        getDevice(),
        FeatureId.itemDeviceCount(AggMetric.valueOf(getAggMetrics()), windowSize, windowUnit));
  }
}
