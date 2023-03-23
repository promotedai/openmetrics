package ai.promoted.metrics.logprocessor.common.counter;

import ai.promoted.proto.delivery.internal.features.AggMetric;
import java.time.temporal.ChronoUnit;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;

/** platformId, contentId, aggMetric, OS, userAgent */
public class ContentEventDevice extends Tuple5<Long, String, String, String, String>
    implements JoinedEventRedisHashSupplier {
  public static final String NAME = "content-device";

  public ContentEventDevice() {}

  public ContentEventDevice(
      Long platformId, String contentId, String aggMetrics, String os, String userAgent) {
    super(platformId, contentId, aggMetrics, os, userAgent);
  }

  public Long getPlatformId() {
    return f0;
  }

  public String getContentId() {
    return f1;
  }

  public String getAggMetrics() {
    return f2;
  }

  public String getOs() {
    return f3;
  }

  public String getUserAgent() {
    return f4;
  }

  /** (platformId, contentId) */
  @Override
  public Tuple getHashKey() {
    return Tuple2.of(getPlatformId(), getContentId());
  }

  /** (os, userAgent, featId) */
  @Override
  public Tuple getHashField(int windowSize, ChronoUnit windowUnit) {
    return Tuple3.of(
        getOs(),
        getUserAgent(),
        FeatureId.itemDeviceCount(AggMetric.valueOf(getAggMetrics()), windowSize, windowUnit));
  }
}
