package ai.promoted.metrics.logprocessor.common.functions;

import java.time.Clock;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

/** Factory class to generate {@link LocalProcAndEventBoundedOutOfOrdernessWatermarkGenerator}. */
public class LocalProcAndEventBoundedOutOfOrdernessWatermarkStrategy<T>
    implements WatermarkStrategy<T> {
  private final Duration maxOutOfOrderness;
  private final Duration watermarkIdleness;
  private final Duration processingTimeTrailingDuration;

  public LocalProcAndEventBoundedOutOfOrdernessWatermarkStrategy(
      Duration maxOutOfOrderness,
      Duration watermarkIdleness,
      Duration processingTimeTrailingDuration) {
    this.maxOutOfOrderness = maxOutOfOrderness;
    this.watermarkIdleness = watermarkIdleness;
    this.processingTimeTrailingDuration = processingTimeTrailingDuration;
  }

  @Override
  public WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context var1) {
    return new LocalProcAndEventBoundedOutOfOrdernessWatermarkGenerator<T>(
        Clock.systemDefaultZone(),
        maxOutOfOrderness,
        watermarkIdleness,
        processingTimeTrailingDuration);
  }
}
