package ai.promoted.metrics.logprocessor.common.functions;

import java.time.Clock;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.util.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A {@code BoundedOutOfOrdernessWatermarks} but meant for local development. During local
 * development (for now), we want to increase the watermark more frequently to output events.
 *
 * <p>This file was copied and forked from {@code BoundedOutOfOrdernessWatermarks}.
 */
public class LocalProcAndEventBoundedOutOfOrdernessWatermarkGenerator<T>
    implements WatermarkGenerator<T> {
  private static final Logger LOGGER =
      LogManager.getLogger(LocalProcAndEventBoundedOutOfOrdernessWatermarkGenerator.class);

  private final Clock clock;

  // Code is partially copied from this example.
  // https://github.com/aljoscha/flink/blob/6e4419e550caa0e5b162bc0d2ccc43f6b0b3860f/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/functions/timestamps/ProcessingTimeTrailingBoundedOutOfOrdernessTimestampExtractor.java

  /**
   * The (fixed) interval between the maximum seen timestamp seen in the records and that of the
   * watermark to be emitted.
   */
  private final long maxOutOfOrdernessMillis;

  /**
   * After not extracting a timestamp for this duration the stream is considered idle. We will then
   * generate a watermark that trails by a certain amount behind processing time.
   */
  private final long idleThresholdMillis;

  /** The last event timestamp seen so far. */
  private long lastEventTimestamp = 0;

  /** The timestamp of the last emitted watermark. */
  private long lastEmittedWatermark = Long.MIN_VALUE;

  // MAX_VALUE is a default that has special handling.
  private long lastUpdatedTimestamp = Long.MAX_VALUE;

  // State to track receiving events with the same timestamp (fake data generator).
  // We initialize nEventsWithSameTimestamp to 1 to make the count accurate.  It really doesn't
  // matter due to the reset behavior in onEvent.
  private long nEventsWithSameTimestamp = 1;
  private long lastNEventsWithSameTimestamp = 0;

  /** The amount by which the idle-watermark trails behind current processing time. */
  private final long processingTimeTrailingMillis;

  public LocalProcAndEventBoundedOutOfOrdernessWatermarkGenerator(
      Clock clock,
      Duration maxOutOfOrderness,
      Duration idleDetectionThreshold,
      Duration processingTimeTrailingDuration) {
    this.clock = Preconditions.checkNotNull(clock, "maxOutOfOrderness");
    Preconditions.checkNotNull(maxOutOfOrderness, "maxOutOfOrderness");
    Preconditions.checkArgument(
        !maxOutOfOrderness.isNegative(), "maxOutOfOrderness cannot be negative");
    this.maxOutOfOrdernessMillis = maxOutOfOrderness.toMillis();
    this.lastEventTimestamp = Long.MIN_VALUE + this.maxOutOfOrdernessMillis + 1L;
    this.idleThresholdMillis = idleDetectionThreshold.toMillis();
    this.processingTimeTrailingMillis = processingTimeTrailingDuration.toMillis();
  }

  public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
    if (lastEventTimestamp == eventTimestamp) nEventsWithSameTimestamp++;
    else nEventsWithSameTimestamp = 1;
    this.lastEventTimestamp = Math.max(this.lastEventTimestamp, eventTimestamp);
    lastUpdatedTimestamp = clock.millis();
  }

  public void onPeriodicEmit(WatermarkOutput output) {
    long currentTimeMillis = clock.millis();

    // also initialize the lastUpdatedTimestamp here in case we never saw an element
    if (lastUpdatedTimestamp == Long.MAX_VALUE) {
      lastUpdatedTimestamp = currentTimeMillis;
    }

    // this guarantees that the watermark never goes backwards.
    long potentialWM = lastEventTimestamp - maxOutOfOrdernessMillis - 1;

    if (potentialWM > lastEmittedWatermark) {
      // update based on timestamps if we see progress
      lastEmittedWatermark = potentialWM;
      lastNEventsWithSameTimestamp = 0;
    } else if (currentTimeMillis - lastUpdatedTimestamp > idleThresholdMillis) {
      if (potentialWM == lastEmittedWatermark
          && nEventsWithSameTimestamp != lastNEventsWithSameTimestamp) {
        // still getting new events
        lastNEventsWithSameTimestamp = nEventsWithSameTimestamp;
      } else {
        // no incoming event and we're past the idle threshold, so bump watermark based on
        // processing time
        long processingTimeWatermark = currentTimeMillis - processingTimeTrailingMillis - 1;
        if (processingTimeWatermark > lastEmittedWatermark) {
          lastEmittedWatermark = processingTimeWatermark;
        }
      }
    }
    LOGGER.trace(
        "emitting instance: {} watermark: {}", System.identityHashCode(this), lastEmittedWatermark);
    output.emitWatermark(new Watermark(lastEmittedWatermark));
  }
}
