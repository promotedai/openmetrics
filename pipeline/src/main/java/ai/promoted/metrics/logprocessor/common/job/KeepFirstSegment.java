package ai.promoted.metrics.logprocessor.common.job;

import ai.promoted.metrics.logprocessor.common.functions.KeepFirstRow;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.GeneratedMessageV3;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import picocli.CommandLine;

/**
 * A FlinkSegment that deduplicates inputs by putting the first record for a key in a configured
 * time period.
 *
 * <p>TODO - Evaluate if we can avoid an extra shuffle by relying on Kafka keys and using MapState
 * instead. This would probably be a lot more efficient than shuffling records. This could be
 * important for DeliveryLog.
 */
public class KeepFirstSegment implements FlinkSegment {

  private final BaseFlinkJob baseFlinkJob;

  @CommandLine.Option(
      names = {"--uidToKeepFirstDuration"},
      description =
          "The duration to keeping records for deduplication, keyed by source uid.  This is used to de-duplicate raw inputs.  Default=use --defaultKeepFirstDuration.  Java8 Duration parse format.")
  public Map<String, Duration> uidToKeepFirstDuration = new HashMap<>();

  @CommandLine.Option(
      names = {"--defaultKeepFirstDuration"},
      defaultValue = "P1D",
      description =
          "The default duration for keeping records for deduplication.  Default=P1D.  Java8 Duration parse format.")
  public Duration defaultKeepFirstDuration = Duration.parse("P1D");

  public KeepFirstSegment(BaseFlinkJob baseFlinkJob) {
    this.baseFlinkJob = baseFlinkJob;
  }

  @Override
  public void validateArgs() {
    // Do nothing.
  }

  public <T> SingleOutputStreamOperator<T> keepFirst(
      DataStream<T> stream, TypeInformation<T> type, KeySelector<T, ?> keySelector, String id) {
    Duration keepFirstDuration = uidToKeepFirstDuration.getOrDefault(id, defaultKeepFirstDuration);
    return baseFlinkJob.add(
        stream.keyBy(keySelector).process(new KeepFirstRow<>(id, keepFirstDuration), type),
        "keep-first-" + id);
  }

  @Override
  public Set<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    // Relies on clients to register so this class works with non-proto classes too.
    return ImmutableSet.of();
  }
}
