package ai.promoted.metrics.logprocessor.common.job;

import ai.promoted.metrics.logprocessor.common.functions.KeepFirstRow;
import ai.promoted.metrics.logprocessor.common.functions.KeyUtil;
import ai.promoted.proto.event.JoinedEvent;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.GeneratedMessageV3;
import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import picocli.CommandLine;

/**
 * A FlinkSegment that provides joined impressions as a source. It's a separate FlinkSegment to
 * isolate the flags to just jobs that use the deduplication source.
 */
public class JoinedImpressionSegment implements FlinkSegment {

  @CommandLine.Option(
      names = {"--keepFirstJoinedImpressionDuration"},
      defaultValue = "PT6H",
      description =
          "The duration to keep track of recent JoinedImpressions.  This is used to de-duplicate joined inputs.  Default=PT6H.  Java8 Duration parse format.")
  public Duration keepFirstJoinedImpressionDuration = Duration.parse("PT6H");

  private final BaseFlinkJob baseFlinkJob;

  public JoinedImpressionSegment(BaseFlinkJob baseFlinkJob) {
    this.baseFlinkJob = baseFlinkJob;
  }

  @Override
  public void validateArgs() {
    // Do nothing.
  }

  public DataStream<JoinedEvent> getDeduplicatedJoinedImpression(
      DataStream<JoinedEvent> joinedImpression) {
    return baseFlinkJob.add(
        joinedImpression
            .keyBy(KeyUtil.joinedImpressionKey)
            .process(
                new KeepFirstRow<>("joined-impression", keepFirstJoinedImpressionDuration),
                TypeInformation.of(JoinedEvent.class)),
        "keep-first-joined-impression");
  }

  @Override
  public List<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    return ImmutableList.of(JoinedEvent.class);
  }
}
