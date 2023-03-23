package ai.promoted.metrics.logprocessor.common.job;

import ai.promoted.metrics.logprocessor.common.functions.KeepFirstRow;
import ai.promoted.metrics.logprocessor.common.functions.KeyUtil;
import ai.promoted.proto.event.Action;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.GeneratedMessageV3;
import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import picocli.CommandLine;

/**
 * A FlinkSegment that provides actions as a source. It's a separate FlinkSegment to isolate the
 * flags to just jobs that use the deduplication source.
 */
public class RawActionSegment implements FlinkSegment {

  @CommandLine.Option(
      names = {"--keepFirstActionDuration"},
      defaultValue = "P1D",
      description =
          "The duration to keep track of recent Actions.  This is used to de-duplicate raw inputs.  Default=P1D to match flat_response_insertion join window.  Java8 Duration parse format.")
  public Duration keepFirstActionDuration = Duration.parse("P1D");

  private final BaseFlinkJob baseFlinkJob;

  public RawActionSegment(BaseFlinkJob baseFlinkJob) {
    this.baseFlinkJob = baseFlinkJob;
  }

  @Override
  public void validateArgs() {
    // Do nothing.
  }

  public DataStream<Action> getDeduplicatedAction(DataStream<Action> action) {
    return baseFlinkJob.add(
        action
            .keyBy(KeyUtil.actionKeySelector)
            .process(
                new KeepFirstRow<>("action", keepFirstActionDuration),
                TypeInformation.of(Action.class)),
        "keep-first-action");
  }

  @Override
  public List<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    return ImmutableList.of(Action.class);
  }
}
