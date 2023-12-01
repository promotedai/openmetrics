package ai.promoted.metrics.logprocessor.common.job;

import ai.promoted.metrics.logprocessor.common.functions.KeepFirstRow;
import ai.promoted.metrics.logprocessor.common.functions.KeyUtil;
import ai.promoted.proto.event.AttributedAction;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.GeneratedMessageV3;
import java.time.Duration;
import java.util.Set;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import picocli.CommandLine;

/**
 * A FlinkSegment that provides joined actions as a source. It's a separate FlinkSegment to isolate
 * the flags to just jobs that use the deduplication source.
 */
public class AttributedActionSegment implements FlinkSegment {

  private final BaseFlinkJob baseFlinkJob;

  @CommandLine.Option(
      names = {"--keepFirstAttributedActionDuration"},
      defaultValue = "PT6H",
      description =
          "The duration to keep track of recent AttributedActions.  This is used to de-duplicate joined inputs.  Default=PT6H.  Java8 Duration parse format.")
  public Duration keepFirstAttributedActionDuration = Duration.parse("PT6H");

  public AttributedActionSegment(BaseFlinkJob baseFlinkJob) {
    this.baseFlinkJob = baseFlinkJob;
  }

  @Override
  public void validateArgs() {
    // Do nothing.
  }

  public DataStream<AttributedAction> getDeduplicatedAttributedAction(
      DataStream<AttributedAction> attributedAction) {
    return baseFlinkJob.add(
        attributedAction
            .keyBy(KeyUtil.attributedActionKey)
            .process(
                new KeepFirstRow<>("attributed-action", keepFirstAttributedActionDuration),
                TypeInformation.of(AttributedAction.class)),
        "keep-first-attributed-action");
  }

  @Override
  public Set<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    return ImmutableSet.of(AttributedAction.class);
  }
}
