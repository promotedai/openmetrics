package ai.promoted.metrics.logprocessor.common.job;

import ai.promoted.metrics.logprocessor.common.functions.KeepFirstRow;
import ai.promoted.metrics.logprocessor.common.functions.KeyUtil;
import ai.promoted.proto.event.Impression;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.GeneratedMessageV3;
import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import picocli.CommandLine;

/**
 * A FlinkSegment that provides impressions as a source. It's a separate FlinkSegment to isolate the
 * flags to just jobs that use the deduplication source.
 */
public class RawImpressionSegment implements FlinkSegment {

  @CommandLine.Option(
      names = {"--keepFirstImpressionDuration"},
      defaultValue = "P1D",
      description =
          "The duration to keep track of recent Impressions.  This is used to de-duplicate raw inputs.  Default=P1D to match flat_response_insertion join window.  Java8 Duration parse format.")
  public Duration keepFirstImpressionDuration = Duration.parse("P1D");

  private final BaseFlinkJob baseFlinkJob;

  public RawImpressionSegment(BaseFlinkJob baseFlinkJob) {
    this.baseFlinkJob = baseFlinkJob;
  }

  @Override
  public void validateArgs() {
    // Do nothing.
  }

  public DataStream<Impression> getDeduplicatedImpression(
      DataStream<Impression> impressionDataStream) {
    return baseFlinkJob.add(
        impressionDataStream
            .keyBy(KeyUtil.impressionKeySelector)
            .process(
                new KeepFirstRow<>("impression", keepFirstImpressionDuration),
                TypeInformation.of(Impression.class)),
        "keep-first-impression");
  }

  @Override
  public List<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    return ImmutableList.of(Impression.class);
  }
}
