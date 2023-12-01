package ai.promoted.metrics.logprocessor.common.job;

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.GeneratedMessageV3;
import java.util.Set;
import picocli.CommandLine.Option;

/** A Segment for getting a shared region flag. */
public class RegionSegment implements FlinkSegment {
  @Option(
      names = {"--region"},
      defaultValue = "",
      description = "The AWS Region. Default=empty string.")
  public String region = "";

  public RegionSegment() {}

  @Override
  public void validateArgs() {}

  @Override
  public Set<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    return ImmutableSet.of();
  }
}
