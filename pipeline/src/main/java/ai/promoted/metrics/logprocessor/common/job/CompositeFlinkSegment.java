package ai.promoted.metrics.logprocessor.common.job;

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.GeneratedMessageV3;
import java.util.Set;

/** A {@code Composite} for {@code FlinkSegment}. */
public interface CompositeFlinkSegment extends FlinkSegment {

  /** Returns a set of inner FlinkSegments. Not ones passed into constructors. */
  Set<FlinkSegment> getInnerFlinkSegments();

  @Override
  default Set<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    ImmutableSet.Builder<Class<? extends GeneratedMessageV3>> protoClasses = ImmutableSet.builder();
    for (FlinkSegment flinkSegment : getInnerFlinkSegments()) {
      protoClasses.addAll(flinkSegment.getProtoClasses());
    }
    return protoClasses.build();
  }

  @Override
  default void validateArgs() {
    for (FlinkSegment flinkSegment : getInnerFlinkSegments()) {
      flinkSegment.validateArgs();
    }
  }
}
