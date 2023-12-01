package ai.promoted.metrics.logprocessor.common.job;

import com.google.protobuf.GeneratedMessageV3;
import com.twitter.chill.protobuf.ProtobufSerializer;
import java.util.Set;
import org.apache.flink.api.common.ExecutionConfig;

/** Interface to define a modular segment of a Flink pipeline. */
public interface FlinkSegment {

  // TODO - auto-generate this.
  /** Registers a Protobuf serializer in a Flink ExecutionConfig. */
  static <T extends GeneratedMessageV3> void optionalRegisterProtobufSerializer(
      ExecutionConfig config, Class<T> clazz) {
    if (clazz != null) {
      config.registerTypeWithKryoSerializer(clazz, ProtobufSerializer.class);
    }
  }

  /** Returns a list of Proto classes that need to be registered with Kryo. */
  Set<Class<? extends GeneratedMessageV3>> getProtoClasses();

  /**
   * Make sure the command line args are valid. Log/throw whatever you need to here for any cli
   * argument consistency you need. IMPORTANT: call all ancestor and interface validateArgs to
   * ensure correct behavior.
   */
  // TODO: consider using reflection to ensure all segments are validated.
  void validateArgs();
}
