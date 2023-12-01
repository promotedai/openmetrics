package ai.promoted.metrics.logprocessor.common.queryablestate;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.flink.api.common.typeutils.TypeSerializer;

@AutoValue
public abstract class OperatorInfo implements Serializable {
  public static OperatorInfo create(
      TypeSerializer<?> keySerializer, int parallelism, int maxParallelism) {
    return new AutoValue_OperatorInfo(keySerializer, parallelism, maxParallelism);
  }

  public abstract TypeSerializer<?> getKeySerializer();

  public abstract int getParallelism();

  public abstract int getMaxParallelism();
}
