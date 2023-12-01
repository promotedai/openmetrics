package ai.promoted.metrics.logprocessor.common.job;

import ai.promoted.metrics.error.MismatchError;
import com.google.auto.value.AutoValue;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * Small wrapper so we can return multiple outputs from a function that creates MergeDetails Flink
 * operators.
 */
@AutoValue
public abstract class MergeDetailsOutputs<T> {
  public abstract SingleOutputStreamOperator<T> joinedEvents();

  public abstract DataStream<MismatchError> mismatchErrors();

  public static <T> MergeDetailsOutputs<T> create(
      SingleOutputStreamOperator<T> joinedEvents, DataStream<MismatchError> mismatchErrors) {
    return new AutoValue_MergeDetailsOutputs(joinedEvents, mismatchErrors);
  }
}
