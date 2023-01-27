package ai.promoted.metrics.logprocessor.common.job;

import ai.promoted.metrics.error.MismatchError;
import ai.promoted.proto.event.JoinedEvent;
import com.google.auto.value.AutoValue;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * Small wrapper so we can return multiple outputs from a function that creates MergeDetails Flink operators.
 */
@AutoValue
public abstract class MergeDetailsOutputs {
    public abstract SingleOutputStreamOperator<JoinedEvent> joinedEvent();
    public abstract DataStream<MismatchError> mismatchErrors();

    public static MergeDetailsOutputs create(
            SingleOutputStreamOperator<JoinedEvent> joinedEvent,
            DataStream<MismatchError> mismatchErrors) {
        return new AutoValue_MergeDetailsOutputs(joinedEvent, mismatchErrors);
    }
}
