package ai.promoted.metrics.logprocessor.common.flink.operator;

import ai.promoted.metrics.logprocessor.common.functions.LogSlowOnTimer;
import ai.promoted.metrics.logprocessor.common.functions.inferred.BaseInferred;
import ai.promoted.proto.event.TinyEvent;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Flink operator primarily to allow for inference delays.
 *
 * Also allows us to mess w/ operator internals.  See the git commit history for this file to see
 * examples.
 */
public class InferenceOperator<RHS extends GeneratedMessageV3>
        extends KeyedCoProcessOperatorWithWatermarkDelay<Tuple2<Long, String>, TinyEvent, RHS, TinyEvent> {
    private static final Logger LOGGER = LogManager.getLogger(InferenceOperator.class);

    private final String name;

    public static <RHS extends GeneratedMessageV3> InferenceOperator<RHS> of(BaseInferred<RHS> inferenceProcessFunction, boolean textLogWatermark) {
        return new InferenceOperator<>(inferenceProcessFunction, textLogWatermark);
    }

    private InferenceOperator(BaseInferred<RHS> inferenceProcessFunction, boolean textLogWatermark) {
        super(new LogSlowOnTimer<>(inferenceProcessFunction), inferenceProcessFunction.getWatermarkDelay(), textLogWatermark);
        name = inferenceProcessFunction.getName();
    }

    @Override
    public void processWatermark1(Watermark mark) throws Exception {
        LOGGER.trace("{} processWatermark1: {}", name, mark.getTimestamp());
        super.processWatermark1(mark);
    }
    @Override
    public void processWatermark2(Watermark mark) throws Exception {
        LOGGER.trace("{} processWatermark2: {}", name, mark.getTimestamp());
        super.processWatermark2(mark);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        LOGGER.trace("{} processWatermark: {}", name, mark.getTimestamp());
        super.processWatermark(mark);
    }

    // TODO(jin): flink's AbstractStreamOperator doesn't actually use this for now.
    @Override
    public void processWatermarkStatus(WatermarkStatus status) throws Exception {
        LOGGER.trace("{} processWatermarkStatus: {}", name, status);
        super.processWatermarkStatus(status);
    }
}
