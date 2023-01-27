package ai.promoted.metrics.logprocessor.common.functions;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Used to de-dupe rows with the same key in the stateRetentionTime period.
 */
public class KeepFirstRow<K, T> extends KeyedProcessFunction<K, T, T> {

    private transient ValueState<Boolean> alreadyOutputted;
    private final String stateName;
    private final Duration stateRetentionDuration;

    public KeepFirstRow(String stateLabel, Duration stateRetentionDuration) {
        this.stateName = "outputted-" + stateLabel;
        this.stateRetentionDuration = stateRetentionDuration;
    }

    @Override
    public void processElement(T value, KeyedProcessFunction<K, T, T>.Context ctx, Collector<T> out) throws Exception {
        // Since value can be null.
        if (alreadyOutputted.value() != Boolean.TRUE) {
            alreadyOutputted.update(Boolean.TRUE);
            ctx.timerService().registerEventTimeTimer(ctx.timestamp() + stateRetentionDuration.toMillis());
            out.collect(value);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<T> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        alreadyOutputted.clear();
    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>(
                stateName,
                TypeInformation.of(new TypeHint<Boolean>() {}));
        alreadyOutputted = getRuntimeContext().getState(descriptor);
    }
}
