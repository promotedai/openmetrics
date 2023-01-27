package ai.promoted.metrics.logprocessor.common.functions;

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Clock;
import java.time.Duration;

/**
 * A KeyedCoProcessFunction that logs if the timer is too slow.
 */
public class LogSlowOnTimer<K, IN1, IN2, OUT>  extends KeyedCoProcessFunction<K, IN1, IN2, OUT> {
    private static final Logger LOGGER = LogManager.getLogger(LogSlowOnTimer.class);

    private Duration ON_TIMER_SLOW_THRESHOLD = Duration.ofMinutes(10);

    private KeyedCoProcessFunction<K, IN1, IN2, OUT> delegate;
    private Clock clock;

    public LogSlowOnTimer(KeyedCoProcessFunction<K, IN1, IN2, OUT> delegate) {
        this(delegate, Clock.systemUTC());
    }

    public LogSlowOnTimer(KeyedCoProcessFunction<K, IN1, IN2, OUT> delegate, Clock clock) {
        this.delegate = delegate;
        this.clock = clock;
    }

    @Override
    public void processElement1(IN1 var1, KeyedCoProcessFunction<K, IN1, IN2, OUT>.Context var2, Collector<OUT> var3) throws Exception {
        delegate.processElement1(var1, var2, var3);
    }

    @Override
    public void processElement2(IN2 var1, KeyedCoProcessFunction<K, IN1, IN2, OUT>.Context var2, Collector<OUT> var3) throws Exception {
        delegate.processElement2(var1, var2, var3);
    }

    @Override
    public void onTimer(long timestamp, KeyedCoProcessFunction<K, IN1, IN2, OUT>.OnTimerContext ctx, Collector<OUT> out) throws Exception {
        long start = clock.millis();
        delegate.onTimer(timestamp, ctx, out);
        long end = clock.millis();
        Duration duration = Duration.ofMillis(end - start);
        if (duration.compareTo(ON_TIMER_SLOW_THRESHOLD) > 0) {
            LOGGER.warn("{} is slower ({}) than expected ({}).", delegate, duration, ON_TIMER_SLOW_THRESHOLD);
        }
    }

    @Override
    public void setRuntimeContext(RuntimeContext t) {
        delegate.setRuntimeContext(t);
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        return delegate.getRuntimeContext();
    }

    @Override
    public IterationRuntimeContext getIterationRuntimeContext() {
        return delegate.getIterationRuntimeContext();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        delegate.open(parameters);
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }
}
