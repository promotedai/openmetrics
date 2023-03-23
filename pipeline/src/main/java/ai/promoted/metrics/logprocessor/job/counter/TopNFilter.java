package ai.promoted.metrics.logprocessor.job.counter;

import ai.promoted.metrics.logprocessor.common.functions.SerializableFunction;
import java.time.Duration;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TopNFilter<B, K, V>
    extends KeyedBroadcastProcessFunction<K, V, Tuple2<B, Boolean>, V> {
  private static final Logger LOGGER = LogManager.getLogger(TopNFilter.class);
  private static final long DELAY_WINDOW_MS = Duration.ofSeconds(5).toMillis();

  private final MapStateDescriptor<B, Boolean> broadcastDescriptor;
  private final TypeInformation<V> valueTypeInfo;
  private final SerializableFunction<V, B> valueToIdFunction;

  private ListState<V> delayedInput;

  public TopNFilter(
      MapStateDescriptor<B, Boolean> broadcastDescriptor,
      TypeInformation<V> valueTypeInfo,
      SerializableFunction<V, B> valueToIdFunction) {
    this.broadcastDescriptor = broadcastDescriptor;
    this.valueTypeInfo = valueTypeInfo;
    this.valueToIdFunction = valueToIdFunction;
  }

  @Override
  public void open(Configuration config) {
    delayedInput =
        getRuntimeContext().getListState(new ListStateDescriptor<>("delayed-input", valueTypeInfo));
  }

  @Override
  public void processElement(V in, ReadOnlyContext rCtx, Collector<V> out) throws Exception {
    B id = valueToIdFunction.apply(in);
    if (rCtx.getBroadcastState(broadcastDescriptor).contains(id)) {
      out.collect(in);
    } else {
      // There could be a miss b/c the broadcast state may be delayed.
      // It's not bad to delay something that's not in the descriptor as it's not in the topn.
      delayedInput.add(in);
      rCtx.timerService().registerEventTimeTimer(toBucketedTime(rCtx.timestamp()));
    }
  }

  // Buckets timers to reduce the number of timers.
  private long toBucketedTime(long timestamp) {
    return timestamp + 2 * DELAY_WINDOW_MS + (timestamp % DELAY_WINDOW_MS);
  }

  @Override
  public void onTimer(long ts, OnTimerContext ctx, Collector<V> out) throws Exception {
    for (V e : delayedInput.get()) {
      B id = valueToIdFunction.apply(e);
      if (ctx.getBroadcastState(broadcastDescriptor).contains(id)) {
        out.collect(e);
      } else {
        // TODO: add side output to log filtered output
        LOGGER.trace("filtered {}", e);
      }
    }
    delayedInput.clear();
  }

  @Override
  public void processBroadcastElement(Tuple2<B, Boolean> in, Context ctx, Collector<V> out)
      throws Exception {
    BroadcastState<B, Boolean> bcState = ctx.getBroadcastState(broadcastDescriptor);
    LOGGER.trace("broadcasted {} {}", in.f0, in.f1);
    if (in.f1) {
      bcState.put(in.f0, in.f1);
    } else if (bcState.contains(in.f0)) {
      bcState.remove(in.f0);
    }
  }
}
