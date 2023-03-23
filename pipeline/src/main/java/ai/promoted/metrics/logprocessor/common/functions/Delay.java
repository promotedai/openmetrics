package ai.promoted.metrics.logprocessor.common.functions;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Delays a stream of events by a duration. This is currently used as a hack so we can delay some
 * synthetic User records for real ones. I hope we can remove this after changing the User DB join
 * structure.
 */
public class Delay<T> extends KeyedProcessFunction<Tuple2<String, Long>, T, T> {

  private final String stateLabel;
  private final Duration delayDuration;
  private final Class<T> valueClass;
  private MapState<Long, List<T>> timestampToList;

  public Delay(String stateLabel, Duration delayDuration, Class<T> valueClass) {
    this.stateLabel = stateLabel;
    this.delayDuration = delayDuration;
    this.valueClass = valueClass;
  }

  @Override
  public void processElement(T input, Context ctx, Collector<T> collector) throws Exception {
    Long futureTime = ctx.timestamp() + delayDuration.toMillis();
    List<T> values = timestampToList.get(futureTime);
    if (values == null) {
      // Keep the capacity small since the initial uses will be small.
      values = new ArrayList<>();
    }
    values.add(input);
    timestampToList.put(futureTime, values);
    ctx.timerService().registerEventTimeTimer(futureTime);
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<T> out) throws Exception {
    super.onTimer(timestamp, ctx, out);
    List<T> values = timestampToList.get(timestamp);
    values.forEach(out::collect);
    timestampToList.remove(timestamp);
  }

  @Override
  public void open(Configuration config) throws Exception {
    super.open(config);
    timestampToList =
        getRuntimeContext()
            .getMapState(
                // TODO(PRO-1683) - add caches back in.
                new MapStateDescriptor<>(
                    stateLabel + "-delay", Types.LONG, Types.LIST(TypeInformation.of(valueClass))));
  }
}
