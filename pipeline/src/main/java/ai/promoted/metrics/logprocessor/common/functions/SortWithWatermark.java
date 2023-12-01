package ai.promoted.metrics.logprocessor.common.functions;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Holds the incoming events until the watermark and then outputs them in temporal order (with
 * RocksDB MapState). Used to sort events before writing them to Kafka. The key of this function
 * should be identical to the Kafka record key so that no data shuffling happens when writing
 * records to Kafka brokers.
 */
public class SortWithWatermark<T>
    extends KeyedProcessFunction<SortWithWatermark.ByteArrayKey, T, Tuple2<byte[], T>> {
  private static final Logger LOG = LogManager.getLogger(SortWithWatermark.class);
  @VisibleForTesting transient MapState<Long, List<T>> timeToEvents;
  @VisibleForTesting transient ValueState<Long> nextTimer;
  private final TypeInformation<T> inputTypeInfo;
  private final boolean sideOutputLateEvent;
  private final OutputTag<T> lateEventTag;

  public SortWithWatermark(
      TypeInformation<T> inputTypeInfo, boolean sideOutputLateEvent, OutputTag<T> lateEventTag) {
    this.inputTypeInfo = inputTypeInfo;
    this.sideOutputLateEvent = sideOutputLateEvent;
    this.lateEventTag = lateEventTag;
  }

  @Override
  public void open(Configuration config) throws Exception {
    super.open(config);
    timeToEvents =
        getRuntimeContext()
            .getMapState(
                new MapStateDescriptor<>("time-to-array", Types.LONG, Types.LIST(inputTypeInfo)));
    nextTimer = getRuntimeContext().getState(new ValueStateDescriptor<>("next-timer", Types.LONG));
  }

  @Override
  public void processElement(
      T event,
      KeyedProcessFunction<ByteArrayKey, T, Tuple2<byte[], T>>.Context ctx,
      Collector<Tuple2<byte[], T>> out)
      throws Exception {
    if (ctx.timestamp() <= ctx.timerService().currentWatermark()) {
      if (sideOutputLateEvent) {
        ctx.output(lateEventTag, event);
      } else {
        // Simply output the late event.
        LOG.debug(() -> "Output an late event " + Tuple2.of(ctx.getCurrentKey(), event));
        out.collect(Tuple2.of(ctx.getCurrentKey().byteArray, event));
      }
    } else {
      long timestamp = ctx.timestamp();
      List<T> list = timeToEvents.get(timestamp);
      if (list == null) {
        list = new ArrayList<>();
        Long nextTimerValue = nextTimer.value();
        if (null == nextTimerValue || timestamp < nextTimerValue) {
          // Update to an earlier time.
          ctx.timerService().registerEventTimeTimer(timestamp);
          if (null != nextTimerValue) {
            ctx.timerService().deleteEventTimeTimer(nextTimerValue);
          }
          nextTimer.update(timestamp);
        }
      }
      list.add(event);
      timeToEvents.put(timestamp, list);
    }
  }

  @Override
  public void onTimer(
      long timestamp,
      KeyedProcessFunction<ByteArrayKey, T, Tuple2<byte[], T>>.OnTimerContext ctx,
      Collector<Tuple2<byte[], T>> out)
      throws Exception {
    TimestampedCollector<Tuple2<byte[], T>> collector =
        (TimestampedCollector<Tuple2<byte[], T>>) out;
    Iterator<Map.Entry<Long, List<T>>> iterator = timeToEvents.iterator();
    Long nextTimestamp = null;
    while (iterator.hasNext()) {
      // Output all events whose timestamp is less than the watermark.
      Map.Entry<Long, List<T>> entry = iterator.next();
      if (entry.getKey() <= ctx.timerService().currentWatermark()) {
        for (T event : entry.getValue()) {
          // Force set the timestamp to be the record's timestamp instead of the timer's timestamp.
          collector.setAbsoluteTimestamp(entry.getKey());
          collector.collect(Tuple2.of(ctx.getCurrentKey().byteArray, event));
        }
        iterator.remove();
      } else {
        // There are more events in the map.
        nextTimestamp = entry.getKey();
        break;
      }
    }
    if (null != nextTimestamp) {
      ctx.timerService().registerEventTimeTimer(nextTimestamp);
      nextTimer.update(nextTimestamp);
    } else {
      nextTimer.update(null);
    }
  }

  public static class ByteArrayKey {

    public ByteArrayKey() {}

    public byte[] byteArray;

    public ByteArrayKey(byte[] byteArray) {
      this.byteArray = byteArray;
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(byteArray);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof byte[]) {
        return Arrays.equals((byte[]) obj, byteArray);
      } else {
        return false;
      }
    }

    @Override
    public String toString() {
      return Arrays.toString(byteArray);
    }
  }
}
