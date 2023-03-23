package ai.promoted.metrics.logprocessor.common.functions;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.runtime.state.DefaultKeyedStateStore;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

/**
 * A temporal join function that enriches the fact stream with a dimension stream. Will always join
 * the dimensional event whose timestamp is the largest among the ones that are less than or equal
 * to the timestamp of a fact event (i.e., the dimensional event should be the lasted one that
 * happens before or with the fact event). <br>
 * Note: this function only works with RocksDB state backend since it assumes the keys for a map
 * state are ordered.
 */
public class TemporalJoinFunction<KEY, FACT, DIMENSION, OUT>
    extends KeyedCoProcessFunction<KEY, FACT, DIMENSION, OUT> {
  private final TypeInformation<FACT> factTypeInformation;
  private final TypeInformation<DIMENSION> dimensionTypeInformation;
  private final SerializableFunction<FACT, OUT> leftOuterJoinFunction;
  private final SerializableBiFunction<FACT, DIMENSION, OUT> joinFunction;
  private MapState<Long, List<FACT>> factCache; // fact timestamp -> fact events
  private MapState<Long, DIMENSION> dimensionCache; // dimension timestamp -> dimensional event

  /**
   * @param leftOuterJoinFunction runs a left outer join (i.e., always output fact events) when
   *     offered
   */
  public TemporalJoinFunction(
      TypeInformation<FACT> factTypeInformation,
      TypeInformation<DIMENSION> dimensionTypeInformation,
      SerializableBiFunction<FACT, DIMENSION, OUT> joinFunction,
      @Nullable SerializableFunction<FACT, OUT> leftOuterJoinFunction) {
    this.factTypeInformation = factTypeInformation;
    this.dimensionTypeInformation = dimensionTypeInformation;
    this.leftOuterJoinFunction = leftOuterJoinFunction;
    this.joinFunction = joinFunction;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    validateRocksDBBackend();
    dimensionCache =
        getRuntimeContext()
            .getMapState(
                new MapStateDescriptor<>(
                    "temporal-join-dimension-events-cache", Types.LONG, dimensionTypeInformation));
    factCache =
        getRuntimeContext()
            .getMapState(
                new MapStateDescriptor<>(
                    "temporal-join-fact-events-cache",
                    Types.LONG,
                    Types.LIST(factTypeInformation)));
  }

  /** Uses reflection to verify that RocksDB state backend is used. */
  private void validateRocksDBBackend() throws NoSuchFieldException, IllegalAccessException {
    Field keyedStateStoreField = StreamingRuntimeContext.class.getDeclaredField("keyedStateStore");
    keyedStateStoreField.setAccessible(true);
    KeyedStateStore stateStore = (KeyedStateStore) keyedStateStoreField.get(getRuntimeContext());
    Field stateBackendField = DefaultKeyedStateStore.class.getDeclaredField("keyedStateBackend");
    stateBackendField.setAccessible(true);
    Preconditions.checkArgument(
        (stateBackendField.get(stateStore) instanceof RocksDBKeyedStateBackend),
        "For efficiency, temporalJoinFunction only works for RocksDB backend.");
  }

  @Override
  public void processElement1(
      FACT factEvent,
      KeyedCoProcessFunction<KEY, FACT, DIMENSION, OUT>.Context context,
      Collector<OUT> collector)
      throws Exception {
    List<FACT> factEventList = factCache.get(context.timestamp());
    if (null == factEventList) {
      factEventList = new ArrayList<>();
    }
    factEventList.add(factEvent);
    factCache.put(context.timestamp(), factEventList);
    // Register a timer to trigger join
    // TODO introduce latency and share timers to reduce timer state
    context.timerService().registerEventTimeTimer(context.timestamp());
  }

  @Override
  public void processElement2(
      DIMENSION dimensionEvent,
      KeyedCoProcessFunction<KEY, FACT, DIMENSION, OUT>.Context context,
      Collector<OUT> collector)
      throws Exception {
    dimensionCache.put(context.timestamp(), dimensionEvent);
  }

  @Override
  public void onTimer(
      long timestamp,
      KeyedCoProcessFunction<KEY, FACT, DIMENSION, OUT>.OnTimerContext ctx,
      Collector<OUT> out)
      throws Exception {
    super.onTimer(timestamp, ctx, out);
    long currentWatermark = ctx.timerService().currentWatermark();
    TimestampedCollector<OUT> timestampedCollector = (TimestampedCollector<OUT>) out;
    Iterator<Map.Entry<Long, List<FACT>>> joinedEventIterator = factCache.iterator();

    StatefulDimensionalEventsIterator<DIMENSION> statefulDimensionalEventsIterator =
        new StatefulDimensionalEventsIterator<>(dimensionCache.iterator(), currentWatermark);

    Map.Entry<Long, List<FACT>> factEventsEntry;
    while (joinedEventIterator.hasNext()) { // RocksDB map states are ordered by key.
      factEventsEntry = joinedEventIterator.next();
      if (factEventsEntry.getKey() > currentWatermark) {
        // Will be triggered by another timer in the future.
        break;
      }
      final long eventTime = factEventsEntry.getKey();
      DIMENSION dimensionTarget =
          statefulDimensionalEventsIterator.nextVersionFor(factEventsEntry.getKey());
      // Set the output StreamRecord's timestamp to be the timestamp of the fact event
      timestampedCollector.setAbsoluteTimestamp(eventTime);
      if (null != dimensionTarget) {
        // Found the dimensional event. Join and output.
        factEventsEntry
            .getValue()
            .forEach(
                factEvent -> {
                  timestampedCollector.collect(joinFunction.apply(factEvent, dimensionTarget));
                });
      } else if (null != leftOuterJoinFunction) {
        // Failed to get the dimensional event. Just output the fact events (left-outer semantics).
        factEventsEntry
            .getValue()
            .forEach(
                factEvent -> {
                  timestampedCollector.collect(leftOuterJoinFunction.apply(factEvent));
                });
      }
      joinedEventIterator.remove();
    }
  }

  private static class StatefulDimensionalEventsIterator<T> {
    private final Iterator<Map.Entry<Long, T>> iterator;
    private final long currentWatermark;
    private Tuple2<Long, T> currentVersion = null;
    private Tuple2<Long, T> nextVersion = null;

    public StatefulDimensionalEventsIterator(
        Iterator<Map.Entry<Long, T>> iterator, long currentWatermark) {
      this.iterator = iterator;
      this.currentWatermark = currentWatermark;
    }

    /**
     * Return the next version of dimensional event whose timestamp is less than or equal to the
     * give timestamp. Could be repeatedly invoked with monotonically increasing timestamps. The
     * method will also remove outdated dimensional events.
     */
    public T nextVersionFor(long timestamp) {
      if (null != currentVersion && currentVersion.f0 <= timestamp && nextVersion.f0 > timestamp) {
        return currentVersion.f1;
      }
      while (iterator.hasNext()) {
        currentVersion = nextVersion;
        Map.Entry<Long, T> entry = iterator.next();
        nextVersion = new Tuple2<>(entry.getKey(), entry.getValue());
        if (nextVersion.f0 <= currentWatermark && iterator.hasNext()) {
          // Remove if there are newer version and the current version's timestamp is less
          // than the current watermark
          iterator.remove();
        }
        if (nextVersion.f0 >= timestamp) {
          break;
        }
      }
      if (null == currentVersion) {
        if (null != nextVersion && nextVersion.f0 <= timestamp) {
          return nextVersion.f1;
        }
        return null;
      } else { // null != currentVersion
        if (nextVersion.f0 <= timestamp) {
          return nextVersion.f1;
        } else {
          return currentVersion.f1;
        }
      }
    }
  }
}
