package ai.promoted.metrics.logprocessor.common.functions;

import static ai.promoted.metrics.logprocessor.common.util.FlinkFunctionUtil.validateRocksDBBackend;

import ai.promoted.metrics.logprocessor.common.functions.base.SerializableBiFunction;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableFunction;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A temporal join function that enriches the fact stream with a dimension stream. Will always join
 * the dimensional event whose timestamp is the largest among the ones that are less than or equal
 * to the timestamp of a fact event (i.e., the dimensional event should be the lasted one that
 * happens before or with the fact event). <br>
 * For late events, the function may generate some unpredictable joined results. Literally, it's
 * barely impossible to come up with a perfect solution to deal with the late events. <br>
 * Note: this function only works with RocksDB state backend since it assumes the keys for a map
 * state are ordered.
 */
public class TemporalJoinFunction<KEY, FACT, DIMENSION, OUT>
    extends KeyedCoProcessFunction<KEY, FACT, DIMENSION, OUT> {

  // Keep the dimension events for 3 extra hours.
  public static final Duration DIMENSION_ROWS_CLEAN_DELAY = Duration.ofMinutes(180);
  private static final Logger LOG = LogManager.getLogger(TemporalJoinFunction.class);
  private final TypeInformation<FACT> factTypeInformation;
  private final TypeInformation<DIMENSION> dimensionTypeInformation;
  private final SerializableFunction<FACT, OUT> leftOuterJoinFunction;
  private final SerializableBiFunction<FACT, DIMENSION, OUT> joinFunction;
  private final long firstDimensionAllowedDelay;
  private ValueState<Long> nextTimer;
  private MapState<Long, List<FACT>> factCache; // fact timestamp -> fact events
  private MapState<Long, DIMENSION> dimensionCache; // dimension timestamp -> dimensional event
  private final boolean eagerJoin; // Always try to join a fact event even it's late

  /**
   * @param leftOuterJoinFunction runs a left outer join (i.e., always output fact events) when
   *     offered
   */
  public TemporalJoinFunction(
      TypeInformation<FACT> factTypeInformation,
      TypeInformation<DIMENSION> dimensionTypeInformation,
      SerializableBiFunction<FACT, DIMENSION, OUT> joinFunction,
      boolean eagerJoin,
      @Nullable SerializableFunction<FACT, OUT> leftOuterJoinFunction,
      long dimensionAllowedDelay) {
    Preconditions.checkArgument(
        dimensionAllowedDelay >= 0, "The dimensionAllowedDelay should be non-negative!");
    this.factTypeInformation = factTypeInformation;
    this.dimensionTypeInformation = dimensionTypeInformation;
    this.leftOuterJoinFunction = leftOuterJoinFunction;
    this.joinFunction = joinFunction;
    this.eagerJoin = eagerJoin;
    this.firstDimensionAllowedDelay = dimensionAllowedDelay;
  }

  public TemporalJoinFunction(
      TypeInformation<FACT> factTypeInformation,
      TypeInformation<DIMENSION> dimensionTypeInformation,
      SerializableBiFunction<FACT, DIMENSION, OUT> joinFunction,
      boolean eagerJoin,
      @Nullable SerializableFunction<FACT, OUT> leftOuterJoinFunction) {
    this(
        factTypeInformation,
        dimensionTypeInformation,
        joinFunction,
        eagerJoin,
        leftOuterJoinFunction,
        0);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    validateRocksDBBackend(getRuntimeContext());
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
    nextTimer = getRuntimeContext().getState(new ValueStateDescriptor<>("next-timer", Types.LONG));
  }

  @Override
  public void processElement1(
      FACT factEvent,
      KeyedCoProcessFunction<KEY, FACT, DIMENSION, OUT>.Context context,
      Collector<OUT> collector)
      throws Exception {
    long eventTime = context.timestamp();
    if (eventTime <= context.timerService().currentWatermark()) {
      LOG.debug(
          "Received a late fact event {} with timestamp {} <= watermark {}",
          factEvent.toString(),
          eventTime,
          context.timerService().currentWatermark());
    }
    List<FACT> factEventList = factCache.get(context.timestamp());
    if (null == factEventList) {
      factEventList = new ArrayList<>();
    }
    factEventList.add(factEvent);
    factCache.put(eventTime, factEventList);
    if (null == nextTimer.value()) {
      long firstTimer = eventTime;
      // It's the first fact event
      if (dimensionCache.isEmpty()) {
        firstTimer += firstDimensionAllowedDelay;
      }
      context.timerService().registerEventTimeTimer(firstTimer);
      nextTimer.update(firstTimer);
    }
  }

  @Override
  public void processElement2(
      DIMENSION dimensionEvent,
      KeyedCoProcessFunction<KEY, FACT, DIMENSION, OUT>.Context context,
      Collector<OUT> collector)
      throws Exception {
    long eventTime = context.timestamp();
    if (eventTime <= context.timerService().currentWatermark()) {
      LOG.debug(
          "Received a late dimension event {} with timestamp {} <= watermark {}",
          dimensionEvent.toString(),
          eventTime,
          context.timerService().currentWatermark());
    }
    dimensionCache.put(eventTime, dimensionEvent);
  }

  @Override
  public void onTimer(
      long timestamp,
      KeyedCoProcessFunction<KEY, FACT, DIMENSION, OUT>.OnTimerContext ctx,
      Collector<OUT> out)
      throws Exception {
    long currentWatermark = ctx.timerService().currentWatermark();
    TimestampedCollector<OUT> timestampedCollector = (TimestampedCollector<OUT>) out;
    Iterator<Map.Entry<Long, List<FACT>>> joinedEventIterator = factCache.iterator();

    StatefulDimensionalEventsIterator<DIMENSION> statefulDimensionalEventsIterator =
        new StatefulDimensionalEventsIterator<>(dimensionCache.iterator(), currentWatermark);

    Map.Entry<Long, List<FACT>> factEventsEntry;
    while (joinedEventIterator.hasNext()) { // RocksDB map states are ordered by key.
      factEventsEntry = joinedEventIterator.next();
      if (factEventsEntry.getKey() > timestamp) {
        // Register another timer for the next fact event
        ctx.timerService().registerEventTimeTimer(factEventsEntry.getKey());
        nextTimer.update(factEventsEntry.getKey());
        break;
      }
      DIMENSION dimensionTarget =
          statefulDimensionalEventsIterator.nextVersionFor(factEventsEntry.getKey());
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
      } else {
        if (!dimensionCache.isEmpty()) {
          LOG.warn(
              "Failed to join fact events {} with timestamp {} <= watermark {}",
              factEventsEntry.getValue(),
              timestamp,
              ctx.timerService().currentWatermark());
        }
      }
      joinedEventIterator.remove();
    }
    if (nextTimer.value() == timestamp) {
      // if next timer is not updated, it means there's no more fact events
      nextTimer.update(null);
    }
    for (Long key : statefulDimensionalEventsIterator.outDatedVersions) {
      dimensionCache.remove(key);
    }
  }

  private class StatefulDimensionalEventsIterator<T> {
    private final Iterator<Map.Entry<Long, T>> iterator;
    private final long currentWatermark;
    private Tuple2<Long, T> currentVersion = null;
    private Tuple2<Long, T> nextVersion = null;
    private List<Long> outDatedVersions = new ArrayList<>();

    public StatefulDimensionalEventsIterator(
        Iterator<Map.Entry<Long, T>> iterator, long currentWatermark) {
      this.iterator = iterator;
      this.currentWatermark = currentWatermark;
    }

    /**
     * Return the next version of dimensional event whose timestamp is less than or equal to the
     * give timestamp. Could be repeatedly invoked with monotonically increasing timestamps. The
     * method will also collect outdated dimensional events that should be removed.
     */
    public T nextVersionFor(long timestamp) {
      if (null != currentVersion && currentVersion.f0 <= timestamp && nextVersion.f0 > timestamp) {
        return currentVersion.f1;
      }
      while (iterator.hasNext()) {
        currentVersion = nextVersion;
        Map.Entry<Long, T> entry = iterator.next();
        nextVersion = new Tuple2<>(entry.getKey(), entry.getValue());
        if (null != currentVersion) {
          if (currentVersion.f0
                  < timestamp
                      - TemporalJoinFunction.this.firstDimensionAllowedDelay
                      - DIMENSION_ROWS_CLEAN_DELAY.toMillis()
              && nextVersion.f0
                  < timestamp - TemporalJoinFunction.this.firstDimensionAllowedDelay) {
            // Remove the current version if there are newer versions and the timestamps for both
            // the current version and next version are less than the requested timestamp
            outDatedVersions.add(currentVersion.f0);
          }
        }
        if (nextVersion.f0 >= timestamp) {
          break;
        }
      }
      if (null == currentVersion) {
        // Allow the fact event to join with a delayed dimension event if it's the first one in the
        // dimension table.
        // Always return a dimension row if eagerJoin is enabled.
        if (null != nextVersion
            && (nextVersion.f0 <= timestamp + TemporalJoinFunction.this.firstDimensionAllowedDelay
                || eagerJoin)) {
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
