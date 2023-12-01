package ai.promoted.metrics.logprocessor.job.validateenrich;

import ai.promoted.proto.event.EnrichmentUnion;
import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * This is a temporary operator to delay EnrichmentUnion records for the watermark. This helps
 * synchronize records since we're not currently using EnrichUserInfo*. This will be removed when
 * those oeprators are added back.
 */
public class HoldUntilWatermark<K>
    extends KeyedProcessFunction<K, EnrichmentUnion, EnrichmentUnion> {
  // For the prototype, keep track of anonUserIds for 90d.
  @VisibleForTesting static final Duration TTL = Duration.ofDays(90);
  @VisibleForTesting transient MapState<Long, List<EnrichmentUnion>> timeToUnion;

  @Override
  public void open(Configuration config) throws Exception {
    super.open(config);
    timeToUnion =
        getRuntimeContext()
            .getMapState(
                new MapStateDescriptor<>(
                    "time-to-union",
                    Types.LONG,
                    Types.LIST(TypeInformation.of(EnrichmentUnion.class))));
  }

  @Override
  public void processElement(
      EnrichmentUnion union,
      KeyedProcessFunction<K, EnrichmentUnion, EnrichmentUnion>.Context ctx,
      Collector<EnrichmentUnion> out)
      throws Exception {
    // Delay events until the watermark.  This helps correct for reduce skew in backfills.
    // This is a little more expensive to run.
    long timestamp = ctx.timestamp();
    List<EnrichmentUnion> unions = timeToUnion.get(timestamp);
    if (unions == null) {
      // 4 is arbitrary.  Keep the list small.
      unions = new ArrayList<>(4);
      // Register the timer if none are already registered.
      ctx.timerService().registerEventTimeTimer(timestamp);
    }
    unions.add(union);
    timeToUnion.put(timestamp, unions);
  }

  @Override
  public void onTimer(
      long timestamp,
      KeyedProcessFunction<K, EnrichmentUnion, EnrichmentUnion>.OnTimerContext ctx,
      Collector<EnrichmentUnion> out)
      throws Exception {
    List<EnrichmentUnion> unions = timeToUnion.get(timestamp);
    if (unions != null) {
      for (EnrichmentUnion union : unions) {
        out.collect(union);
      }
      timeToUnion.remove(timestamp);
    }
  }
}
