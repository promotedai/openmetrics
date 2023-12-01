package ai.promoted.metrics.logprocessor.job.validateenrich;

import ai.promoted.metrics.logprocessor.common.util.FlinkFunctionUtil;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.EnrichmentUnion;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

/**
 * Joins retainedUserId onto {@code EnrichmentUnion}'s UserInfo. Once we encounter a retainedUserId,
 * that's used until a DELETE Row is sent (and state is cleared).
 */
public class JoinRetainedUserId
    extends KeyedCoProcessFunction<
        AuthUserIdKey, EnrichmentUnion, SelectRetainedUserChange, EnrichmentUnion> {
  // Used to delay outputs before the retainedUserId is generated to have more consistent outputs.
  // Delays output and assignment until the onTimer (key milliseconds).
  @VisibleForTesting transient MapState<Long, List<EnrichmentUnion>> timeToDelayedUnions;
  @VisibleForTesting transient ValueState<String> retainedUserId;
  //
  @VisibleForTesting transient ValueState<Long> outputTime;
  // Indicates to clean up in onTimer.
  @VisibleForTesting transient ValueState<Boolean> cleanUp;

  @Override
  public void open(Configuration config) throws Exception {
    super.open(config);
    FlinkFunctionUtil.validateRocksDBBackend(getRuntimeContext());
    timeToDelayedUnions =
        getRuntimeContext()
            .getMapState(
                new MapStateDescriptor<>(
                    "delayedUnions",
                    Types.LONG,
                    Types.LIST(TypeInformation.of(EnrichmentUnion.class))));
    retainedUserId =
        getRuntimeContext().getState(new ValueStateDescriptor<>("retainedUserId", Types.STRING));
    outputTime = getRuntimeContext().getState(new ValueStateDescriptor<>("outputTime", Types.LONG));
    cleanUp = getRuntimeContext().getState(new ValueStateDescriptor<>("cleanUp", Types.BOOLEAN));
  }

  @Override
  public void processElement1(
      EnrichmentUnion union,
      KeyedCoProcessFunction<
                  AuthUserIdKey, EnrichmentUnion, SelectRetainedUserChange, EnrichmentUnion>
              .Context
          ctx,
      Collector<EnrichmentUnion> out)
      throws Exception {
    UserInfo userInfo = EnrichmentUnionUtil.getUserInfo(union);
    // This means there is a bad, obvious bug in the previous operators.
    Preconditions.checkArgument(!userInfo.getUserId().isEmpty(), "userId needs to be specified");
    String retainedUserIdValue = retainedUserId.value();
    if (retainedUserIdValue == null || retainedUserIdValue.isEmpty()) {

      // Save for watermark.
      long inputTime = ctx.timestamp();
      List<EnrichmentUnion> delayedUnions = timeToDelayedUnions.get(inputTime);
      if (delayedUnions == null) {
        delayedUnions = new ArrayList<>();
      }
      delayedUnions.add(union);
      timeToDelayedUnions.put(inputTime, delayedUnions);
      Long previousOutputTimeValue = outputTime.value();
      if (previousOutputTimeValue == null || inputTime < previousOutputTimeValue) {
        if (previousOutputTimeValue != null) {
          ctx.timerService().deleteEventTimeTimer(previousOutputTimeValue);
        }
        outputTime.update(inputTime);
        ctx.timerService().registerEventTimeTimer(inputTime);
      }
    } else {
      Preconditions.checkState(
          !retainedUserIdValue.isEmpty(), "retainedUserId should not be an empty string");
      out.collect(setRetainedUserId(union, retainedUserIdValue));
    }
  }

  @Override
  public void processElement2(
      SelectRetainedUserChange retainedUserRow,
      KeyedCoProcessFunction<
                  AuthUserIdKey, EnrichmentUnion, SelectRetainedUserChange, EnrichmentUnion>
              .Context
          ctx,
      Collector<EnrichmentUnion> out)
      throws Exception {
    switch (retainedUserRow.getKind()) {
      case UPSERT:
        // TODO - support upserts accurately.  This doesn't handle out of order upserts.
        retainedUserId.update(retainedUserRow.getRetainedUserId());
        return;
      case DELETE:
        // Clean up state in onTimer in case we encounter any out of order events.
        cleanUp.update(true);
        // PR - should we rely on the `ctx.timestamp()`?  Or should we put use a timestamp in the
        // message?
        ctx.timerService().registerEventTimeTimer(ctx.timestamp());
        return;
      default:
        throw new UnsupportedOperationException(retainedUserRow.getKind().toString());
    }
  }

  @Override
  public void onTimer(
      long timestamp,
      KeyedCoProcessFunction<
                  AuthUserIdKey, EnrichmentUnion, SelectRetainedUserChange, EnrichmentUnion>
              .OnTimerContext
          ctx,
      Collector<EnrichmentUnion> out)
      throws Exception {

    // Prefer outputting the delayed events on the same timer as the input's event time.
    // We have some watermark issue where the timer fires before we get retainedUserId.
    String retainedUserIdValue = retainedUserId.value();
    List<EnrichmentUnion> unions = timeToDelayedUnions.get(timestamp);
    if (unions != null) {
      for (EnrichmentUnion union : unions) {
        out.collect(setRetainedUserId(union, retainedUserIdValue));
      }
      timeToDelayedUnions.remove(timestamp);
      Iterator<Entry<Long, List<EnrichmentUnion>>> iterator = timeToDelayedUnions.iterator();
      if (iterator.hasNext()) {
        long nextTime = iterator.next().getKey();
        ctx.timerService().registerEventTimeTimer(nextTime);
      }
    }

    Boolean cleanUpValue = cleanUp.value();
    if (cleanUpValue == Boolean.TRUE) {
      retainedUserId.clear();
      outputTime.clear();
      cleanUp.clear();
    }
  }

  private static EnrichmentUnion setRetainedUserId(EnrichmentUnion union, String retainedUserId) {
    UserInfo userInfo = EnrichmentUnionUtil.getUserInfo(union);
    userInfo = userInfo.toBuilder().clearUserId().setRetainedUserId(retainedUserId).build();
    return EnrichmentUnionUtil.setUserInfo(union, userInfo);
  }
}
