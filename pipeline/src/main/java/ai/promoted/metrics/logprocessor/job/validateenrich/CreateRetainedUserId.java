package ai.promoted.metrics.logprocessor.job.validateenrich;

import ai.promoted.metrics.logprocessor.common.util.TrackingUtil;
import ai.promoted.proto.common.RetainedUser;
import ai.promoted.proto.common.RetainedUserState;
import com.google.common.annotations.VisibleForTesting;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

// TODO - It feels like this should be doable using Windows.  It felt like all of built-in functions
// trade offs.  E.g. generating a bunch of extra retainedUserIds but ignoring them.
// TODO - it's not safe to run this code concurrently across blue/green. Different retainedUser IDs
//  will be used.
/**
 * An operator that handles creating missing retainedUserId. This operator should be placed after
 * the lookup retainedUserId operator.
 *
 * <p>The operator is keyed based on the auth userId. Outputs are delayed to the onTimer to handle
 * out of order events. The operator tries to only have one timer.
 *
 * <p>This paragraph is scoped to the first lookup row (not factoring in late events). If the
 * earliest Paimon RetainedUser lookup row has a retainedUserId, then it'll get stored using a
 * ~session window. After ttlMillis passes from the last encountered record for the userId, a DELETE
 * Change will be outputted so downstream operators can clear out their state. If the earliest
 * lookup does not have a retainedUserId, this operator will create a retainedUserId. It'll try to
 * use anonUserId if it exists. New retainedUserIds will be outputted to a side output for writing
 * to the Paimon table.
 */
class CreateRetainedUserId
    extends KeyedProcessFunction<AuthUserIdKey, RetainedUserLookupRow, SelectRetainedUserChange> {
  private static final long ALREADY_OUTPUTTED_TIME = -1L;

  public static final OutputTag<RetainedUser> NEW_RETAINED_USERS_TAG =
      new OutputTag<>("new-retained-users", TypeInformation.of(RetainedUser.class));

  @VisibleForTesting transient ValueState<RetainedUserState> retainedUser;
  // If the row is not outputted, it's the time of the timer to output.  Else it's a special value
  // of 1L to indicate that the row was already outputted.
  @VisibleForTesting transient ValueState<Long> outputTime;
  // Keeps track of the clean up time.  max(eventTimes) + ttlMillis.  Clean-up happens in onTimer.
  @VisibleForTesting transient ValueState<Long> cleanUpTime;

  // Used when anonUserId is not set.
  private final RetainedUserIdAnonymizer userIdAnonymizer;
  // In the onTimer of ~lastMillis+ttlMillis, a DELETE change is sent so a downstream operator can
  // clean up state.
  private final long ttlMillis;

  CreateRetainedUserId(RetainedUserIdAnonymizer userIdAnonymizer, long ttlMillis) {
    this.userIdAnonymizer = userIdAnonymizer;
    this.ttlMillis = ttlMillis;
  }

  @Override
  public void open(Configuration config) throws Exception {
    super.open(config);
    retainedUser =
        getRuntimeContext()
            .getState(
                new ValueStateDescriptor<>(
                    "retainedUser", TypeInformation.of(RetainedUserState.class)));
    outputTime = getRuntimeContext().getState(new ValueStateDescriptor<>("outputTime", Types.LONG));
    cleanUpTime =
        getRuntimeContext().getState(new ValueStateDescriptor<>("cleanUpTime", Types.LONG));
  }

  @Override
  public void processElement(
      RetainedUserLookupRow input,
      KeyedProcessFunction<AuthUserIdKey, RetainedUserLookupRow, SelectRetainedUserChange>.Context
          ctx,
      Collector<SelectRetainedUserChange> out)
      throws Exception {
    Long previousOutputTimeValue = outputTime.value();
    Long previousCleanUpTimeValue = cleanUpTime.value();

    long inputEventApiTime = ctx.timestamp();
    long inputCleanUpTime = inputEventApiTime + ttlMillis;

    if (previousOutputTimeValue != null && previousOutputTimeValue == ALREADY_OUTPUTTED_TIME) {
      // Already outputted.  The timer is cleanUpTime.
      if (inputCleanUpTime > previousCleanUpTimeValue) {
        // Delay existing cleanup timer.
        cleanUpTime.update(inputCleanUpTime);
        ctx.timerService().deleteEventTimeTimer(previousCleanUpTimeValue);
        ctx.timerService().registerEventTimeTimer(inputCleanUpTime);
      }
    } else {
      // See if we should save for later.  The Timer is for the output time.
      RetainedUserState retainedUserValue = retainedUser.value();
      if ((retainedUserValue == null) || (inputEventApiTime < previousOutputTimeValue)) {
        RetainedUserState.Builder builder =
            RetainedUserState.newBuilder()
                .setPlatformId(input.getPlatformId())
                .setUserId(input.getUserId())
                .setAnonUserId(input.getAnonUserId())
                .setCreateEventApiTimeMillis(inputEventApiTime);
        String retainedUserId = input.getRetainedUserId();
        if (retainedUserId != null && !retainedUserId.isEmpty()) {
          builder.setRetainedUserId(retainedUserId);
        }
        retainedUser.update(builder.build());
        outputTime.update(inputEventApiTime);
        if (previousOutputTimeValue != null) {
          ctx.timerService().deleteEventTimeTimer(previousOutputTimeValue);
        }
        ctx.timerService().registerEventTimeTimer(inputEventApiTime);
      }
      if ((previousCleanUpTimeValue == null) || (inputCleanUpTime > previousCleanUpTimeValue)) {
        cleanUpTime.update(inputCleanUpTime);
      }
    }
  }

  @Override
  public void onTimer(
      long timestamp,
      KeyedProcessFunction<AuthUserIdKey, RetainedUserLookupRow, SelectRetainedUserChange>
              .OnTimerContext
          ctx,
      Collector<SelectRetainedUserChange> out)
      throws Exception {
    RetainedUserState retainedUserValue = retainedUser.value();

    Long outputTimeValue = outputTime.value();
    if (outputTimeValue != null && outputTimeValue == timestamp) {
      if (retainedUserValue.getRetainedUserId().isEmpty()) {
        String retainedUserId = retainedUserValue.getAnonUserId();
        if (retainedUserId.isEmpty()) {
          retainedUserId = userIdAnonymizer.apply(retainedUserValue, ctx.timestamp());
        }
        retainedUserValue = retainedUserValue.toBuilder().setRetainedUserId(retainedUserId).build();
        retainedUser.update(retainedUserValue);

        ctx.output(
            NEW_RETAINED_USERS_TAG,
            RetainedUser.newBuilder()
                .setPlatformId(retainedUserValue.getPlatformId())
                .setUserId(retainedUserValue.getUserId())
                .setRetainedUserId(retainedUserValue.getRetainedUserId())
                .setCreateEventApiTimeMillis(retainedUserValue.getCreateEventApiTimeMillis())
                .setProcessTimeMillis(TrackingUtil.getProcessingTime())
                .build());
      }

      out.collect(
          new SelectRetainedUserChange(
              ChangeKind.UPSERT,
              retainedUserValue.getPlatformId(),
              retainedUserValue.getUserId(),
              retainedUserValue.getRetainedUserId()));
      outputTime.update(ALREADY_OUTPUTTED_TIME);
      ctx.timerService().registerEventTimeTimer(cleanUpTime.value());
    }

    Long cleanUpTimeValue = cleanUpTime.value();
    if (cleanUpTimeValue != null && cleanUpTimeValue == timestamp) {
      // This is the cleanup timer.  Clean state.
      out.collect(
          new SelectRetainedUserChange(
              ChangeKind.DELETE,
              retainedUserValue.getPlatformId(),
              retainedUserValue.getUserId(),
              retainedUserValue.getRetainedUserId()));
      retainedUser.clear();
      outputTime.clear();
      cleanUpTime.clear();
    }
  }
}
