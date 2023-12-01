package ai.promoted.metrics.logprocessor.job.join.action;

import ai.promoted.proto.event.TinyAction;
import ai.promoted.proto.event.TinyActionPath;
import ai.promoted.proto.event.TinyJoinedAction;
import ai.promoted.proto.event.TinyJoinedImpression;
import ai.promoted.proto.event.TinyTouchpoint;
import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/** Converts list of joined actions to an action path. */
class FromListToActionPath extends ProcessFunction<List<TinyJoinedAction>, TinyActionPath> {

  static final OutputTag<TinyAction> DROPPED_TAG =
      new OutputTag<>("dropped", TypeInformation.of(TinyAction.class));

  @Override
  public void processElement(
      List<TinyJoinedAction> records,
      ProcessFunction<List<TinyJoinedAction>, TinyActionPath>.Context ctx,
      Collector<TinyActionPath> out)
      throws Exception {
    Preconditions.checkArgument(!records.isEmpty(), "records needs to not be empty");
    TinyActionPath.Builder builder =
        TinyActionPath.newBuilder().setAction(records.get(0).getAction());
    TinyJoinedAction unjoinedAction = null;
    for (TinyJoinedAction record : records) {
      if (!record.getJoinedImpression().getImpression().getImpressionId().isEmpty()) {
        builder.addTouchpoints(
            TinyTouchpoint.newBuilder()
                .setJoinedImpression(
                    TinyJoinedImpression.newBuilder()
                        .setInsertion(record.getJoinedImpression().getInsertion())
                        .setImpression(record.getJoinedImpression().getImpression())));
      } else {
        unjoinedAction = record;
      }
    }
    if (builder.getTouchpointsCount() > 0) {
      out.collect(builder.build());
    } else {
      Preconditions.checkArgument(
          unjoinedAction.getJoinedImpression().getImpression().getImpressionId().isEmpty(),
          "Expected an unjoinedAction when no touchpoints are added, %s",
          unjoinedAction);
      ctx.output(DROPPED_TAG, unjoinedAction.getAction());
    }
  }
}
