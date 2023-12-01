package ai.promoted.metrics.logprocessor.job.join.action;

import ai.promoted.proto.event.TinyAction;
import ai.promoted.proto.event.UnnestedTinyAction;
import java.util.Iterator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/** Used to unnest TinyActions before SQL joins. */
public class UnnestTinyAction extends ProcessFunction<TinyAction, UnnestedTinyAction> {

  @Override
  public void processElement(
      TinyAction tinyAction,
      ProcessFunction<TinyAction, UnnestedTinyAction>.Context context,
      Collector<UnnestedTinyAction> collector)
      throws Exception {
    UnnestedTinyAction.Builder builder =
        UnnestedTinyAction.newBuilder()
            .setAction(tinyAction)
            .setContentId(tinyAction.getContentId());

    // TODO - evaluate how to handle Actions without content_id but if we still want to join them
    // in.

    // Optimization - Clone is fairly expensive.
    if (tinyAction.getOtherContentIdsCount() == 0) {
      collector.collect(builder.build());
    } else {
      collector.collect(builder.clone().build());
      builder.setIsOtherContentId(true);
      // Avoid extra clone on the last build.
      Iterator<String> otherContentIdsIterator =
          tinyAction.getOtherContentIdsMap().values().iterator();
      while (otherContentIdsIterator.hasNext()) {
        String otherContentId = otherContentIdsIterator.next();
        if (otherContentIdsIterator.hasNext()) {
          collector.collect(builder.clone().setContentId(otherContentId).build());
        } else {
          // Avoid extra clone on the last build.
          collector.collect(builder.setContentId(otherContentId).build());
        }
      }
    }
  }
}
