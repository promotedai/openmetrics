package ai.promoted.metrics.logprocessor.common.functions.inferred;

import ai.promoted.proto.event.Attribution;
import ai.promoted.proto.event.TinyActionPath;
import ai.promoted.proto.event.TinyAttributedAction;
import ai.promoted.proto.event.TinyTouchpoint;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class ToTinyAttributedAction
    implements FlatMapFunction<TinyActionPath, TinyAttributedAction> {

  @Override
  public void flatMap(TinyActionPath actionPath, Collector<TinyAttributedAction> out) {
    if (actionPath.getTouchpointsCount() == 0) {
      return;
    }
    outputLastTouchpointModel(actionPath, out);
    outputEven(actionPath, out);
  }

  private void outputLastTouchpointModel(
      TinyActionPath actionPath, Collector<TinyAttributedAction> out) {
    out.collect(
        TinyAttributedAction.newBuilder()
            .setAction(actionPath.getAction())
            .setAttribution(
                Attribution.newBuilder()
                    .setModelId(AttributionModel.LATEST.id)
                    .setCreditMillis(1000))
            .setTouchpoint(
                TinyTouchpoint.newBuilder()
                    .setJoinedImpression(
                        actionPath
                            .getTouchpoints(actionPath.getTouchpointsCount() - 1)
                            .getJoinedImpression()))
            .build());
  }

  private Attribution newEvenAttribution(int creditMillis) {
    return Attribution.newBuilder()
        .setModelId(AttributionModel.EVEN.id)
        .setCreditMillis(creditMillis)
        .build();
  }

  private void outputEven(TinyActionPath actionPath, Collector<TinyAttributedAction> out) {

    // Reduce number of different Attribution messages created.
    // Put any remainder at the end.
    Attribution[] attributions = new Attribution[actionPath.getTouchpointsCount()];
    int creditMillis = 1000 / actionPath.getTouchpointsCount();
    Attribution attribution = newEvenAttribution(creditMillis);
    for (int i = 0; i < actionPath.getTouchpointsCount(); i++) {
      attributions[i] = attribution;
    }
    if (1000 != creditMillis * actionPath.getTouchpointsCount()) {
      int numAdjust = 1000 - creditMillis * actionPath.getTouchpointsCount();
      Attribution adjustedAttribution = newEvenAttribution(creditMillis + 1);
      for (int i = 0; i < numAdjust; i++) {
        attributions[actionPath.getTouchpointsCount() - 1 - i] = adjustedAttribution;
      }
    }

    for (int i = 0; i < actionPath.getTouchpointsCount(); i++) {
      TinyTouchpoint touchpoint = actionPath.getTouchpoints(i);
      out.collect(
          TinyAttributedAction.newBuilder()
              .setAction(actionPath.getAction())
              .setAttribution(attributions[i])
              .setTouchpoint(touchpoint)
              .build());
    }
  }
}
