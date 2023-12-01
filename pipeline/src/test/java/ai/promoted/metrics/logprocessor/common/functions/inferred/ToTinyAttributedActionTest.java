package ai.promoted.metrics.logprocessor.common.functions.inferred;

import ai.promoted.proto.event.Attribution;
import ai.promoted.proto.event.TinyAction;
import ai.promoted.proto.event.TinyActionPath;
import ai.promoted.proto.event.TinyAttributedAction;
import ai.promoted.proto.event.TinyImpression;
import ai.promoted.proto.event.TinyJoinedImpression;
import ai.promoted.proto.event.TinyTouchpoint;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ToTinyAttributedActionTest {

  @Test
  public void flatMap_empty() {
    Collector<TinyAttributedAction> out = Mockito.mock(Collector.class);
    new ToTinyAttributedAction().flatMap(TinyActionPath.getDefaultInstance(), out);
    Mockito.verifyNoInteractions(out);
  }

  @Test
  public void flatMap_oneTouchpoint() {
    Collector<TinyAttributedAction> out = Mockito.mock(Collector.class);
    new ToTinyAttributedAction()
        .flatMap(
            TinyActionPath.newBuilder()
                .setAction(newTinyAction("act1"))
                .addTouchpoints(newTinyTouchpoint(newTinyJoinedImpression("imp1")))
                .build(),
            out);
    Mockito.verify(out)
        .collect(
            TinyAttributedAction.newBuilder()
                .setAction(newTinyAction("act1"))
                .setAttribution(newAttribution(AttributionModel.LATEST.id, 1000))
                .setTouchpoint(newTinyTouchpoint(newTinyJoinedImpression("imp1")))
                .build());
    Mockito.verify(out)
        .collect(
            TinyAttributedAction.newBuilder()
                .setAction(newTinyAction("act1"))
                .setAttribution(newAttribution(AttributionModel.EVEN.id, 1000))
                .setTouchpoint(newTinyTouchpoint(newTinyJoinedImpression("imp1")))
                .build());
    Mockito.verifyNoMoreInteractions(out);
  }

  @Test
  public void flatMap_multipleTouchpoints() {
    Collector<TinyAttributedAction> out = Mockito.mock(Collector.class);
    new ToTinyAttributedAction()
        .flatMap(
            TinyActionPath.newBuilder()
                .setAction(newTinyAction("act1"))
                .addTouchpoints(
                    TinyTouchpoint.newBuilder()
                        .setJoinedImpression(newTinyJoinedImpression("imp1"))
                        .build())
                .addTouchpoints(
                    TinyTouchpoint.newBuilder()
                        .setJoinedImpression(newTinyJoinedImpression("imp2"))
                        .build())
                .addTouchpoints(
                    TinyTouchpoint.newBuilder()
                        .setJoinedImpression(newTinyJoinedImpression("imp3"))
                        .build())
                .build(),
            out);
    Mockito.verify(out)
        .collect(
            TinyAttributedAction.newBuilder()
                .setAction(newTinyAction("act1"))
                .setAttribution(newAttribution(AttributionModel.LATEST.id, 1000))
                .setTouchpoint(newTinyTouchpoint(newTinyJoinedImpression("imp3")))
                .build());
    Mockito.verify(out)
        .collect(
            TinyAttributedAction.newBuilder()
                .setAction(newTinyAction("act1"))
                .setAttribution(newAttribution(AttributionModel.EVEN.id, 333))
                .setTouchpoint(newTinyTouchpoint(newTinyJoinedImpression("imp1")))
                .build());
    Mockito.verify(out)
        .collect(
            TinyAttributedAction.newBuilder()
                .setAction(newTinyAction("act1"))
                .setAttribution(newAttribution(AttributionModel.EVEN.id, 333))
                .setTouchpoint(newTinyTouchpoint(newTinyJoinedImpression("imp2")))
                .build());
    Mockito.verify(out)
        .collect(
            TinyAttributedAction.newBuilder()
                .setAction(newTinyAction("act1"))
                .setAttribution(newAttribution(AttributionModel.EVEN.id, 334))
                .setTouchpoint(newTinyTouchpoint(newTinyJoinedImpression("imp3")))
                .build());
    Mockito.verifyNoMoreInteractions(out);
  }

  private static TinyAction newTinyAction(String actionId) {
    return TinyAction.newBuilder().setActionId(actionId).build();
  }

  private static TinyJoinedImpression newTinyJoinedImpression(String impressionId) {
    return TinyJoinedImpression.newBuilder()
        .setImpression(TinyImpression.newBuilder().setImpressionId(impressionId))
        .build();
  }

  private static Attribution newAttribution(long modelId, int creditMillis) {
    return Attribution.newBuilder().setModelId(modelId).setCreditMillis(creditMillis).build();
  }

  private static TinyTouchpoint newTinyTouchpoint(TinyJoinedImpression joinedImpression) {
    return TinyTouchpoint.newBuilder().setJoinedImpression(joinedImpression).build();
  }
}
