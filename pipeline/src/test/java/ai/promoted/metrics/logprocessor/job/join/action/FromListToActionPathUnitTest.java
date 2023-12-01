package ai.promoted.metrics.logprocessor.job.join.action;

import ai.promoted.proto.event.TinyAction;
import ai.promoted.proto.event.TinyActionPath;
import ai.promoted.proto.event.TinyImpression;
import ai.promoted.proto.event.TinyInsertion;
import ai.promoted.proto.event.TinyInsertionCore;
import ai.promoted.proto.event.TinyJoinedAction;
import ai.promoted.proto.event.TinyJoinedImpression;
import ai.promoted.proto.event.TinyTouchpoint;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class FromListToActionPathUnitTest {
  private FromListToActionPath fn;
  private ProcessFunction<List<TinyJoinedAction>, TinyActionPath>.Context ctx;
  private Collector<TinyActionPath> out;

  @BeforeEach
  public void setUp() throws Exception {
    fn = new FromListToActionPath();
    ctx = Mockito.mock(ProcessFunction.Context.class);
    out = Mockito.mock(Collector.class);
  }

  @Test
  public void oneRecord() throws Exception {
    List<TinyJoinedAction> joinedActions =
        ImmutableList.of(
            newJoinedAction(
                newJoinedImpression(newInsertion("ins1"), newImpression("imp1")),
                newAction("act1")));

    fn.processElement(joinedActions, ctx, out);
    Mockito.verify(out)
        .collect(
            TinyActionPath.newBuilder()
                .setAction(newAction("act1"))
                .addTouchpoints(
                    TinyTouchpoint.newBuilder()
                        .setJoinedImpression(
                            newJoinedImpression(newInsertion("ins1"), newImpression("imp1"))))
                .build());
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test
  public void unjoined() throws Exception {
    List<TinyJoinedAction> joinedActions =
        ImmutableList.of(
            newJoinedAction(
                newJoinedImpression(newInsertion(""), newImpression("")), newAction("act1")));

    fn.processElement(joinedActions, ctx, out);
    Mockito.verifyNoMoreInteractions(out);
    Mockito.verify(ctx).output(FromListToActionPath.DROPPED_TAG, newAction("act1"));
  }

  @Test
  public void multipleRecords() throws Exception {
    List<TinyJoinedAction> joinedActions =
        ImmutableList.of(
            newJoinedAction(
                newJoinedImpression(newInsertion("ins1"), newImpression("imp1")),
                newAction("act1")),
            newJoinedAction(
                newJoinedImpression(newInsertion("ins2"), newImpression("imp2")),
                newAction("act1")),
            newJoinedAction(
                newJoinedImpression(newInsertion(""), newImpression("")), newAction("act1")),
            newJoinedAction(
                newJoinedImpression(newInsertion("ins3"), newImpression("imp3")),
                newAction("act1")));

    fn.processElement(joinedActions, ctx, out);
    Mockito.verify(out)
        .collect(
            TinyActionPath.newBuilder()
                .setAction(newAction("act1"))
                .addTouchpoints(
                    TinyTouchpoint.newBuilder()
                        .setJoinedImpression(
                            newJoinedImpression(newInsertion("ins1"), newImpression("imp1"))))
                .addTouchpoints(
                    TinyTouchpoint.newBuilder()
                        .setJoinedImpression(
                            newJoinedImpression(newInsertion("ins2"), newImpression("imp2"))))
                .addTouchpoints(
                    TinyTouchpoint.newBuilder()
                        .setJoinedImpression(
                            newJoinedImpression(newInsertion("ins3"), newImpression("imp3"))))
                .build());
    Mockito.verifyNoMoreInteractions(ctx);
  }

  private TinyJoinedAction newJoinedAction(
      TinyJoinedImpression joinedImpression, TinyAction action) {
    return TinyJoinedAction.newBuilder()
        .setJoinedImpression(joinedImpression)
        .setAction(action)
        .build();
  }

  private static TinyAction newAction(String actionId) {
    return TinyAction.newBuilder().setActionId(actionId).build();
  }

  // otherContentId = used to create lower priority TinyInsertions.
  private static TinyInsertion newInsertion(String insertionId) {
    return TinyInsertion.newBuilder()
        .setCore(TinyInsertionCore.newBuilder().setInsertionId(insertionId))
        .build();
  }

  private static TinyImpression newImpression(String impressionId) {
    return TinyImpression.newBuilder().setImpressionId(impressionId).build();
  }

  private static TinyJoinedImpression newJoinedImpression(
      TinyInsertion insertion, TinyImpression impression) {
    return TinyJoinedImpression.newBuilder()
        .setInsertion(insertion)
        .setImpression(impression)
        .build();
  }
}
