package ai.promoted.metrics.logprocessor.job.join.action;

import ai.promoted.proto.event.TinyAction;
import ai.promoted.proto.event.TinyCommonInfo;
import ai.promoted.proto.event.UnnestedTinyAction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class UnnestTinyActionTest {
  private UnnestTinyAction fn;
  private ProcessFunction<TinyAction, UnnestedTinyAction>.Context ctx;
  private Collector<UnnestedTinyAction> out;

  @BeforeEach
  public void setUp() throws Exception {
    fn = new UnnestTinyAction();
    ctx = Mockito.mock(ProcessFunction.Context.class);
    out = Mockito.mock(Collector.class);
  }

  @Test
  public void noOtherContentIds() throws Exception {
    TinyAction input =
        TinyAction.newBuilder()
            .setCommon(TinyCommonInfo.newBuilder().setPlatformId(1L).setAnonUserId("user1"))
            .setActionId("a1")
            .setContentId("c1")
            .build();

    fn.processElement(input, ctx, out);
    Mockito.verify(out)
        .collect(UnnestedTinyAction.newBuilder().setAction(input).setContentId("c1").build());
    Mockito.verifyNoMoreInteractions(out);
    Mockito.verifyNoInteractions(ctx);
  }

  @Test
  public void oneOtherContentIds() throws Exception {
    TinyAction input =
        TinyAction.newBuilder()
            .setCommon(TinyCommonInfo.newBuilder().setPlatformId(1L).setAnonUserId("user1"))
            .setActionId("a1")
            .setContentId("c1")
            .putOtherContentIds(1, "c2")
            .build();

    fn.processElement(input, ctx, out);
    Mockito.verify(out)
        .collect(UnnestedTinyAction.newBuilder().setAction(input).setContentId("c1").build());
    Mockito.verify(out)
        .collect(
            UnnestedTinyAction.newBuilder()
                .setAction(input)
                .setIsOtherContentId(true)
                .setContentId("c2")
                .build());
    Mockito.verifyNoMoreInteractions(out);
    Mockito.verifyNoInteractions(ctx);
  }

  @Test
  public void twoOtherContentIds() throws Exception {
    TinyAction input =
        TinyAction.newBuilder()
            .setCommon(TinyCommonInfo.newBuilder().setPlatformId(1L).setAnonUserId("user1"))
            .setActionId("a1")
            .setContentId("c1")
            .putOtherContentIds(1, "c2")
            .putOtherContentIds(2, "c3")
            .build();

    fn.processElement(input, ctx, out);
    Mockito.verify(out)
        .collect(UnnestedTinyAction.newBuilder().setAction(input).setContentId("c1").build());
    Mockito.verify(out)
        .collect(
            UnnestedTinyAction.newBuilder()
                .setAction(input)
                .setIsOtherContentId(true)
                .setContentId("c2")
                .build());
    Mockito.verify(out)
        .collect(
            UnnestedTinyAction.newBuilder()
                .setAction(input)
                .setIsOtherContentId(true)
                .setContentId("c3")
                .build());
    Mockito.verifyNoMoreInteractions(out);
    Mockito.verifyNoInteractions(ctx);
  }
}
