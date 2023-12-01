package ai.promoted.metrics.logprocessor.job.join.impression;

import ai.promoted.proto.event.TinyCommonInfo;
import ai.promoted.proto.event.TinyImpression;
import ai.promoted.proto.event.TinyInsertion;
import ai.promoted.proto.event.TinyInsertionCore;
import ai.promoted.proto.event.TinyJoinedImpression;
import ai.promoted.proto.event.UnnestedTinyJoinedImpression;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class MaterializeAndUnnestJoinImpressionTest {
  private MaterializeAndUnnestJoinedImpression fn;
  private ProcessFunction<TinyJoinedImpression, UnnestedTinyJoinedImpression>.Context ctx;
  private Collector<UnnestedTinyJoinedImpression> out;

  @BeforeEach
  public void setUp() throws Exception {
    fn = new MaterializeAndUnnestJoinedImpression();
    ctx = Mockito.mock(ProcessFunction.Context.class);
    out = Mockito.mock(Collector.class);
  }

  @Test
  public void noOtherContentIds_valuesOnInsertion() throws Exception {
    TinyJoinedImpression input =
        TinyJoinedImpression.newBuilder()
            .setInsertion(
                TinyInsertion.newBuilder()
                    .setCommon(TinyCommonInfo.newBuilder().setPlatformId(1L).setAnonUserId("user1"))
                    .setCore(
                        TinyInsertionCore.newBuilder().setInsertionId("ins1").setContentId("c1")))
            .setImpression(TinyImpression.newBuilder().setImpressionId("imp1"))
            .build();

    fn.processElement(input, ctx, out);
    Mockito.verify(out)
        .collect(
            UnnestedTinyJoinedImpression.newBuilder()
                .setJoinedImpression(input)
                .setPlatformId(1L)
                .setAnonUserId("user1")
                .setContentId("c1")
                .build());
    Mockito.verifyNoMoreInteractions(out);
    Mockito.verifyNoInteractions(ctx);
  }

  @Test
  public void noOtherContentIds_valuesOnImpression() throws Exception {
    TinyJoinedImpression input =
        TinyJoinedImpression.newBuilder()
            .setImpression(
                TinyImpression.newBuilder()
                    .setCommon(TinyCommonInfo.newBuilder().setPlatformId(1L).setAnonUserId("user1"))
                    .setContentId("c1")
                    .setImpressionId("imp1"))
            .build();

    fn.processElement(input, ctx, out);
    Mockito.verify(out)
        .collect(
            UnnestedTinyJoinedImpression.newBuilder()
                .setJoinedImpression(input)
                .setPlatformId(1L)
                .setAnonUserId("user1")
                .setContentId("c1")
                .build());
    Mockito.verifyNoMoreInteractions(out);
    Mockito.verifyNoInteractions(ctx);
  }

  @Test
  public void oneOtherContentIds() throws Exception {
    TinyJoinedImpression input =
        TinyJoinedImpression.newBuilder()
            .setInsertion(
                TinyInsertion.newBuilder()
                    .setCommon(TinyCommonInfo.newBuilder().setPlatformId(1L).setAnonUserId("user1"))
                    .setCore(
                        TinyInsertionCore.newBuilder()
                            .setInsertionId("ins1")
                            .setContentId("c1")
                            .putOtherContentIds(1, "c2")))
            .setImpression(TinyImpression.newBuilder().setImpressionId("imp1"))
            .build();

    fn.processElement(input, ctx, out);
    Mockito.verify(out)
        .collect(
            UnnestedTinyJoinedImpression.newBuilder()
                .setJoinedImpression(input)
                .setPlatformId(1L)
                .setAnonUserId("user1")
                .setContentId("c1")
                .build());
    Mockito.verify(out)
        .collect(
            UnnestedTinyJoinedImpression.newBuilder()
                .setJoinedImpression(input)
                .setPlatformId(1L)
                .setAnonUserId("user1")
                .setContentId("c2")
                .setIsOtherContentId(true)
                .build());
    Mockito.verifyNoMoreInteractions(out);
    Mockito.verifyNoInteractions(ctx);
  }

  @Test
  public void twoOtherContentIds() throws Exception {
    TinyJoinedImpression input =
        TinyJoinedImpression.newBuilder()
            .setInsertion(
                TinyInsertion.newBuilder()
                    .setCommon(TinyCommonInfo.newBuilder().setPlatformId(1L).setAnonUserId("user1"))
                    .setCore(
                        TinyInsertionCore.newBuilder()
                            .setInsertionId("ins1")
                            .setContentId("c1")
                            .putOtherContentIds(1, "c2")
                            .putOtherContentIds(2, "c3")))
            .setImpression(TinyImpression.newBuilder().setImpressionId("imp1"))
            .build();

    fn.processElement(input, ctx, out);
    Mockito.verify(out)
        .collect(
            UnnestedTinyJoinedImpression.newBuilder()
                .setJoinedImpression(input)
                .setPlatformId(1L)
                .setAnonUserId("user1")
                .setContentId("c1")
                .build());
    Mockito.verify(out)
        .collect(
            UnnestedTinyJoinedImpression.newBuilder()
                .setJoinedImpression(input)
                .setPlatformId(1L)
                .setAnonUserId("user1")
                .setContentId("c2")
                .setIsOtherContentId(true)
                .build());
    Mockito.verify(out)
        .collect(
            UnnestedTinyJoinedImpression.newBuilder()
                .setJoinedImpression(input)
                .setPlatformId(1L)
                .setAnonUserId("user1")
                .setContentId("c3")
                .setIsOtherContentId(true)
                .build());
    Mockito.verifyNoMoreInteractions(out);
    Mockito.verifyNoInteractions(ctx);
  }
}
