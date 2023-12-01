package ai.promoted.metrics.logprocessor.common.functions.redundantevent;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.promoted.metrics.common.RedundantAction;
import ai.promoted.metrics.logprocessor.common.util.DebugIds;
import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.TinyAction;
import ai.promoted.proto.event.TinyActionPath;
import ai.promoted.proto.event.TinyCommonInfo;
import ai.promoted.proto.event.TinyImpression;
import ai.promoted.proto.event.TinyInsertion;
import ai.promoted.proto.event.TinyInsertionCore;
import ai.promoted.proto.event.TinyJoinedImpression;
import ai.promoted.proto.event.TinyTouchpoint;
import com.google.common.collect.Iterables;
import com.twitter.chill.protobuf.ProtobufSerializer;
import java.time.Duration;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ReduceRedundantTinyActionTest {

  private static final Duration ttl = Duration.ofHours(1);

  private ReduceRedundantTinyActions reduceRedundantTinyActions;
  private KeyedOneInputStreamOperatorTestHarness<?, TinyActionPath, TinyActionPath> harness;

  @BeforeEach
  public void setUp() throws Exception {
    reduceRedundantTinyActions = new ReduceRedundantTinyActions(ttl, DebugIds.empty());
    harness =
        ProcessFunctionTestHarnesses.forKeyedProcessFunction(
            reduceRedundantTinyActions,
            RedundantEventKeys::forRedundantActionsInPostNavigateActionStream,
            Types.TUPLE(Types.LONG, Types.STRING, Types.STRING, Types.STRING));

    harness
        .getExecutionConfig()
        .registerTypeWithKryoSerializer(TinyActionPath.class, ProtobufSerializer.class);
  }

  @AfterEach
  public void tearDown() throws Exception {
    assertThat(harness.numEventTimeTimers()).isEqualTo(0);
  }

  private void assertEmpty(Iterable it) {
    assertTrue(Iterables.isEmpty(it));
  }

  @Test
  public void noEvents() throws Exception {
    harness.processWatermark(new Watermark(0));
    assertEmpty(harness.extractOutputValues());
    assertNull(harness.getSideOutput(ReduceRedundantTinyActions.REDUNDANT_ACTION));
  }

  @Test
  public void oneAction() throws Exception {
    TinyActionPath nav1 =
        TinyActionPath.newBuilder()
            .setAction(newAction(0, "c1", "act1", ActionType.NAVIGATE))
            .build();
    harness.processElement(nav1, 0);
    harness.processWatermark(0);
    assertThat(harness.extractOutputValues()).containsExactly(nav1);
    assertThat(harness.numEventTimeTimers()).isEqualTo(1);

    harness.processWatermark(new Watermark(ttl.toMillis() + 1));
    assertThat(harness.extractOutputValues()).containsExactly(nav1);
    assertNull(harness.getSideOutput(ReduceRedundantTinyActions.REDUNDANT_ACTION));
  }

  @Test
  public void multipleActions() throws Exception {
    TinyActionPath nav1 =
        TinyActionPath.newBuilder()
            .setAction(newAction(0, "c1", "act1", ActionType.NAVIGATE))
            .addTouchpoints(
                TinyTouchpoint.newBuilder()
                    .setJoinedImpression(newImpression(0L, "paging1", "ins1", "imp1")))
            .build();
    harness.processElement(nav1, 0);
    harness.processWatermark(0);
    assertThat(harness.extractOutputValues()).containsExactly(nav1);
    assertThat(harness.numEventTimeTimers()).isEqualTo(1);

    TinyActionPath.Builder nav2Builder = nav1.toBuilder();
    nav2Builder.getActionBuilder().setActionId("act2").getCommonBuilder().setEventApiTimestamp(2L);
    harness.processElement(nav2Builder.build(), 2);
    harness.processWatermark(2);
    assertThat(harness.extractOutputValues()).containsExactly(nav1);
    assertThat(harness.numEventTimeTimers()).isEqualTo(1);

    harness.processWatermark(new Watermark(ttl.toMillis() + 1));
    assertThat(harness.extractOutputValues()).containsExactly(nav1);
    assertThat(harness.numEventTimeTimers()).isEqualTo(0);

    assertThat(harness.getSideOutput(ReduceRedundantTinyActions.REDUNDANT_ACTION))
        .containsExactly(
            new StreamRecord(
                RedundantAction.newBuilder()
                    .setPlatformId(1)
                    .setEventApiTimestamp(2)
                    .setRedundantActionId("act2")
                    .setRedundantActionType("NAVIGATE")
                    .setRedundantCustomActionType("")
                    .setActionId("act1")
                    .setActionType("NAVIGATE")
                    .setCustomActionType("")
                    .build(),
                2));
  }

  @Test
  public void multipleActions_delayedWatermark() throws Exception {
    TinyActionPath nav1 =
        TinyActionPath.newBuilder()
            .setAction(newAction(0, "c1", "act1", ActionType.NAVIGATE))
            .addTouchpoints(
                TinyTouchpoint.newBuilder()
                    .setJoinedImpression(newImpression(0L, "paging1", "ins1", "imp1")))
            .build();
    harness.processElement(nav1, 0);
    assertThat(harness.extractOutputValues()).isEmpty();
    assertThat(harness.numEventTimeTimers()).isEqualTo(1);

    TinyActionPath.Builder nav2Builder = nav1.toBuilder();
    nav2Builder.getActionBuilder().setActionId("act2").getCommonBuilder().setEventApiTimestamp(2L);
    harness.processElement(nav2Builder.build(), 2);
    harness.processWatermark(2);
    assertThat(harness.extractOutputValues()).containsExactly(nav1);
    assertThat(harness.numEventTimeTimers()).isEqualTo(1);

    harness.processWatermark(new Watermark(ttl.toMillis() + 1));
    assertThat(harness.extractOutputValues()).containsExactly(nav1);
    assertThat(harness.numEventTimeTimers()).isEqualTo(0);

    // The redundant event gets outputted with an earlier time.  This redundant event is already
    // in Flink state before the watermark passes and the onTimer=0 fires.  That onTimer marks
    // this action as redundant.
    assertThat(harness.getSideOutput(ReduceRedundantTinyActions.REDUNDANT_ACTION))
        .containsExactly(
            new StreamRecord(
                RedundantAction.newBuilder()
                    .setPlatformId(1)
                    .setEventApiTimestamp(2)
                    .setRedundantActionId("act2")
                    .setRedundantActionType("NAVIGATE")
                    .setRedundantCustomActionType("")
                    .setActionId("act1")
                    .setActionType("NAVIGATE")
                    .setCustomActionType("")
                    .build(),
                0));
  }

  private TinyJoinedImpression newImpression(
      long eventApiTimestamp, String pagingId, String insertionId, String impressionId) {
    return TinyJoinedImpression.newBuilder()
        .setImpression(
            TinyImpression.newBuilder()
                .setCommon(newCommonInfo(eventApiTimestamp))
                .setImpressionId(impressionId))
        .setInsertion(
            TinyInsertion.newBuilder()
                .setPagingId(pagingId)
                .setCore(TinyInsertionCore.newBuilder().setInsertionId(insertionId)))
        .build();
  }

  private TinyAction newAction(
      long timestamp, String contentId, String actionId, ActionType actionType) {
    return newAction(timestamp, contentId, actionId, actionType, "");
  }

  private TinyAction newAction(
      long timestamp,
      String contentId,
      String actionId,
      ActionType actionType,
      String customActionType) {
    return TinyAction.newBuilder()
        .setCommon(newCommonInfo(timestamp))
        .setActionId(actionId)
        .setContentId(contentId)
        .setActionType(actionType)
        .setCustomActionType(customActionType)
        .build();
  }

  private TinyCommonInfo newCommonInfo(long eventApiTimestamp) {
    return TinyCommonInfo.newBuilder()
        .setPlatformId(1L)
        .setEventApiTimestamp(eventApiTimestamp)
        .setAnonUserId("anonUserId")
        .build();
  }
}
