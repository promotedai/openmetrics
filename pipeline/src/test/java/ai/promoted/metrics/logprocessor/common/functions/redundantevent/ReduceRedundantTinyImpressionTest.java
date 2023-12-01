package ai.promoted.metrics.logprocessor.common.functions.redundantevent;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import ai.promoted.metrics.common.RedundantImpression;
import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.TinyAction;
import ai.promoted.proto.event.TinyActionPath;
import ai.promoted.proto.event.TinyCommonInfo;
import ai.promoted.proto.event.TinyImpression;
import ai.promoted.proto.event.TinyInsertion;
import ai.promoted.proto.event.TinyInsertionCore;
import ai.promoted.proto.event.TinyJoinedImpression;
import ai.promoted.proto.event.TinyTouchpoint;
import com.google.common.collect.ImmutableList;
import com.twitter.chill.protobuf.ProtobufSerializer;
import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ReduceRedundantTinyImpressionTest {

  private static final Duration ttl = Duration.ofHours(1);

  private ReduceRedundantTinyImpressions reduceRedundantTinyImpressions;
  private KeyedTwoInputStreamOperatorTestHarness<
          ?, TinyJoinedImpression, TinyActionPath, TinyJoinedImpression>
      harness;

  @BeforeEach
  public void setUp() throws Exception {
    reduceRedundantTinyImpressions = new ReduceRedundantTinyImpressions(ttl);
    harness =
        ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(
            reduceRedundantTinyImpressions,
            RedundantEventKeys::forRedundantImpressionInImpressionStream,
            RedundantEventKeys::forRedundantImpressionInActionPathStream,
            Types.TUPLE(Types.LONG, Types.STRING, Types.STRING));
    harness
        .getExecutionConfig()
        .registerTypeWithKryoSerializer(TinyJoinedImpression.class, ProtobufSerializer.class);
    harness
        .getExecutionConfig()
        .registerTypeWithKryoSerializer(TinyActionPath.class, ProtobufSerializer.class);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (harness != null) {
      // Assert empty function state.
      assertEquals(0, harness.numEventTimeTimers());
      assertNull(reduceRedundantTinyImpressions.nextTimer.value());
      assertThat(reduceRedundantTinyImpressions.keyToReducedImpression.entries()).isEmpty();
      assertThat((List) reduceRedundantTinyImpressions.delayOutputImpressions.get()).isEmpty();
      assertThat((List) reduceRedundantTinyImpressions.delayOutputActionPaths.get()).isEmpty();
    }
  }

  @Test
  public void noEvents() throws Exception {
    harness.processBothWatermarks(new Watermark(0));
    harness.processBothWatermarks(new Watermark(ttl.toMillis() * 2));
    assertEquals(ImmutableList.of(), harness.extractOutputValues());
    assertNull(harness.getSideOutput(ReduceRedundantTinyImpressions.OUTPUT_ACTION_PATH));
    assertNull(harness.getSideOutput(ReduceRedundantTinyImpressions.REDUNDANT_IMPRESSION));
    // Skip tearDown asserts.
    harness = null;
  }

  @Test
  public void oneImpression() throws Exception {
    TinyJoinedImpression impression = newImpression(0L, "ins1", "imp1");
    harness.processElement1(impression, 0);
    harness.processBothWatermarks(new Watermark(0));
    assertThat(harness.extractOutputValues()).containsExactly(impression);
    assertThat(harness.numEventTimeTimers()).isEqualTo(1);
    harness.processBothWatermarks(new Watermark(ttl.toMillis()));
    assertThat(harness.extractOutputValues()).containsExactly(impression);
    assertNull(harness.getSideOutput(ReduceRedundantTinyImpressions.OUTPUT_ACTION_PATH));
    assertNull(harness.getSideOutput(ReduceRedundantTinyImpressions.REDUNDANT_IMPRESSION));
  }

  @Test
  public void redundantImpressions() throws Exception {
    TinyJoinedImpression impression = newImpression(0L, "ins1", "imp1");
    harness.processElement1(impression, 0);
    // Process redundant impressions.
    harness.processElement1(newImpression(1000, "ins1", "imp2"), 1000);
    harness.processElement1(newImpression(6000, "ins1", "imp3"), 6000);
    harness.processElement1(newImpression(60000, "ins1", "imp4"), 60000);
    harness.processBothWatermarks(new Watermark(0));
    assertThat(harness.extractOutputValues()).containsExactly(impression);
    assertNull(harness.getSideOutput(ReduceRedundantTinyImpressions.OUTPUT_ACTION_PATH));
    assertNull(harness.getSideOutput(ReduceRedundantTinyImpressions.REDUNDANT_IMPRESSION));
    harness.processBothWatermarks(new Watermark(60000));
    assertThat(harness.getSideOutput(ReduceRedundantTinyImpressions.REDUNDANT_IMPRESSION))
        .containsExactly(
            new StreamRecord(newRedundantImpression(1000, "imp2", "imp1"), 1000),
            new StreamRecord(newRedundantImpression(6000, "imp3", "imp1"), 6000),
            new StreamRecord(newRedundantImpression(60000, "imp4", "imp1"), 60000));

    assertThat(harness.numEventTimeTimers()).isEqualTo(1);
    harness.processBothWatermarks(new Watermark(60000 + ttl.toMillis()));
  }

  @Test
  public void redundantImpressionsOutOfOrder() throws Exception {
    harness.processElement1(newImpression(1000, "ins1", "imp2"), 1000);
    TinyJoinedImpression impression = newImpression(0L, "ins1", "imp1");
    harness.processElement1(impression, 0);
    // Redundant.
    harness.processElement1(newImpression(6000, "ins1", "imp3"), 6000);
    harness.processElement1(newImpression(60000, "ins1", "imp4"), 60000);
    harness.processBothWatermarks(new Watermark(0));
    assertThat(harness.extractOutputValues()).containsExactly(impression);
    assertNull(harness.getSideOutput(ReduceRedundantTinyImpressions.OUTPUT_ACTION_PATH));
    assertNull(harness.getSideOutput(ReduceRedundantTinyImpressions.REDUNDANT_IMPRESSION));
    harness.processBothWatermarks(new Watermark(60000));
    assertThat(harness.getSideOutput(ReduceRedundantTinyImpressions.REDUNDANT_IMPRESSION))
        .containsExactly(
            new StreamRecord(newRedundantImpression(1000, "imp2", "imp1"), 1000),
            new StreamRecord(newRedundantImpression(6000, "imp3", "imp1"), 6000),
            new StreamRecord(newRedundantImpression(60000, "imp4", "imp1"), 60000));

    assertThat(harness.numEventTimeTimers()).isEqualTo(1);
    harness.processBothWatermarks(new Watermark(60000 + ttl.toMillis()));
  }

  @Test
  public void redundantImpressionsOutOfOrderSlowWatermark() throws Exception {
    harness.processElement1(newImpression(1000, "ins1", "imp2"), 1000);
    TinyJoinedImpression impression = newImpression(0L, "ins1", "imp1");
    harness.processElement1(impression, 0);
    // Redundant.
    harness.processElement1(newImpression(6000, "ins1", "imp3"), 6000);
    harness.processElement1(newImpression(60000, "ins1", "imp4"), 60000);
    harness.processBothWatermarks(new Watermark(1000));
    assertThat(harness.extractOutputValues()).containsExactly(impression);
    assertNull(harness.getSideOutput(ReduceRedundantTinyImpressions.OUTPUT_ACTION_PATH));
    assertThat(harness.getSideOutput(ReduceRedundantTinyImpressions.REDUNDANT_IMPRESSION))
        .containsExactly(new StreamRecord(newRedundantImpression(1000, "imp2", "imp1"), 1000));
    harness.processBothWatermarks(new Watermark(60000));
    assertThat(harness.getSideOutput(ReduceRedundantTinyImpressions.REDUNDANT_IMPRESSION))
        .containsExactly(
            new StreamRecord(newRedundantImpression(1000, "imp2", "imp1"), 1000),
            new StreamRecord(newRedundantImpression(6000, "imp3", "imp1"), 6000),
            new StreamRecord(newRedundantImpression(60000, "imp4", "imp1"), 60000));

    assertThat(harness.numEventTimeTimers()).isEqualTo(1);
    harness.processBothWatermarks(new Watermark(60000 + ttl.toMillis()));
  }

  @Test
  public void oneActionPathNoImpression() throws Exception {
    TinyActionPath actionPath =
        newActionPath(
            newAction(0, "content1", "act1", ActionType.NAVIGATE),
            newImpression(0, "ins1", "imp1"));
    harness.processElement2(actionPath, 0);
    harness.processBothWatermarks(new Watermark(0));
    assertThat(harness.extractOutputValues()).isEmpty();
    assertThat(harness.getSideOutput(ReduceRedundantTinyImpressions.OUTPUT_ACTION_PATH))
        .contains(new StreamRecord(actionPath, 0));
    assertNull(harness.getSideOutput(ReduceRedundantTinyImpressions.REDUNDANT_IMPRESSION));
    assertThat(harness.numEventTimeTimers()).isEqualTo(0);
    harness.processBothWatermarks(new Watermark(ttl.toMillis()));
  }

  @Test
  public void oneImpressionAndActionInOrderNotRedundantImpression() throws Exception {
    TinyJoinedImpression impression = newImpression(0, "ins1", "imp1", "paging1", 0L);
    harness.processElement1(impression, 0);
    TinyJoinedImpression impression2 = newImpression(1, "ins1", "imp2", "paging1", 1L);
    harness.processElement1(impression2, 1);
    TinyActionPath navigate =
        newActionPath(newAction(1, "content1", "act1", ActionType.NAVIGATE), impression2);
    harness.processElement2(navigate, 1);
    harness.processBothWatermarks(new Watermark(1));
    assertThat(harness.extractOutputValues()).containsExactly(impression, impression2);
    assertThat(harness.getSideOutput(ReduceRedundantTinyImpressions.OUTPUT_ACTION_PATH))
        .contains(new StreamRecord(navigate, 1));
    assertNull(harness.getSideOutput(ReduceRedundantTinyImpressions.REDUNDANT_IMPRESSION));
    assertThat(harness.numEventTimeTimers()).isEqualTo(1);
    harness.processBothWatermarks(new Watermark(1 + ttl.toMillis()));
  }

  @Test
  public void oneImpressionAndActionInOrderRedundantImpression() throws Exception {
    TinyJoinedImpression impression = newImpression(0, "ins1", "imp1", "paging1", 0L);
    harness.processElement1(impression, 0);
    TinyJoinedImpression redundantImpression = newImpression(1, "ins1", "imp2", "paging1", 0L);
    harness.processElement1(redundantImpression, 1);
    TinyActionPath inputNavigate =
        newActionPath(newAction(1, "content1", "act1", ActionType.NAVIGATE), redundantImpression);
    harness.processElement2(inputNavigate, 1);
    harness.processBothWatermarks(new Watermark(1));
    assertThat(harness.extractOutputValues()).containsExactly(impression);
    TinyActionPath expectedNavigate =
        newActionPath(newAction(1, "content1", "act1", ActionType.NAVIGATE), impression);
    assertThat(harness.getSideOutput(ReduceRedundantTinyImpressions.OUTPUT_ACTION_PATH))
        .contains(new StreamRecord(expectedNavigate, 1));
    assertThat(harness.getSideOutput(ReduceRedundantTinyImpressions.REDUNDANT_IMPRESSION))
        .containsExactly(new StreamRecord(newRedundantImpression(1, "imp2", "imp1"), 1));
    assertThat(harness.numEventTimeTimers()).isEqualTo(1);
    harness.processBothWatermarks(new Watermark(1 + ttl.toMillis()));
  }

  @Test
  public void oneImpressionAndActionInOrderRedundantImpressionUnprocessedImpression()
      throws Exception {
    TinyJoinedImpression impression = newImpression(0, "ins1", "imp1", "paging1", 0L);
    harness.processElement1(impression, 0);
    // Even if the redundantImpression hasn't been processed, we use the pagingId and position for
    // the redundant check.
    TinyJoinedImpression redundantImpression = newImpression(0, "ins1", "imp2", "paging1", 0L);
    TinyActionPath inputNavigate =
        newActionPath(newAction(1, "content1", "act1", ActionType.NAVIGATE), redundantImpression);
    harness.processElement2(inputNavigate, 1);
    harness.processBothWatermarks(new Watermark(1));
    assertThat(harness.extractOutputValues()).containsExactly(impression);
    TinyActionPath expectedNavigate =
        newActionPath(newAction(1, "content1", "act1", ActionType.NAVIGATE), impression);
    assertThat(harness.getSideOutput(ReduceRedundantTinyImpressions.OUTPUT_ACTION_PATH))
        .contains(new StreamRecord(expectedNavigate, 1));
    //
    assertNull(harness.getSideOutput(ReduceRedundantTinyImpressions.REDUNDANT_IMPRESSION));
    assertThat(harness.numEventTimeTimers()).isEqualTo(1);
    harness.processBothWatermarks(new Watermark(ttl.toMillis() * 2 + 1));
  }

  @Test
  public void oneImpressionAndActionOutOfOrderRedundantImpression() throws Exception {
    TinyJoinedImpression impression = newImpression(0, "ins1", "imp1", "paging1", 0L);
    TinyJoinedImpression redundantImpression = newImpression(1, "ins1", "imp2", "paging1", 0L);
    TinyActionPath inputNavigate =
        newActionPath(newAction(1, "content1", "act1", ActionType.NAVIGATE), redundantImpression);
    harness.processElement2(inputNavigate, 1);
    harness.processElement1(redundantImpression, 1);
    harness.processElement1(impression, 0);
    harness.processBothWatermarks(new Watermark(1));
    assertThat(harness.extractOutputValues()).containsExactly(impression);
    TinyActionPath expectedNavigate =
        newActionPath(newAction(1, "content1", "act1", ActionType.NAVIGATE), impression);
    assertThat(harness.getSideOutput(ReduceRedundantTinyImpressions.OUTPUT_ACTION_PATH))
        .contains(new StreamRecord(expectedNavigate, 1));
    assertThat(harness.getSideOutput(ReduceRedundantTinyImpressions.REDUNDANT_IMPRESSION))
        .containsExactly(new StreamRecord(newRedundantImpression(1, "imp2", "imp1"), 1));
    assertThat(harness.numEventTimeTimers()).isEqualTo(1);
    harness.processBothWatermarks(new Watermark(1 + ttl.toMillis()));
  }

  @Test
  public void veryOutOfOrder() throws Exception {
    TinyJoinedImpression impression1 = newImpression(500, "ins1", "imp1");
    TinyJoinedImpression impression2 = newImpression(1000, "ins1", "imp2");

    TinyActionPath inputNavigate1 =
        newActionPath(newAction(0, "cont1", "act1", ActionType.NAVIGATE), impression2);

    harness.processElement2(inputNavigate1, 0);
    harness.processElement1(impression1, 500);
    harness.processElement1(impression2, 1000);

    // Events are not outputted at 0 because inputNavigate1 has a touchpoint that's out of order.
    harness.processBothWatermarks(new Watermark(0));
    assertThat(harness.extractOutputValues()).isEmpty();
    assertNull(harness.getSideOutput(ReduceRedundantTinyImpressions.OUTPUT_ACTION_PATH));

    // Even though we progress the watermark to 70000, the first timer is for 0.  We don't have the
    // impressions processed yet so inputNavigate1 is outputted unmodified.
    harness.processBothWatermarks(new Watermark(70000));
    assertThat(harness.extractOutputValues()).containsExactly(impression1);

    TinyActionPath expectedNavigate1 =
        newActionPath(newAction(0, "cont1", "act1", ActionType.NAVIGATE), impression1);
    assertThat(harness.getSideOutput(ReduceRedundantTinyImpressions.OUTPUT_ACTION_PATH))
        .containsExactly(new StreamRecord<>(expectedNavigate1, 1000));

    assertThat(harness.getSideOutput(ReduceRedundantTinyImpressions.REDUNDANT_IMPRESSION))
        .containsExactly(new StreamRecord(newRedundantImpression(1000, "imp2", "imp1"), 1000));

    assertThat(harness.numEventTimeTimers()).isEqualTo(1);
    harness.processBothWatermarks(new Watermark(1000 + ttl.toMillis()));
  }

  @Test
  public void multipleImpressionsAndActions() throws Exception {
    TinyJoinedImpression impression1 = newImpression(500, "ins1", "imp1");
    TinyJoinedImpression impression2 = newImpression(1000, "ins1", "imp2");
    TinyJoinedImpression impression3 = newImpression(6000, "ins1", "imp3");
    TinyJoinedImpression impression4 = newImpression(60000, "ins1", "imp4");

    TinyActionPath inputNavigate1 =
        newActionPath(newAction(1500, "cont1", "act1", ActionType.NAVIGATE), impression2);
    TinyActionPath inputNavigate2 =
        newActionPath(newAction(12000, "cont1", "act2", ActionType.NAVIGATE), impression1);
    TinyActionPath inputNegotiate =
        newActionPath(
            newAction(70000, "cont1", "act2", ActionType.CUSTOM_ACTION_TYPE, "negotiate"),
            impression4);

    harness.processElement1(impression1, 500);
    harness.processElement1(impression2, 1000);
    harness.processElement2(inputNavigate1, 1500);
    harness.processElement1(impression3, 6000);
    harness.processElement2(inputNavigate2, 12000);
    harness.processElement1(impression4, 60000);
    harness.processElement2(inputNegotiate, 70000);

    TinyActionPath expectedNavigate1 =
        newActionPath(newAction(1500, "cont1", "act1", ActionType.NAVIGATE), impression1);
    TinyActionPath expectedNavigate2 = inputNavigate2;
    TinyActionPath expectedNegotiate =
        newActionPath(
            newAction(70000, "cont1", "act2", ActionType.CUSTOM_ACTION_TYPE, "negotiate"),
            impression1);

    harness.processBothWatermarks(new Watermark(70000));
    assertThat(harness.extractOutputValues()).containsExactly(impression1);
    assertThat(harness.getSideOutput(ReduceRedundantTinyImpressions.OUTPUT_ACTION_PATH))
        .containsExactly(
            new StreamRecord<>(expectedNavigate1, 1500),
            new StreamRecord<>(expectedNavigate2, 12000),
            new StreamRecord<>(expectedNegotiate, 70000));
    assertThat(harness.getSideOutput(ReduceRedundantTinyImpressions.REDUNDANT_IMPRESSION))
        .containsExactly(
            new StreamRecord(newRedundantImpression(1000, "imp2", "imp1"), 1000),
            new StreamRecord(newRedundantImpression(6000, "imp3", "imp1"), 6000),
            new StreamRecord(newRedundantImpression(60000, "imp4", "imp1"), 60000));

    assertThat(harness.numEventTimeTimers()).isEqualTo(1);
    harness.processBothWatermarks(new Watermark(70000 + ttl.toMillis()));
  }

  @Test
  public void impressionInTtl() throws Exception {
    TinyJoinedImpression impression1 = newImpression(0, "ins1", "imp1");
    TinyJoinedImpression impression2 = newImpression(3599999, "ins1", "imp2");
    TinyActionPath inputNavigate =
        newActionPath(newAction(3600005, "cont1", "act1", ActionType.NAVIGATE), impression2);

    harness.processElement1(impression1, 0);
    harness.processElement1(impression2, 3599999);
    harness.processElement2(inputNavigate, 3600005);

    TinyActionPath expectedNavigate =
        newActionPath(newAction(3600005, "cont1", "act1", ActionType.NAVIGATE), impression1);

    harness.processBothWatermarks(new Watermark(3600005));
    assertThat(harness.extractOutputValues()).containsExactly(impression1);
    assertThat(harness.getSideOutput(ReduceRedundantTinyImpressions.OUTPUT_ACTION_PATH))
        .containsExactly(new StreamRecord<>(expectedNavigate, 3600005));
    assertThat(harness.getSideOutput(ReduceRedundantTinyImpressions.REDUNDANT_IMPRESSION))
        .containsExactly(
            new StreamRecord(newRedundantImpression(3599999, "imp2", "imp1"), 3599999));
    assertThat(harness.numEventTimeTimers()).isEqualTo(1);
    harness.processBothWatermarks(new Watermark(3600005 + ttl.toMillis()));
  }

  @Test
  public void impressionOutOfTtl() throws Exception {
    TinyJoinedImpression impression1 = newImpression(0, "ins1", "imp1");
    // The state clears up 1ms before 3600000ms.
    TinyJoinedImpression impression2 = newImpression(3600000, "ins1", "imp2");
    TinyActionPath navigate =
        newActionPath(newAction(3600005, "cont1", "act1", ActionType.NAVIGATE), impression2);

    harness.processElement1(impression1, 0);
    harness.processElement1(impression2, 3600000);
    harness.processElement2(navigate, 3600005);

    harness.processBothWatermarks(new Watermark(3600005));
    assertThat(harness.extractOutputValues()).containsExactly(impression1, impression2);
    assertThat(harness.getSideOutput(ReduceRedundantTinyImpressions.OUTPUT_ACTION_PATH))
        .containsExactly(new StreamRecord<>(navigate, 3600005));
    assertNull(harness.getSideOutput(ReduceRedundantTinyImpressions.REDUNDANT_IMPRESSION));
    assertThat(harness.numEventTimeTimers()).isEqualTo(1);
    harness.processBothWatermarks(new Watermark(3600005 + ttl.toMillis()));
  }

  private TinyJoinedImpression newImpression(
      long eventApiTimestamp, String insertionId, String impressionId) {
    return newImpression(eventApiTimestamp, insertionId, impressionId, "paging1", 0L);
  }

  private TinyJoinedImpression newImpression(
      long eventApiTimestamp,
      String insertionId,
      String impressionId,
      String pagingId,
      long position) {
    return TinyJoinedImpression.newBuilder()
        .setImpression(
            TinyImpression.newBuilder()
                .setCommon(newCommonInfo(eventApiTimestamp))
                .setImpressionId(impressionId))
        .setInsertion(
            TinyInsertion.newBuilder()
                .setPagingId(pagingId)
                .setCore(
                    TinyInsertionCore.newBuilder()
                        .setInsertionId(insertionId)
                        .setPosition(position)))
        .build();
  }

  private TinyActionPath newActionPath(TinyAction action, TinyJoinedImpression joinedImpression) {
    return TinyActionPath.newBuilder()
        .setAction(action)
        .addTouchpoints(TinyTouchpoint.newBuilder().setJoinedImpression(joinedImpression))
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

  private RedundantImpression newRedundantImpression(
      long eventApiTimestamp, String redundantImpressionId, String impressionId) {
    return RedundantImpression.newBuilder()
        .setPlatformId(1)
        .setEventApiTimestamp(eventApiTimestamp)
        .setRedundantImpressionId(redundantImpressionId)
        .setImpressionId(impressionId)
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
