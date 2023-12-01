package ai.promoted.metrics.logprocessor.job.join.action;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.promoted.proto.event.TinyAction;
import ai.promoted.proto.event.TinyCommonInfo;
import ai.promoted.proto.event.TinyImpression;
import ai.promoted.proto.event.TinyInsertion;
import ai.promoted.proto.event.TinyInsertionCore;
import ai.promoted.proto.event.TinyJoinedAction;
import ai.promoted.proto.event.TinyJoinedImpression;
import com.google.common.collect.ImmutableList;
import com.twitter.chill.protobuf.ProtobufSerializer;
import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FilterToHighestPriorityListUnitTest {
  private static final long CLEAN_UP_DELAY_MILLIS = 100;
  private static final String TEST_ANON_USER_ID = "anonUser1";
  private static final String TEST_CONTENT_ID = "con1";
  private static final KeySelector<TinyJoinedAction, Tuple2<Long, String>> actionKey =
      new KeySelector<>() {
        @Override
        public Tuple2<Long, String> getKey(TinyJoinedAction p) {
          return Tuple2.of(
              p.getAction().getCommon().getPlatformId(), p.getAction().getImpressionId());
        }
      };

  private FilterToHighestPriorityList<Tuple2<Long, String>, TinyJoinedAction> filter;
  private KeyedOneInputStreamOperatorTestHarness<?, TinyJoinedAction, List<TinyJoinedAction>>
      harness;

  @BeforeEach
  public void setUp() throws Exception {
    filter =
        new FilterToHighestPriorityList<>(
            TypeInformation.of(TinyJoinedAction.class),
            new JoinedImpressionActionComparatorSupplier(),
            p -> p.getJoinedImpression().getImpression().getImpressionId(),
            p -> p.getAction().getCommon().getEventApiTimestamp(),
            p -> p.getJoinedImpression().getImpression().getCommon().getEventApiTimestamp(),
            Duration.ofMillis(CLEAN_UP_DELAY_MILLIS));

    harness =
        ProcessFunctionTestHarnesses.forKeyedProcessFunction(
            filter, actionKey, Types.TUPLE(Types.LONG, Types.STRING));
    harness
        .getExecutionConfig()
        .registerTypeWithKryoSerializer(TinyJoinedAction.class, ProtobufSerializer.class);
  }

  @AfterEach
  public void tearDown() throws Exception {
    assertThat(harness.numEventTimeTimers()).isEqualTo(0);
    assertNull(filter.alreadyExecutedOutputTimer.value(), "alreadyExecutedOutputTimer");
    assertTrue(filter.parentIdToJoin.isEmpty());
  }

  @Test
  public void oneRecord() throws Exception {
    TinyInsertion insertion = newInsertion("ins1", true, 1);
    TinyImpression impression = newImpression("imp1", 1L);
    TinyAction action = newAction("act1", 1L);
    TinyJoinedAction input = newTinyJoinedAction(insertion, impression, action);

    harness.processElement(input, 1);
    assertThat(harness.extractOutputValues()).isEmpty();
    harness.processWatermark(1L);
    assertThat(harness.extractOutputValues()).containsExactly(ImmutableList.of(input));

    // Check the internal state.
    assertThat(harness.numEventTimeTimers()).isEqualTo(1);
    assertTrue(filter.alreadyExecutedOutputTimer.value(), "alreadyExecutedOutputTimer");

    harness.processWatermark(101L);
  }

  @Test
  public void twoRecords_samePriority() throws Exception {
    TinyInsertion insertion1 = newInsertion("ins1", true, 1);
    TinyImpression impression1 = newImpression("imp1", 1L);

    TinyInsertion insertion2 = newInsertion("ins2", true, 2);
    TinyImpression impression2 = newImpression("imp2", 2L);
    TinyAction action = newAction("act1", 2L);
    TinyJoinedAction input1 = newTinyJoinedAction(insertion1, impression1, action);
    TinyJoinedAction input2 = newTinyJoinedAction(insertion2, impression2, action);

    // Both are 2 because of action.rowtime=2
    harness.processElement(input1, 2);
    harness.processElement(input2, 2);
    assertThat(harness.extractOutputValues()).isEmpty();
    harness.processWatermark(2L);
    assertThat(harness.extractOutputValues()).containsExactly(ImmutableList.of(input1, input2));

    harness.processWatermark(102L);
  }

  @Test
  public void twoRecords_laterIsLowerPriority() throws Exception {
    TinyInsertion insertion1 = newInsertion("ins1", true, 1);
    TinyImpression impression1 = newImpression("imp1", 1L);

    TinyInsertion insertion2 = newInsertion("ins2", false, 2);
    TinyImpression impression2 = newImpression("imp2", 2L);
    TinyAction action = newAction("act1", 2L);
    TinyJoinedAction input1 = newTinyJoinedAction(insertion1, impression1, action);
    TinyJoinedAction input2 = newTinyJoinedAction(insertion2, impression2, action);

    // Both are 2 because of action.rowtime=2
    harness.processElement(input1, 2);
    harness.processElement(input2, 2);
    assertThat(harness.extractOutputValues()).isEmpty();
    harness.processWatermark(2L);
    assertThat(harness.extractOutputValues()).containsExactly(ImmutableList.of(input1));

    harness.processWatermark(102L);
  }

  @Test
  public void twoRecords_laterHasHigherPriority() throws Exception {
    TinyInsertion insertion1 = newInsertion("ins1", false, 1);
    TinyImpression impression1 = newImpression("imp1", 1L);

    TinyInsertion insertion2 = newInsertion("ins2", true, 2);
    TinyImpression impression2 = newImpression("imp2", 2L);
    TinyAction action = newAction("act1", 2L);
    TinyJoinedAction input1 = newTinyJoinedAction(insertion1, impression1, action);
    TinyJoinedAction input2 = newTinyJoinedAction(insertion2, impression2, action);

    // Both are 2 because of action.rowtime=2
    harness.processElement(input1, 2);
    harness.processElement(input2, 2);
    assertThat(harness.extractOutputValues()).isEmpty();
    harness.processWatermark(2L);
    assertThat(harness.extractOutputValues()).containsExactly(ImmutableList.of(input2));

    harness.processWatermark(102L);
  }

  @Test
  public void twoRecords_outOfOrderIsLowerPriority() throws Exception {
    TinyInsertion insertion1 = newInsertion("ins1", true, 3L);
    TinyImpression impression1 = newImpression("imp1", 3L);

    TinyInsertion insertion2 = newInsertion("ins2", true, 2);
    TinyImpression impression2 = newImpression("imp2", 2L);
    TinyAction action = newAction("act1", 2L);
    TinyJoinedAction input1 = newTinyJoinedAction(insertion1, impression1, action);
    TinyJoinedAction input2 = newTinyJoinedAction(insertion2, impression2, action);

    // Both are 2 because of action.rowtime=2
    harness.processElement(input1, 2);
    harness.processElement(input2, 2);
    assertThat(harness.extractOutputValues()).isEmpty();
    harness.processWatermark(3L);
    assertThat(harness.extractOutputValues()).containsExactly(ImmutableList.of(input2));

    harness.processWatermark(103L);
  }

  @Test
  public void skipSecondInput_alreadyOutputted() throws Exception {
    TinyInsertion insertion1 = newInsertion("ins1", true, 1);
    TinyImpression impression1 = newImpression("imp1", 1L);

    TinyInsertion insertion2 = newInsertion("ins2", true, 2);
    TinyImpression impression2 = newImpression("imp2", 2L);
    TinyAction action = newAction("act1", 2L);
    TinyJoinedAction input1 = newTinyJoinedAction(insertion1, impression1, action);
    TinyJoinedAction input2 = newTinyJoinedAction(insertion2, impression2, action);

    // Both are 2 because of action.rowtime=2
    harness.processElement(input1, 2);
    assertThat(harness.extractOutputValues()).isEmpty();
    harness.processWatermark(2L);
    assertThat(harness.extractOutputValues()).containsExactly(ImmutableList.of(input1));

    harness.processElement(input2, 2);
    harness.processWatermark(3L);
    assertThat(harness.extractOutputValues()).hasSize(1);

    harness.processWatermark(102L);
  }

  @Test
  public void outputVeryDelayedSecondInput() throws Exception {
    TinyInsertion insertion1 = newInsertion("ins1", true, 1);
    TinyImpression impression1 = newImpression("imp1", 1L);

    TinyInsertion insertion2 = newInsertion("ins2", true, 2);
    TinyImpression impression2 = newImpression("imp2", 2L);
    TinyAction action = newAction("act1", 2L);
    TinyJoinedAction input1 = newTinyJoinedAction(insertion1, impression1, action);
    TinyJoinedAction input2 = newTinyJoinedAction(insertion2, impression2, action);

    // Both are 2 because of action.rowtime=2
    harness.processElement(input1, 2);
    assertThat(harness.extractOutputValues()).isEmpty();
    harness.processWatermark(102L);
    assertThat(harness.extractOutputValues()).containsExactly(ImmutableList.of(input1));

    harness.processElement(input2, 2);
    harness.processWatermark(103L);
    // Note the separate lists.
    assertThat(harness.extractOutputValues())
        .containsExactly(ImmutableList.of(input1), ImmutableList.of(input2));
    harness.processWatermark(103L);
  }

  private TinyJoinedAction newTinyJoinedAction(
      TinyInsertion insertion, TinyImpression impression, TinyAction action) {
    return TinyJoinedAction.newBuilder()
        .setJoinedImpression(newJoinedImpression(insertion, impression))
        .setAction(action)
        .build();
  }

  private static TinyAction newAction(String actionId, long time) {
    return TinyAction.newBuilder()
        .setCommon(newCommonInfo(time))
        .setActionId(actionId)
        .setContentId(TEST_CONTENT_ID)
        .build();
  }

  // otherContentId = used to create lower priority TinyInsertions.
  private static TinyInsertion newInsertion(
      String insertionId, boolean contentIdOnAction, long time) {
    TinyInsertion.Builder builder =
        TinyInsertion.newBuilder()
            .setCommon(newCommonInfo(time))
            .setCore(TinyInsertionCore.newBuilder().setInsertionId(insertionId));
    if (contentIdOnAction) {
      builder.getCoreBuilder().setContentId(TEST_CONTENT_ID);
    } else {
      builder.getCoreBuilder().putOtherContentIds(1, TEST_CONTENT_ID);
    }
    return builder.build();
  }

  private static TinyImpression newImpression(String impressionId, long time) {
    return TinyImpression.newBuilder()
        .setCommon(newCommonInfo(time))
        .setImpressionId(impressionId)
        .build();
  }

  private static TinyJoinedImpression newJoinedImpression(
      TinyInsertion insertion, TinyImpression impression) {
    return TinyJoinedImpression.newBuilder()
        .setInsertion(insertion)
        .setImpression(impression)
        .build();
  }

  private static TinyCommonInfo newCommonInfo(long time) {
    return TinyCommonInfo.newBuilder()
        .setAnonUserId(TEST_ANON_USER_ID)
        .setEventApiTimestamp(time)
        .build();
  }
}
