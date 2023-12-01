package ai.promoted.metrics.logprocessor.job.join.common;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.promoted.metrics.logprocessor.job.join.impression.JoinedImpressionComparatorSupplier;
import ai.promoted.proto.event.TinyCommonInfo;
import ai.promoted.proto.event.TinyImpression;
import ai.promoted.proto.event.TinyInsertion;
import ai.promoted.proto.event.TinyInsertionCore;
import ai.promoted.proto.event.TinyJoinedImpression;
import com.twitter.chill.protobuf.ProtobufSerializer;
import java.time.Duration;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FilterToHighestPriorityUnitTest {
  private static final long CLEAN_UP_DELAY_MILLIS = 100;
  private static final String TEST_ANON_USER_ID = "anonUser1";
  private static final String TEST_CONTENT_ID = "con1";
  private static final KeySelector<TinyJoinedImpression, Tuple2<Long, String>>
      prioritizedInsertionKey =
          new KeySelector<>() {
            @Override
            public Tuple2<Long, String> getKey(TinyJoinedImpression p) {
              return Tuple2.of(
                  p.getImpression().getCommon().getPlatformId(),
                  p.getImpression().getImpressionId());
            }
          };

  private FilterToHighestPriority<Tuple2<Long, String>, TinyJoinedImpression, TinyJoinedImpression>
      filter;
  private KeyedOneInputStreamOperatorTestHarness<?, TinyJoinedImpression, TinyJoinedImpression>
      harness;

  @BeforeEach
  public void setUp() throws Exception {
    filter =
        new FilterToHighestPriority<>(
            TypeInformation.of(TinyJoinedImpression.class),
            p -> p,
            JoinedImpressionComparatorSupplier::hasMatchingInsertionId,
            new JoinedImpressionComparatorSupplier(),
            p -> p.getInsertion().getCore().getInsertionId(),
            p -> p.getImpression().getCommon().getEventApiTimestamp(),
            p -> p.getInsertion().getCommon().getEventApiTimestamp(),
            Duration.ofMillis(CLEAN_UP_DELAY_MILLIS));

    harness =
        ProcessFunctionTestHarnesses.forKeyedProcessFunction(
            filter, prioritizedInsertionKey, Types.TUPLE(Types.LONG, Types.STRING));

    harness
        .getExecutionConfig()
        .registerTypeWithKryoSerializer(TinyJoinedImpression.class, ProtobufSerializer.class);
  }

  @AfterEach
  public void tearDown() throws Exception {
    assertThat(harness.numEventTimeTimers()).isEqualTo(0);
    assertNull(filter.readyToClear.value(), "readyToClear");
    assertNull(filter.bestJoinState.value(), "bestJoinState");
  }

  @Test
  public void oneRecord_exactMatch_outputImmediately() throws Exception {
    TinyInsertion insertion = newInsertion("ins1", true, 1);
    TinyImpression impression = newImpression("imp1", "ins1", 1L);
    TinyJoinedImpression input = newJoinedImpression(insertion, impression);

    harness.processElement(input, 1);
    assertThat(harness.extractOutputValues()).containsExactly(input);

    // Check the internal state.
    assertThat(harness.numEventTimeTimers()).isEqualTo(1);
    assertTrue(filter.readyToClear.value(), "readyToClear");
    assertNull(filter.bestJoinState.value(), "bestJoinState");

    harness.processWatermark(1L);
    assertThat(harness.extractOutputValues()).hasSize(1);
    assertThat(harness.numEventTimeTimers()).isEqualTo(1);
    assertTrue(filter.readyToClear.value(), "readyToClear");
    assertNull(filter.bestJoinState.value(), "bestJoinState");

    harness.processWatermark(101L);
  }

  @Test
  public void oneRecord_notExactMatch_outputInTimer() throws Exception {
    TinyInsertion insertion = newInsertion("ins1", true, 1);
    TinyImpression impression = newImpression("imp1", "ins2", 1L);
    TinyJoinedImpression input = newJoinedImpression(insertion, impression);

    harness.processElement(input, 1);
    assertThat(harness.extractOutputValues()).isEmpty();

    harness.processWatermark(1L);
    assertThat(harness.extractOutputValues()).containsExactly(input);

    // Check the internal state.
    assertThat(harness.numEventTimeTimers()).isEqualTo(1);
    assertTrue(filter.readyToClear.value(), "readyToClear");
    assertNull(filter.bestJoinState.value(), "bestJoinState");

    harness.processWatermark(101L);
  }

  @Test
  public void lowPriFollowedByLowerPri() throws Exception {
    TinyInsertion insertion = newInsertion("ins1", true, 1);
    TinyImpression impression = newImpression("imp1", "", 1L);
    TinyJoinedImpression input = newJoinedImpression(insertion, impression);
    TinyInsertion insertion2 = newInsertion("ins2", false, 1);
    TinyJoinedImpression input2 = newJoinedImpression(insertion2, impression);

    harness.processElement(input, 1);
    harness.processElement(input2, 1);
    assertThat(harness.extractOutputValues()).isEmpty();

    harness.processWatermark(1L);
    assertThat(harness.extractOutputValues()).containsExactly(input);
    harness.processWatermark(101L);
  }

  @Test
  public void lowPriorityFollowedByExact() throws Exception {
    TinyInsertion insertion = newInsertion("ins1", true, 1);
    TinyImpression impression = newImpression("imp1", "ins2", 1);
    TinyJoinedImpression input = newJoinedImpression(insertion, impression);
    TinyInsertion insertion2 = newInsertion("ins2", true, 1);
    TinyJoinedImpression input2 = newJoinedImpression(insertion2, impression);

    harness.processElement(input, 1);
    harness.processElement(input2, 1);
    assertThat(harness.extractOutputValues()).containsExactly(input2);

    harness.processWatermark(1L);
    assertThat(harness.extractOutputValues()).hasSize(1);
    harness.processWatermark(101L);
  }

  @Test
  public void lowPriFollowedByHigherPri() throws Exception {
    TinyInsertion insertion = newInsertion("ins1", false, 1);
    TinyImpression impression = newImpression("imp1", "", 1L);
    TinyJoinedImpression input = newJoinedImpression(insertion, impression);
    TinyInsertion insertion2 = newInsertion("ins2", true, 1);
    TinyJoinedImpression input2 = newJoinedImpression(insertion2, impression);

    harness.processElement(input, 1);
    harness.processElement(input2, 1);
    assertThat(harness.extractOutputValues()).isEmpty();

    harness.processWatermark(1L);
    assertThat(harness.extractOutputValues()).containsExactly(input2);
    harness.processWatermark(101L);
  }

  @Test
  public void prevInsBeforeEvent_nextEarlier() throws Exception {
    TinyInsertion insertion = newInsertion("ins1", true, 5);
    TinyImpression impression = newImpression("imp1", "", 10L);
    TinyJoinedImpression input = newJoinedImpression(insertion, impression);
    TinyInsertion insertion2 = newInsertion("ins2", true, 3);
    TinyJoinedImpression input2 = newJoinedImpression(insertion2, impression);

    harness.processElement(input, 10);
    harness.processElement(input2, 10);
    assertThat(harness.extractOutputValues()).isEmpty();

    harness.processWatermark(10L);
    assertThat(harness.extractOutputValues()).containsExactly(input);
    harness.processWatermark(110L);
  }

  @Test
  public void prevInsBeforeEvent_nextLater() throws Exception {
    TinyInsertion insertion = newInsertion("ins1", true, 5);
    TinyImpression impression = newImpression("imp1", "", 10L);
    TinyJoinedImpression input = newJoinedImpression(insertion, impression);
    TinyInsertion insertion2 = newInsertion("ins2", true, 11);
    TinyJoinedImpression input2 = newJoinedImpression(insertion2, impression);

    harness.processElement(input, 10);
    harness.processElement(input2, 10);
    assertThat(harness.extractOutputValues()).isEmpty();

    harness.processWatermark(10L);
    assertThat(harness.extractOutputValues()).containsExactly(input);
    harness.processWatermark(111L);
  }

  @Test
  public void prevInsBeforeEvent_nextCloser() throws Exception {
    TinyInsertion insertion = newInsertion("ins1", true, 5);
    TinyImpression impression = newImpression("imp1", "", 10L);
    TinyJoinedImpression input = newJoinedImpression(insertion, impression);
    TinyInsertion insertion2 = newInsertion("ins2", true, 8);
    TinyJoinedImpression input2 = newJoinedImpression(insertion2, impression);

    harness.processElement(input, 10);
    harness.processElement(input2, 10);
    assertThat(harness.extractOutputValues()).isEmpty();

    harness.processWatermark(10L);
    assertThat(harness.extractOutputValues()).containsExactly(input2);
    harness.processWatermark(110L);
  }

  @Test
  public void prevInsAfterEvent_nextBefore() throws Exception {
    TinyInsertion insertion = newInsertion("ins1", true, 12);
    TinyImpression impression = newImpression("imp1", "", 10L);
    TinyJoinedImpression input = newJoinedImpression(insertion, impression);
    TinyInsertion insertion2 = newInsertion("ins2", true, 8);
    TinyJoinedImpression input2 = newJoinedImpression(insertion2, impression);

    harness.processElement(input, 10);
    harness.processElement(input2, 10);
    assertThat(harness.extractOutputValues()).isEmpty();

    harness.processWatermark(10L);
    assertThat(harness.extractOutputValues()).containsExactly(input2);
    harness.processWatermark(112L);
  }

  @Test
  public void prevInsAfterEvent_nextCloser() throws Exception {
    TinyInsertion insertion = newInsertion("ins1", true, 12);
    TinyImpression impression = newImpression("imp1", "", 10L);
    TinyJoinedImpression input = newJoinedImpression(insertion, impression);
    TinyInsertion insertion2 = newInsertion("ins2", true, 11);
    TinyJoinedImpression input2 = newJoinedImpression(insertion2, impression);

    harness.processElement(input, 10);
    harness.processElement(input2, 10);
    assertThat(harness.extractOutputValues()).isEmpty();

    harness.processWatermark(10L);
    assertThat(harness.extractOutputValues()).containsExactly(input2);
    harness.processWatermark(112L);
  }

  @Test
  public void prevInsAfterEvent_nextAfter() throws Exception {
    TinyInsertion insertion = newInsertion("ins1", true, 12);
    TinyImpression impression = newImpression("imp1", "", 10L);
    TinyJoinedImpression input = newJoinedImpression(insertion, impression);
    TinyInsertion insertion2 = newInsertion("ins2", true, 13);
    TinyJoinedImpression input2 = newJoinedImpression(insertion2, impression);

    harness.processElement(input, 10);
    harness.processElement(input2, 10);
    assertThat(harness.extractOutputValues()).isEmpty();

    harness.processWatermark(10L);
    assertThat(harness.extractOutputValues()).containsExactly(input);
    harness.processWatermark(113L);
  }

  @Test
  public void eventTimeSameAsParentTime() throws Exception {
    TinyInsertion insertion = newInsertion("ins1", true, 10);
    TinyImpression impression = newImpression("imp1", "", 10L);
    TinyJoinedImpression input = newJoinedImpression(insertion, impression);
    TinyInsertion insertion2 = newInsertion("ins2", true, 8);
    TinyJoinedImpression input2 = newJoinedImpression(insertion2, impression);

    harness.processElement(input, 10);
    harness.processElement(input2, 10);
    assertThat(harness.extractOutputValues()).isEmpty();

    harness.processWatermark(10L);
    assertThat(harness.extractOutputValues()).containsExactly(input);
    harness.processWatermark(110L);
  }

  @Test
  public void sameTimes_prioritizeByInsertionId() throws Exception {
    TinyInsertion insertion = newInsertion("ins1", true, 8);
    TinyImpression impression = newImpression("imp1", "", 10L);
    TinyJoinedImpression input = newJoinedImpression(insertion, impression);
    TinyInsertion insertion2 = newInsertion("ins2", true, 8);
    TinyJoinedImpression input2 = newJoinedImpression(insertion2, impression);

    harness.processElement(input, 10);
    harness.processElement(input2, 10);
    assertThat(harness.extractOutputValues()).isEmpty();

    harness.processWatermark(10L);
    assertThat(harness.extractOutputValues()).containsExactly(input);
    harness.processWatermark(110L);
  }

  @Test
  public void outputLateRecord_lateParent() throws Exception {
    TinyInsertion insertion = newInsertion("ins1", true, 30L);
    TinyImpression impression = newImpression("imp1", "ins2", 1L);
    TinyJoinedImpression input = newJoinedImpression(insertion, impression);

    harness.processWatermark(50L);
    assertThat(harness.extractOutputValues()).isEmpty();

    harness.processElement(input, 1);
    assertThat(harness.extractOutputValues()).isEmpty();

    harness.processWatermark(51L);
    assertThat(harness.extractOutputValues()).containsExactly(input);

    harness.processWatermark(101L);
  }

  @Test
  public void outputLateRecord_lateChild() throws Exception {
    TinyInsertion insertion = newInsertion("ins1", true, 1L);
    TinyImpression impression = newImpression("imp1", "ins2", 30L);
    TinyJoinedImpression input = newJoinedImpression(insertion, impression);

    harness.processWatermark(50L);
    assertThat(harness.extractOutputValues()).isEmpty();

    harness.processElement(input, 30);
    assertThat(harness.extractOutputValues()).isEmpty();

    harness.processWatermark(51L);
    assertThat(harness.extractOutputValues()).containsExactly(input);

    harness.processWatermark(131L);
  }

  @Test
  public void ignoreLateEvent_lateParent() throws Exception {
    TinyInsertion insertion = newInsertion("ins1", true, 1);
    TinyImpression impression = newImpression("imp1", "ins2", 1L);
    TinyJoinedImpression input = newJoinedImpression(insertion, impression);

    harness.processElement(input, 1);
    assertThat(harness.extractOutputValues()).isEmpty();

    harness.processWatermark(1L);
    assertThat(harness.extractOutputValues()).containsExactly(input);

    TinyInsertion insertion2 = newInsertion("ins2", true, 30L);
    TinyJoinedImpression input2 = newJoinedImpression(insertion2, impression);
    harness.processElement(input2, 1);

    harness.processWatermark(31L);
    assertThat(harness.extractOutputValues()).hasSize(1);

    harness.processWatermark(101L);
  }

  @Test
  public void ignoreLateEvent_lateChild() throws Exception {
    TinyInsertion insertion = newInsertion("ins1", true, 1);
    TinyImpression impression = newImpression("imp1", "ins2", 1L);
    TinyJoinedImpression input = newJoinedImpression(insertion, impression);

    harness.processElement(input, 1);
    assertThat(harness.extractOutputValues()).isEmpty();

    harness.processWatermark(1L);
    assertThat(harness.extractOutputValues()).containsExactly(input);

    // In case something breaks up stream and this operator get duplicates.
    TinyImpression impression2 = newImpression("imp1", "ins2", 1L);
    TinyJoinedImpression input2 = newJoinedImpression(insertion, impression2);
    harness.processElement(input2, 1);

    harness.processWatermark(31L);
    assertThat(harness.extractOutputValues()).hasSize(1);

    harness.processWatermark(101L);
  }

  @Test
  public void outputVeryDelayedEvent_outsideOfTimeWindow() throws Exception {
    TinyInsertion insertion = newInsertion("ins1", true, 1);
    TinyImpression impression = newImpression("imp1", "ins2", 1L);
    TinyJoinedImpression input = newJoinedImpression(insertion, impression);

    harness.processElement(input, 1);
    assertThat(harness.extractOutputValues()).isEmpty();

    harness.processWatermark(1L);
    assertThat(harness.extractOutputValues()).containsExactly(input);

    // Watermark progressed too much.
    harness.processWatermark(101L);

    // In case something breaks up stream and this operator get duplicates.
    TinyImpression impression2 = newImpression("imp1", "ins2", 30L);
    TinyJoinedImpression input2 = newJoinedImpression(insertion, impression2);
    harness.processElement(input2, 1);

    harness.processWatermark(31L);
    assertThat(harness.extractOutputValues()).containsExactly(input, input2);

    harness.processWatermark(101L);
  }

  // otherContentId = used to create lower priority TinyInsertions.
  private static TinyInsertion newInsertion(
      String insertionId, boolean contentIdOnAction, long time) {
    TinyInsertion.Builder builder =
        TinyInsertion.newBuilder()
            .setCommon(
                TinyCommonInfo.newBuilder()
                    .setAnonUserId(TEST_ANON_USER_ID)
                    .setEventApiTimestamp(time))
            .setCore(TinyInsertionCore.newBuilder().setInsertionId(insertionId));
    if (contentIdOnAction) {
      builder.getCoreBuilder().setContentId(TEST_CONTENT_ID);
    } else {
      builder.getCoreBuilder().putOtherContentIds(1, TEST_CONTENT_ID);
    }
    return builder.build();
  }

  private static TinyImpression newImpression(String impressionId, String insertionId, long time) {
    return TinyImpression.newBuilder()
        .setCommon(
            TinyCommonInfo.newBuilder().setAnonUserId(TEST_ANON_USER_ID).setEventApiTimestamp(time))
        .setImpressionId(impressionId)
        .setInsertionId(insertionId)
        .setContentId(TEST_CONTENT_ID)
        .build();
  }

  private static TinyJoinedImpression newJoinedImpression(
      TinyInsertion insertion, TinyImpression impression) {
    return TinyJoinedImpression.newBuilder()
        .setInsertion(insertion)
        .setImpression(impression)
        .build();
  }
}
