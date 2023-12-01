package ai.promoted.metrics.logprocessor.job.validateenrich;

import static com.google.common.truth.Truth.assertThat;

import ai.promoted.proto.common.Timing;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.EnrichmentUnion;
import ai.promoted.proto.event.Impression;
import com.twitter.chill.protobuf.ProtobufSerializer;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JoinRetainedUserIdTest {
  private JoinRetainedUserId fn;
  private KeyedTwoInputStreamOperatorTestHarness harness;

  @BeforeEach
  public void setUp() throws Exception {
    fn = new JoinRetainedUserId();
    harness =
        new KeyedTwoInputStreamOperatorTestHarness<>(
            new KeyedCoProcessOperator<>(fn),
            EnrichmentUnionUtil::toAuthUserIdKey,
            AuthUserIdKey::toKey,
            Types.TUPLE(Types.LONG, Types.STRING));
    harness
        .getExecutionConfig()
        .registerTypeWithKryoSerializer(EnrichmentUnion.class, ProtobufSerializer.class);
    harness.setStateBackend(new EmbeddedRocksDBStateBackend());
    harness.open();
  }

  @AfterEach
  public void tearDown() throws Exception {
    assertThat(harness.numEventTimeTimers()).isEqualTo(0);
    assertThat(fn.timeToDelayedUnions.entries()).isEmpty();
    assertThat(fn.retainedUserId.value()).isNull();
    assertThat(fn.outputTime.value()).isNull();
    assertThat(fn.cleanUp.value()).isNull();
  }

  @Test
  public void oneEventAndRetainedUserIdRow() throws Exception {
    Impression impression =
        Impression.newBuilder()
            .setPlatformId(1L)
            .setUserInfo(UserInfo.newBuilder().setAnonUserId("anonUserId1").setUserId("userId"))
            .setImpressionId("imp1")
            .setTiming(Timing.newBuilder().setLogTimestamp(2L))
            .build();
    harness.processElement1(EnrichmentUnionUtil.toImpressionUnion(impression), 2L);
    harness.processElement2(
        new SelectRetainedUserChange(ChangeKind.UPSERT, 1L, "userId", "retainedUserId"), 2L);
    harness.processBothWatermarks(new Watermark(2L));
    assertThat(harness.extractOutputValues())
        .contains(
            EnrichmentUnionUtil.toImpressionUnion(
                impression.toBuilder()
                    .setUserInfo(
                        UserInfo.newBuilder()
                            .setAnonUserId("anonUserId1")
                            .setRetainedUserId("retainedUserId"))
                    .build()));
    assertThat(harness.numEventTimeTimers()).isEqualTo(0);

    harness.processElement2(
        new SelectRetainedUserChange(ChangeKind.DELETE, 1L, "userId", "retainedUserId"), 4L);
    assertThat(harness.numEventTimeTimers()).isEqualTo(1);
    harness.processBothWatermarks(new Watermark(4L));
  }

  @Test
  public void thenEarlierEvent() throws Exception {
    Impression impression =
        Impression.newBuilder()
            .setPlatformId(1L)
            .setUserInfo(UserInfo.newBuilder().setAnonUserId("anonUserId1").setUserId("userId"))
            .setImpressionId("imp1")
            .setTiming(Timing.newBuilder().setLogTimestamp(2L))
            .build();
    harness.processElement1(EnrichmentUnionUtil.toImpressionUnion(impression), 2L);
    Impression impression2 =
        Impression.newBuilder()
            .setPlatformId(1L)
            .setUserInfo(UserInfo.newBuilder().setAnonUserId("anonUserId1").setUserId("userId"))
            .setImpressionId("imp2")
            .setTiming(Timing.newBuilder().setLogTimestamp(1L))
            .build();
    harness.processElement1(EnrichmentUnionUtil.toImpressionUnion(impression2), 1L);
    harness.processElement2(
        new SelectRetainedUserChange(ChangeKind.UPSERT, 1L, "userId", "retainedUserId"), 2L);
    harness.processBothWatermarks(new Watermark(1L));
    assertThat(harness.extractOutputValues())
        .contains(
            EnrichmentUnionUtil.toImpressionUnion(
                impression2.toBuilder()
                    .setUserInfo(
                        UserInfo.newBuilder()
                            .setAnonUserId("anonUserId1")
                            .setRetainedUserId("retainedUserId"))
                    .build()));
    harness.processBothWatermarks(new Watermark(2L));
    assertThat(harness.extractOutputValues())
        .contains(
            EnrichmentUnionUtil.toImpressionUnion(
                impression.toBuilder()
                    .setUserInfo(
                        UserInfo.newBuilder()
                            .setAnonUserId("anonUserId1")
                            .setRetainedUserId("retainedUserId"))
                    .build()));
    assertThat(harness.numEventTimeTimers()).isEqualTo(0);

    harness.processElement2(
        new SelectRetainedUserChange(ChangeKind.DELETE, 1L, "userId", "retainedUserId"), 4L);
    assertThat(harness.numEventTimeTimers()).isEqualTo(1);
    harness.processBothWatermarks(new Watermark(4L));
  }

  @Test
  public void followUpEvents() throws Exception {
    Impression impression =
        Impression.newBuilder()
            .setPlatformId(1L)
            .setUserInfo(UserInfo.newBuilder().setAnonUserId("anonUserId1").setUserId("userId"))
            .setImpressionId("imp1")
            .setTiming(Timing.newBuilder().setLogTimestamp(2L))
            .build();
    harness.processElement1(EnrichmentUnionUtil.toImpressionUnion(impression), 2L);
    harness.processElement2(
        new SelectRetainedUserChange(ChangeKind.UPSERT, 1L, "userId", "retainedUserId"), 2L);
    harness.processBothWatermarks(new Watermark(2L));
    assertThat(harness.extractOutputValues())
        .contains(
            EnrichmentUnionUtil.toImpressionUnion(
                impression.toBuilder()
                    .setUserInfo(
                        UserInfo.newBuilder()
                            .setAnonUserId("anonUserId1")
                            .setRetainedUserId("retainedUserId"))
                    .build()));

    Impression impression2 =
        Impression.newBuilder()
            .setPlatformId(1L)
            .setUserInfo(UserInfo.newBuilder().setAnonUserId("anonUserId1").setUserId("userId"))
            .setImpressionId("imp2")
            .setTiming(Timing.newBuilder().setLogTimestamp(3L))
            .build();
    harness.processElement1(EnrichmentUnionUtil.toImpressionUnion(impression2), 3L);
    harness.processBothWatermarks(new Watermark(3L));
    assertThat(harness.extractOutputValues())
        .contains(
            EnrichmentUnionUtil.toImpressionUnion(
                impression2.toBuilder()
                    .setUserInfo(
                        UserInfo.newBuilder()
                            .setAnonUserId("anonUserId1")
                            .setRetainedUserId("retainedUserId"))
                    .build()));
    assertThat(harness.numEventTimeTimers()).isEqualTo(0);

    harness.processElement2(
        new SelectRetainedUserChange(ChangeKind.DELETE, 1L, "userId", "retainedUserId"), 4L);
    assertThat(harness.numEventTimeTimers()).isEqualTo(1);
    harness.processBothWatermarks(new Watermark(4L));
  }
}
