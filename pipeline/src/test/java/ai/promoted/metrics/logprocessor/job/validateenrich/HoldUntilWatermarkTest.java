package ai.promoted.metrics.logprocessor.job.validateenrich;

import static com.google.common.truth.Truth.assertThat;

import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.EnrichmentUnion;
import ai.promoted.proto.event.Impression;
import com.twitter.chill.protobuf.ProtobufSerializer;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class HoldUntilWatermarkTest {
  private HoldUntilWatermark fn;
  private KeyedOneInputStreamOperatorTestHarness harness;

  @BeforeEach
  public void setUp() throws Exception {
    fn = new HoldUntilWatermark();
    harness =
        ProcessFunctionTestHarnesses.forKeyedProcessFunction(
            fn, EnrichmentUnionUtil::toAnonUserIdKey, Types.TUPLE(Types.LONG, Types.STRING));
    harness
        .getExecutionConfig()
        .registerTypeWithKryoSerializer(EnrichmentUnion.class, ProtobufSerializer.class);
  }

  private void assertEmptyFunctionState() throws Exception {
    assertThat(harness.numEventTimeTimers()).isEqualTo(0);
    assertThat(fn.timeToUnion.isEmpty()).isTrue();
  }

  @Test
  public void delayOneEvent() throws Exception {
    Impression impression = createImpression("imp1");
    harness.processElement(EnrichmentUnionUtil.toImpressionUnion(impression), 1);
    harness.processWatermark(0);
    assertThat(harness.extractOutputValues()).isEmpty();
    harness.processWatermark(1);
    assertThat(harness.extractOutputValues())
        .contains(EnrichmentUnionUtil.toImpressionUnion(impression));
    assertEmptyFunctionState();
  }

  @Test
  public void delayMultipleEvents() throws Exception {
    Impression impression1 = createImpression("imp1");
    Impression impression2 = createImpression("imp2");
    Impression impression3 = createImpression("imp3");
    harness.processElement(EnrichmentUnionUtil.toImpressionUnion(impression1), 1);
    harness.processElement(EnrichmentUnionUtil.toImpressionUnion(impression2), 1);
    harness.processElement(EnrichmentUnionUtil.toImpressionUnion(impression3), 2);
    harness.processWatermark(0);
    assertThat(harness.extractOutputValues()).isEmpty();
    harness.processWatermark(1);
    assertThat(harness.extractOutputValues())
        .contains(EnrichmentUnionUtil.toImpressionUnion(impression1));
    assertThat(harness.extractOutputValues())
        .contains(EnrichmentUnionUtil.toImpressionUnion(impression2));
    assertThat(harness.extractOutputValues())
        .doesNotContain(EnrichmentUnionUtil.toImpressionUnion(impression3));
    harness.processWatermark(2);
    assertThat(harness.extractOutputValues())
        .contains(EnrichmentUnionUtil.toImpressionUnion(impression3));
    assertEmptyFunctionState();
  }

  private Impression createImpression(String impressionId) {
    return Impression.newBuilder()
        .setPlatformId(1L)
        .setUserInfo(UserInfo.newBuilder().setAnonUserId("anonUserId1").setUserId("userId").build())
        .setImpressionId(impressionId)
        .build();
  }
}
