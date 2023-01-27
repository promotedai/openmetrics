package ai.promoted.metrics.logprocessor.common.functions.redundantimpression;

import ai.promoted.proto.event.TinyEvent;
import com.google.common.collect.ImmutableList;
import com.twitter.chill.protobuf.ProtobufSerializer;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RedundantImpressionKeyTest {

    @Test
    public void fromImpressionWithInsertionId() throws Exception {
        assertEquals(
                Tuple4.of(1L, "logUserId", "ins1", ""),
                RedundantImpressionKey.of(
                        newBaseEventBuilder()
                                .setViewId("view1")
                                .setInsertionId("ins1")
                                .setImpressionId("imp1")
                                .setContentId("c1")
                                .build()));
    }

    @Test
    public void fromImpressionWithoutInsertionId() throws Exception {
        assertEquals(
                Tuple4.of(1L, "logUserId", "view1", "c1"),
                RedundantImpressionKey.of(
                        newBaseEventBuilder()
                                .setViewId("view1")
                                .setImpressionId("imp1")
                                .setContentId("c1")
                                .build()));
    }

    @Test
    public void fromActionWithInsertionId() throws Exception {
        assertEquals(
                Tuple4.of(1L, "logUserId", "ins1", ""),
                RedundantImpressionKey.of(
                        newBaseEventBuilder()
                                .setViewId("view1")
                                .setInsertionId("ins1")
                                .setImpressionId("imp1")
                                .setContentId("c1")
                                .setActionId("act1")
                                .build()));
    }

    @Test
    public void fromActionWithoutInsertionId() throws Exception {
        assertEquals(
                Tuple4.of(1L, "logUserId", "view1", "c1"),
                RedundantImpressionKey.of(
                        newBaseEventBuilder()
                                .setViewId("view1")
                                .setImpressionId("imp1")
                                .setContentId("c1")
                                .setActionId("act1")
                                .build()));
    }

    private TinyEvent.Builder newBaseEventBuilder() {
        return TinyEvent.newBuilder()
                .setPlatformId(1L)
                .setLogUserId("logUserId");
    }
}
