package ai.promoted.metrics.logprocessor.common.functions.redundantimpression;

import ai.promoted.proto.event.TinyEvent;
import com.google.common.collect.ImmutableList;
import com.twitter.chill.protobuf.ProtobufSerializer;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReduceRedundantTinyImpressionTest {

    private static final Duration ttl = Duration.ofHours(1);

    private ReduceRedundantTinyImpressions reduceRedundantTinyImpressions;
    private KeyedOneInputStreamOperatorTestHarness harness;

    @BeforeEach
    public void setUp() throws Exception {
        reduceRedundantTinyImpressions = new ReduceRedundantTinyImpressions(ttl);
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                reduceRedundantTinyImpressions,
                RedundantImpressionKey::of,
                Types.TUPLE(Types.LONG, Types.STRING, Types.STRING, Types.STRING));
        harness.setTimeCharacteristic(TimeCharacteristic.EventTime);
        harness.getExecutionConfig().registerTypeWithKryoSerializer(
                TinyEvent.class, ProtobufSerializer.class);
    }

    private void assertEmptyFunctionState() throws Exception {
        assertEquals(0, harness.numEventTimeTimers());
        assertNull(reduceRedundantTinyImpressions.reducedImpression.value());
        assertNull(reduceRedundantTinyImpressions.replaceReducedImpressionAfter.value());
        assertFalse(((Iterable) reduceRedundantTinyImpressions.outOfOrderActions.get()).iterator().hasNext());
        assertNull(reduceRedundantTinyImpressions.latestCleanupTime.value());
    }

    @Test
    public void noEvents() throws Exception {
        harness.processWatermark(new Watermark(0));
        assertEquals(ImmutableList.of(), harness.extractOutputValues());
    }

    @Test
    public void oneImpression() throws Exception {
        harness.processElement(newImpression(0, "ins1", "imp1"), 0);
        assertEquals(ImmutableList.of(newImpression(0, "ins1", "imp1")),
                harness.extractOutputValues());
        assertEquals(1, harness.numEventTimeTimers());
        harness.processWatermark(new Watermark(ttl.toMillis() * 2));
        assertEmptyFunctionState();
    }

    @Test
    public void redundantImpressions() throws Exception {
        harness.processElement(newImpression(0, "ins1", "imp1"), 0);
        harness.processElement(newImpression(1000, "ins1", "imp2"), 1000);
        harness.processElement(newImpression(6000, "ins1", "imp3"), 6000);
        harness.processElement(newImpression(60000, "ins1", "imp4"), 60000);
        assertEquals(ImmutableList.of(newImpression(0, "ins1", "imp1")),
                harness.extractOutputValues());
        assertEquals(1, harness.numEventTimeTimers());
        harness.processWatermark(new Watermark(ttl.toMillis() * 2));
        assertEmptyFunctionState();
    }

    @Test
    public void redundantImpressionsOutOfOrder() throws Exception {
        harness.processElement(newImpression(1000, "ins1", "imp2"), 1000);
        harness.processElement(newImpression(0, "ins1", "imp1"), 0);
        harness.processElement(newImpression(6000, "ins1", "imp3"), 6000);
        harness.processElement(newImpression(60000, "ins1", "imp4"), 60000);
        assertEquals(ImmutableList.of(newImpression(1000, "ins1", "imp2")),
                harness.extractOutputValues());
        assertEquals(1, harness.numEventTimeTimers());
        harness.processWatermark(new Watermark(ttl.toMillis() * 2));
        assertEmptyFunctionState();
    }

    @Test
    public void oneActionNoImpression() throws Exception {
        harness.processElement(newAction(0, "ins1", "imp1", "act1"), 0);
        assertEquals(ImmutableList.of(), harness.extractOutputValues());
        assertEquals(1, harness.numEventTimeTimers());
        // The ttl setting should clear this out.
        assertTrue(((Iterable) reduceRedundantTinyImpressions.outOfOrderActions.get()).iterator().hasNext());

        harness.processWatermark(new Watermark(ttl.toMillis() * 2 - 1));
        assertEquals(ImmutableList.of(), harness.extractOutputValues());
        assertEquals(1, harness.numEventTimeTimers());

        harness.processWatermark(new Watermark(ttl.toMillis() * 2));
        assertEquals(
                ImmutableList.of(newAction(0, "ins1", "imp1", "act1")),
                harness.extractOutputValues());
        assertEquals(0, harness.numEventTimeTimers());
    }

    @Test
    public void oneImpressionAndOneActionInOrder() throws Exception {
        harness.processElement(newImpression(0, "ins1", "imp1"), 0);
        harness.processElement(newAction(0, "ins1", "imp1", "act1"), 0);
        assertEquals(
                ImmutableList.of(
                        newImpression(0, "ins1", "imp1"),
                        newAction(0, "ins1", "imp1", "act1")),
                harness.extractOutputValues());
        assertEquals(1, harness.numEventTimeTimers());
        harness.processWatermark(new Watermark(ttl.toMillis() * 2));
        assertEmptyFunctionState();
    }

    @Test
    public void oneImpressionAndOneActionOutOfOrder() throws Exception {
        harness.processElement(newAction(0, "ins1", "imp1", "act1"), 0);
        harness.processElement(newImpression(0, "ins1", "imp1"), 0);
        assertEquals(
                ImmutableList.of(
                        newImpression(0, "ins1", "imp1"),
                        newAction(0, "ins1", "imp1", "act1")),
                harness.extractOutputValues());
        assertEquals(1, harness.numEventTimeTimers());
        harness.processWatermark(new Watermark(ttl.toMillis() * 2));
        assertEmptyFunctionState();
    }

    @Test
    public void multipleImpressionsAndActions() throws Exception {
        harness.processElement(newAction(0, "ins1", "imp2", "act1"), 0);
        harness.processElement(newImpression(500, "ins1", "imp1"), 500);
        harness.processElement(newImpression(1000, "ins1", "imp2"), 1000);
        harness.processElement(newImpression(6000, "ins1", "imp3"), 6000);
        harness.processElement(newAction(12000, "ins1", "imp1", "act2"), 12000);
        harness.processElement(newImpression(60000, "ins1", "imp4"), 60000);
        harness.processElement(newAction(70000, "ins1", "imp4", "act3"), 70000);
        assertEquals(
                ImmutableList.of(
                        newImpression(500, "ins1", "imp1"),
                        // imp2 was changed to imp1.
                        newAction(0, "ins1", "imp1", "act1"),
                        newAction(12000, "ins1", "imp1", "act2"),
                        // imp4 was changed to imp1.
                        newAction(70000, "ins1", "imp1", "act3")),
                harness.extractOutputValues());
        assertEquals(1, harness.numEventTimeTimers());
        harness.processWatermark(new Watermark(ttl.toMillis() * 2));
        assertEmptyFunctionState();
    }

    @Test
    public void impressionInTtl() throws Exception {
        harness.processElement(newImpression(0, "ins1", "imp1"), 0);
        harness.processElement(newImpression(3600000, "ins1", "imp2"), 3600000);
        harness.processElement(newAction(3600005, "ins1", "imp2", "act1"), 3600005);
        assertEquals(
                ImmutableList.of(
                        newImpression(0, "ins1", "imp1"),
                        newAction(3600005, "ins1", "imp1", "act1")),
                harness.extractOutputValues());
        assertEquals(1, harness.numEventTimeTimers());
        harness.processWatermark(new Watermark(ttl.toMillis() * 3));
        assertEmptyFunctionState();
    }

    @Test
    public void impressionOutOfTtl() throws Exception {
        harness.processElement(newImpression(0, "ins1", "imp1"), 0);
        harness.processElement(newImpression(3600001, "ins1", "imp2"), 3600001);
        harness.processElement(newAction(3600005, "ins1", "imp2", "act1"), 3600005);
        assertEquals(
                ImmutableList.of(
                        newImpression(0, "ins1", "imp1"),
                        newImpression(3600001, "ins1", "imp2"),
                        newAction(3600005, "ins1", "imp2", "act1")),
                harness.extractOutputValues());
        assertEquals(2, harness.numEventTimeTimers());
        harness.processWatermark(new Watermark(ttl.toMillis() * 3));
        assertEmptyFunctionState();
    }

    // One possible test case, actionOutOf2xTtl(), cannot be test because NeverReturnExpired doesn't seem to work in
    // tests.

    private TinyEvent newImpression(long logTimestamp, String insertionId, String impressionId) {
        return newBaseEventBuilder()
                .setLogTimestamp(logTimestamp)
                .setInsertionId(insertionId)
                .setImpressionId(impressionId)
                .build();
    }

    private TinyEvent newAction(long logTimestamp, String insertionId, String impressionId, String actionId) {
        return newBaseEventBuilder()
                .setLogTimestamp(logTimestamp)
                .setInsertionId(insertionId)
                .setImpressionId(impressionId)
                .setActionId(actionId)
                .build();
    }

    private TinyEvent.Builder newBaseEventBuilder() {
        return TinyEvent.newBuilder()
                .setPlatformId(1L)
                .setLogUserId("logUserId");
    }
}
