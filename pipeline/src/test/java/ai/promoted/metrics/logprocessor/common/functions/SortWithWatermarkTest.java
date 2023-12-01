package ai.promoted.metrics.logprocessor.common.functions;

import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SortWithWatermarkTest {

  private KeyedOneInputStreamOperatorTestHarness<
          SortWithWatermark.ByteArrayKey, String, Tuple2<byte[], String>>
      createHarness(SortWithWatermark<String> sortWithWatermark) throws Exception {
    KeyedOneInputStreamOperatorTestHarness<
            SortWithWatermark.ByteArrayKey, String, Tuple2<byte[], String>>
        harness =
            new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(sortWithWatermark),
                (KeySelector<String, SortWithWatermark.ByteArrayKey>)
                    e -> new SortWithWatermark.ByteArrayKey(new byte[] {0}),
                TypeInformation.of(SortWithWatermark.ByteArrayKey.class));
    harness.setStateBackend(new EmbeddedRocksDBStateBackend());
    harness.open();
    return harness;
  }

  @Test
  public void testSortingEvents() throws Exception {
    SortWithWatermark<String> sortWithWatermark =
        new SortWithWatermark<>(Types.STRING, false, null);
    try (KeyedOneInputStreamOperatorTestHarness<
            SortWithWatermark.ByteArrayKey, String, Tuple2<byte[], String>>
        harness = createHarness(sortWithWatermark)) {
      harness.processElement("4", 4);
      harness.processElement("3", 3);
      Assertions.assertEquals(3, sortWithWatermark.nextTimer.value());
      Assertions.assertEquals(1, harness.numEventTimeTimers());
      harness.processElement("1", 1);
      harness.processElement("2", 2);
      // The next timer was updated to be 1.
      Assertions.assertEquals(1, sortWithWatermark.nextTimer.value());
      Assertions.assertEquals(1, harness.numEventTimeTimers());
      harness.processWatermark(3);
      Assertions.assertEquals(
          "[([0],1), ([0],2), ([0],3)]", harness.extractOutputValues().toString());
      // The next timer was updated to be 4.
      Assertions.assertEquals(4, sortWithWatermark.nextTimer.value());
      Assertions.assertEquals(1, harness.numEventTimeTimers());
      harness.processWatermark(5);
      Assertions.assertEquals(
          "[Record @ 1 : ([0],1), Record @ 2 : ([0],2), Record @ 3 : ([0],3), Record @ 4 : ([0],4)]",
          harness.extractOutputStreamRecords().toString());
    }
  }

  @Test
  public void testOutputLateEvents() throws Exception {
    SortWithWatermark<String> sortWithWatermark =
        new SortWithWatermark<>(Types.STRING, false, null);
    try (KeyedOneInputStreamOperatorTestHarness<
            SortWithWatermark.ByteArrayKey, String, Tuple2<byte[], String>>
        harness = createHarness(sortWithWatermark)) {
      harness.processElement("5", 5);
      harness.processElement("1", 1);
      harness.processWatermark(5);
      harness.processElement("2", 2);
      Assertions.assertEquals(
          "[Record @ 1 : ([0],1), Record @ 5 : ([0],5), Record @ 2 : ([0],2)]",
          harness.extractOutputStreamRecords().toString());
    }
  }

  @Test
  public void testSideOutputLateEvents() throws Exception {
    OutputTag<String> lateEvent = new OutputTag<>("late-event", Types.STRING);
    SortWithWatermark<String> sortWithWatermark =
        new SortWithWatermark<>(Types.STRING, true, lateEvent);
    try (KeyedOneInputStreamOperatorTestHarness<
            SortWithWatermark.ByteArrayKey, String, Tuple2<byte[], String>>
        harness = createHarness(sortWithWatermark)) {
      harness.processElement("5", 5);
      harness.processElement("1", 1);
      harness.processWatermark(5);
      harness.processElement("2", 2);
      Assertions.assertEquals(
          "[Record @ 1 : ([0],1), Record @ 5 : ([0],5)]",
          harness.extractOutputStreamRecords().toString());
      ConcurrentLinkedQueue<StreamRecord<String>> queue = harness.getSideOutput(lateEvent);
      Assertions.assertEquals("Record @ 2 : 2", Objects.requireNonNull(queue.poll()).toString());
    }
  }
}
