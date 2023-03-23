package ai.promoted.metrics.logprocessor.common.functions;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TemporalJoinFunctionTest {
  public static class Event extends Tuple2<String, String> {
    public Event() {
      super();
    }

    public Event(String key, String value) {
      super(key, value);
    }
  }

  private KeyedTwoInputStreamOperatorTestHarness<String, Event, Event, Event> createHarness(
      TemporalJoinFunction<String, Event, Event, Event> temporalJoinFunction) throws Exception {
    KeyedTwoInputStreamOperatorTestHarness<String, Event, Event, Event> harness =
        new KeyedTwoInputStreamOperatorTestHarness<>(
            new KeyedCoProcessOperator<>(temporalJoinFunction),
            (KeySelector<Event, String>) e -> e.f0,
            (KeySelector<Event, String>) e -> e.f0,
            Types.STRING);
    harness.setStateBackend(new EmbeddedRocksDBStateBackend());
    harness.open();
    return harness;
  }

  @Test
  public void testLeftOuterJoin() throws Exception {
    TemporalJoinFunction<String, Event, Event, Event> function =
        new TemporalJoinFunction<>(
            TypeInformation.of(Event.class),
            TypeInformation.of(Event.class),
            (SerializableBiFunction<Event, Event, Event>)
                (s1, s2) -> new Event(s1.f0, s1.f1 + s2.f1),
            (SerializableFunction<Event, Event>) s -> s);

    try (KeyedTwoInputStreamOperatorTestHarness<String, Event, Event, Event> harness =
        createHarness(function)) {

      List<StreamRecord<Event>> expected = new ArrayList<>();
      // Output left outer join result
      harness.processElement1(new Event("k1", "f1-"), 1);
      harness.processElement1(new Event("k1", "f2-"), 2);
      harness.processElement2(new Event("k1", "d3"), 3); // won't be joined with f1 and f2
      Assertions.assertEquals(2, harness.numEventTimeTimers()); // 2 timers for two fact events

      harness.processBothWatermarks(new Watermark(2));
      expected.add(new StreamRecord<>(new Event("k1", "f1-"), 1));
      expected.add(new StreamRecord<>(new Event("k1", "f2-"), 2));
      Assertions.assertEquals(0, harness.numEventTimeTimers());
      Assertions.assertEquals(expected, harness.extractOutputStreamRecords());

      harness.processElement1(new Event("k1", "f4.1-"), 4);
      harness.processElement1(new Event("k1", "f4.2-"), 4);
      harness.processBothWatermarks(new Watermark(4));
      expected.add(new StreamRecord<>(new Event("k1", "f4.1-d3"), 4));
      expected.add(new StreamRecord<>(new Event("k1", "f4.2-d3"), 4));
      Assertions.assertEquals(expected, harness.extractOutputStreamRecords());

      harness.processElement1(new Event("k1", "f4.3-"), 4); // late events will still be joined
      harness.processElement2(new Event("k1", "d3"), 3);
      harness.processElement1(new Event("k1", "f10-"), 10);
      harness.processElement1(new Event("k1", "f6-"), 6);
      harness.processElement2(new Event("k1", "d5"), 5);
      harness.processElement2(new Event("k1", "d11"), 11);
      harness.processBothWatermarks(new Watermark(10));
      expected.add(new StreamRecord<>(new Event("k1", "f4.3-d3"), 4));
      expected.add(new StreamRecord<>(new Event("k1", "f6-d5"), 6));
      expected.add(new StreamRecord<>(new Event("k1", "f10-d5"), 10));
      Assertions.assertEquals(expected, harness.extractOutputStreamRecords());
    }
  }

  @Test
  public void testInnerJoin() throws Exception {
    TemporalJoinFunction<String, Event, Event, Event> function =
        new TemporalJoinFunction<>(
            TypeInformation.of(Event.class),
            TypeInformation.of(Event.class),
            (SerializableBiFunction<Event, Event, Event>)
                (s1, s2) -> new Event(s1.f0, s1.f1 + s2.f1),
            null); // leave null for inner join

    try (KeyedTwoInputStreamOperatorTestHarness<String, Event, Event, Event> harness =
        createHarness(function)) {
      harness.processElement1(new Event("k1", "f1-"), 1);
      harness.processElement1(new Event("k1", "f2-"), 2);
      harness.processElement2(new Event("k1", "d3"), 3);
      harness.processBothWatermarks(new Watermark(2));
      Assertions.assertTrue(harness.extractOutputStreamRecords().isEmpty());
    }
  }
}
