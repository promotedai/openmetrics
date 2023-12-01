package ai.promoted.metrics.logprocessor.common.functions;

import ai.promoted.metrics.logprocessor.common.functions.base.SerializableBiFunction;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableFunction;
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
            false,
            (SerializableFunction<Event, Event>) s -> s);

    try (KeyedTwoInputStreamOperatorTestHarness<String, Event, Event, Event> harness =
        createHarness(function)) {

      List<StreamRecord<Event>> expected = new ArrayList<>();
      // Output left outer join result
      harness.processElement1(new Event("k1", "f1-"), 1);
      harness.processElement1(new Event("k1", "f2-"), 2);
      harness.processElement2(new Event("k1", "d3"), 3); // won't be joined with f1 and f2
      Assertions.assertEquals(1, harness.numEventTimeTimers()); // only 1 timer for two fact events

      harness.processBothWatermarks(new Watermark(2));
      expected.add(new StreamRecord<>(new Event("k1", "f1-"), 1));
      expected.add(new StreamRecord<>(new Event("k1", "f2-"), 2));
      Assertions.assertEquals(0, harness.numEventTimeTimers());
      Assertions.assertEquals(expected, harness.extractOutputStreamRecords());

      harness.processElement1(new Event("k1", "f4.1-"), 4);
      harness.processElement1(new Event("k1", "f4.2-"), 4);
      Assertions.assertEquals(1, harness.numEventTimeTimers());
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
  public void testInnerEagerJoin() throws Exception {
    TemporalJoinFunction<String, Event, Event, Event> function =
        new TemporalJoinFunction<>(
            TypeInformation.of(Event.class),
            TypeInformation.of(Event.class),
            (SerializableBiFunction<Event, Event, Event>)
                (s1, s2) -> new Event(s1.f0, s1.f1 + s2.f1),
            true,
            null); // leave null for inner join

    try (KeyedTwoInputStreamOperatorTestHarness<String, Event, Event, Event> harness =
        createHarness(function)) {
      harness.processElement1(new Event("k1", "f1-"), 1);
      harness.processElement1(new Event("k1", "f2-"), 2);
      harness.processElement2(new Event("k1", "d3"), 3);
      harness.processBothWatermarks(new Watermark(2));
      List<StreamRecord<Event>> expected = new ArrayList<>();
      // Though d3 comes later, f1 and f2 can still join with it since eagerJoin is enabled.
      expected.add(new StreamRecord<>(new Event("k1", "f1-d3"), 1));
      expected.add(new StreamRecord<>(new Event("k1", "f2-d3"), 2));
      Assertions.assertEquals(expected, harness.extractOutputStreamRecords());
    }
  }

  @Test
  public void testInnerJoinWithAllowedFirstDelay() throws Exception {
    long dimensionAllowedDelay = 10;
    TemporalJoinFunction<String, Event, Event, Event> function =
        new TemporalJoinFunction<>(
            TypeInformation.of(Event.class),
            TypeInformation.of(Event.class),
            (SerializableBiFunction<Event, Event, Event>)
                (s1, s2) -> new Event(s1.f0, s1.f1 + s2.f1),
            false,
            null,
            dimensionAllowedDelay); // allow the first dimension data to be delayed for 10ms

    List<StreamRecord<Event>> expected = new ArrayList<>();
    try (KeyedTwoInputStreamOperatorTestHarness<String, Event, Event, Event> harness =
        createHarness(function)) {
      harness.processElement1(new Event("k1", "f2-"), 2);
      harness.processElement1(new Event("k1", "f1-"), 1);
      harness.processElement2(new Event("k1", "d12"), 12);
      harness.processElement1(new Event("k1", "f13-"), 13);
      Assertions.assertEquals(1, harness.numEventTimeTimers());
      // The first timer will be triggered when watermark = 2 + 10 = 12
      harness.processBothWatermarks(new Watermark(11)); // watermark --> 11
      Assertions.assertTrue(harness.extractOutputStreamRecords().isEmpty());

      harness.processBothWatermarks(new Watermark(12)); // watermark --> 12
      // Event("k1", "f1-d11") won't be in the result since 1 + 10 < 12
      expected.add(new StreamRecord<>(new Event("k1", "f2-d12"), 12));
      Assertions.assertEquals(expected, harness.extractOutputStreamRecords());

      // Another timer should be registered for 13
      Assertions.assertEquals(1, harness.numEventTimeTimers());
      harness.processElement2(new Event("k1", "d13"), 13);

      // This is a late record
      harness.processElement1(new Event("k1", "f3-"), 3);
      harness.processBothWatermarks(new Watermark(13)); // watermark --> 13
      // At this time, d12 has not been removed since 12 > 13 - 10
      expected.add(new StreamRecord<>(new Event("k1", "f3-d12"), 13));
      expected.add(new StreamRecord<>(new Event("k1", "f13-d13"), 13));
      Assertions.assertEquals(expected, harness.extractOutputStreamRecords());
      Assertions.assertEquals(0, harness.numEventTimeTimers());

      harness.processElement1(new Event("k1", "f20-"), 20);
      Assertions.assertEquals(1, harness.numEventTimeTimers());
      harness.processBothWatermarks(new Watermark(22)); // watermark --> 22
      expected.add(new StreamRecord<>(new Event("k1", "f20-d13"), 20));
      Assertions.assertEquals(expected, harness.extractOutputStreamRecords());
      Assertions.assertEquals(0, harness.numEventTimeTimers());

      // Another two late fact records
      harness.processElement1(new Event("k1", "f2-"), 2);
      harness.processElement1(new Event("k1", "f5-"), 5);
      Assertions.assertEquals(1, harness.numEventTimeTimers());
      harness.processBothWatermarks(new Watermark(30)); // watermark --> 30
      // They can be joined because d12 is the first dimension event and 12 - 2 <= 10
      expected.add(new StreamRecord<>(new Event("k1", "f2-d12"), 2));
      expected.add(new StreamRecord<>(new Event("k1", "f5-d12"), 5));
      Assertions.assertEquals(expected, harness.extractOutputStreamRecords());

      harness.processElement1(new Event("k1", "f6-"), 6);
      // Luckily, here's a late d5
      harness.processElement2(new Event("k1", "d5"), 5);
      harness.processBothWatermarks(new Watermark(40)); // watermark --> 40
      expected.add(new StreamRecord<>(new Event("k1", "f6-d5"), 6));
      Assertions.assertEquals(expected, harness.extractOutputStreamRecords());

      harness.processElement1(new Event("k1", "f7-"), 7);
      harness.processBothWatermarks(new Watermark(41)); // watermark --> 41
      // so f7 can only be joined with d5
      expected.add(new StreamRecord<>(new Event("k1", "f7-d5"), 7));
      Assertions.assertEquals(expected, harness.extractOutputStreamRecords());
    }
  }

  public static class Event extends Tuple2<String, String> {
    public Event() {
      super();
    }

    public Event(String key, String value) {
      super(key, value);
    }
  }
}
