package ai.promoted.metrics.logprocessor.job.validateenrich;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.promoted.proto.common.Timing;
import ai.promoted.proto.event.Action;
import org.apache.flink.api.common.functions.FilterFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RemoveEventsSegmentUnitTest {

  RemoveEventsSegment segment;

  @BeforeEach
  public void setUp() {
    segment = new RemoveEventsSegment();
  }

  @Test
  public void validate() {
    segment.validateArgs();
    segment = new RemoveEventsSegment();
    segment.customActionTypeToRemoveEventApiTimestampRange.put("custAct", "0,1");
    segment.validateArgs();
    segment = new RemoveEventsSegment();
    segment.customActionTypeToRemoveEventApiTimestampRange.put("custAct", "0");
    assertThrows(IllegalArgumentException.class, () -> segment.validateArgs());
    segment = new RemoveEventsSegment();
    segment.customActionTypeToRemoveEventApiTimestampRange.put("custAct", "a,1");
    assertThrows(IllegalArgumentException.class, () -> segment.validateArgs());
    segment = new RemoveEventsSegment();
    segment.customActionTypeToRemoveEventApiTimestampRange.put("custAct", "0,b");
    assertThrows(IllegalArgumentException.class, () -> segment.validateArgs());
    segment = new RemoveEventsSegment();
    segment.customActionTypeToRemoveEventApiTimestampRange.put("custAct", "0,1,2");
    assertThrows(IllegalArgumentException.class, () -> segment.validateArgs());
  }

  @Test
  public void canRemoveActions() {
    assertFalse(segment.canRemoveActions());
    segment = new RemoveEventsSegment();
    segment.customActionTypeToRemoveEventApiTimestampRange.put("custAct", "0,1");
    assertTrue(segment.canRemoveActions());
  }

  @Test
  public void getKeepActionsPredicate() throws Exception {
    segment.customActionTypeToRemoveEventApiTimestampRange.put("custAct", "100,105");
    FilterFunction<Action> filter = segment.getKeepActionsPredicate();

    assertTrue(filter.filter(newAction("custAct", 99)));
    assertFalse(filter.filter(newAction("custAct", 100)));
    assertFalse(filter.filter(newAction("custAct", 104)));
    assertTrue(filter.filter(newAction("custAct", 105)));
    assertTrue(filter.filter(newAction("custAct2", 99)));
    assertTrue(filter.filter(newAction("custAct2", 102)));
    assertTrue(filter.filter(newAction("custAct2", 105)));
  }

  private Action newAction(String customActionType, long eventApiTimestamp) {
    return Action.newBuilder()
        .setCustomActionType(customActionType)
        .setTiming(Timing.newBuilder().setEventApiTimestamp(eventApiTimestamp))
        .build();
  }
}
