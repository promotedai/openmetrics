package ai.promoted.metrics.logprocessor.common.functions;

import ai.promoted.proto.event.JoinedEvent;

public class CounterUtil {

  /**
   * Returns the count (metric) to aggregate in the Counter Job. Has some special logic for shopping
   * cart support.
   */
  public static long getCount(JoinedEvent event) {
    if (event.hasAction() && event.getAction().hasSingleCartContent()) {
      return event.getAction().getSingleCartContent().getQuantity();
    } else {
      return 1L;
    }
  }
}
