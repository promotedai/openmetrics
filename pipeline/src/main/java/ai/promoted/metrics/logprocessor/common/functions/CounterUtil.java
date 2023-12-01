package ai.promoted.metrics.logprocessor.common.functions;

import ai.promoted.proto.event.AttributedAction;

public class CounterUtil {

  /**
   * Returns the count (metric) to aggregate in the Counter Job. Has some special logic for shopping
   * cart support.
   */
  public static long getCount(AttributedAction event) {
    if (event.getAction().hasSingleCartContent()) {
      return event.getAction().getSingleCartContent().getQuantity();
    } else {
      return 1L;
    }
  }
}
