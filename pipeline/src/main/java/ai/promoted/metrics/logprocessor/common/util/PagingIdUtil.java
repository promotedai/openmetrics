package ai.promoted.metrics.logprocessor.common.util;

import ai.promoted.proto.delivery.PagingInfo;
import ai.promoted.proto.delivery.Response;

/** Utility for Paging IDs. */
public class PagingIdUtil {

  // Early Delivery-cpp logged "_" in this field.  Treat this like an empty paging ID.
  private static final String EARLY_DELIVERY_HIDDEN_PAGING_ID = "_";

  public static boolean isPagingIdEmpty(Response response) {
    return isPagingIdEmpty(response.getPagingInfo());
  }

  public static boolean isPagingIdEmpty(PagingInfo pagingInfo) {
    return isPagingIdEmpty(pagingInfo.getPagingId());
  }

  // TODO - simplify this after enough time has passed.
  /**
   * Returns whether the paging ID is empty. Handles a case where DeliveryLog would output "_" for
   * an empty paging ID.
   */
  public static boolean isPagingIdEmpty(String pagingId) {
    return pagingId.isEmpty() || EARLY_DELIVERY_HIDDEN_PAGING_ID.equals(pagingId);
  }
}
