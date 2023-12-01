package ai.promoted.metrics.logprocessor.common.functions;

import static ai.promoted.metrics.logprocessor.common.util.PagingIdUtil.isPagingIdEmpty;

import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.event.CombinedDeliveryLog;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.TextFormat;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Preconditions;

/**
 * Fills in {@code Response.paging_info.paging_id} if it's not filled in. It's usually not populated
 * in SDK DeliveryLogs.
 *
 * <p>This is done after CombineDeliveryLog to make it easier for SDK fallback to have the same
 * paging_id.
 *
 * <p>The exact hash function does not need to match Delivery C++. Ideally we factor in the same
 * fields.
 */
public class PopulatePagingId implements MapFunction<CombinedDeliveryLog, CombinedDeliveryLog> {
  private static String getPagingId(DeliveryLog deliveryLog) {
    return deliveryLog.getResponse().getPagingInfo().getPagingId();
  }

  @Override
  public CombinedDeliveryLog map(CombinedDeliveryLog combinedDeliveryLog) throws Exception {
    Preconditions.checkArgument(
        combinedDeliveryLog.hasApi() || combinedDeliveryLog.hasSdk(),
        "Encountered a CombinedDeliveryLog that does not have either an API or SDK DeliveryLog");

    boolean updateApiPagingId = false;
    String apiPagingId = getPagingId(combinedDeliveryLog.getApi());
    boolean updateSdkPagingId = false;
    String sdkPagingId = getPagingId(combinedDeliveryLog.getSdk());
    if (combinedDeliveryLog.hasApi() && isPagingIdEmpty(apiPagingId)) {
      if (!isPagingIdEmpty(sdkPagingId)) {
        apiPagingId = sdkPagingId;
      } else {
        apiPagingId = generatePagingId(combinedDeliveryLog.getApi().getRequest());
      }
      updateApiPagingId = true;
    }
    if (combinedDeliveryLog.hasSdk() && isPagingIdEmpty(sdkPagingId)) {
      if (!isPagingIdEmpty(apiPagingId)) {
        sdkPagingId = apiPagingId;
      } else {
        sdkPagingId = generatePagingId(combinedDeliveryLog.getSdk().getRequest());
      }
      updateSdkPagingId = true;
    }

    if (updateApiPagingId || updateSdkPagingId) {
      CombinedDeliveryLog.Builder combinedDeliveryLogBuilder = combinedDeliveryLog.toBuilder();
      if (updateApiPagingId) {
        combinedDeliveryLogBuilder
            .getApiBuilder()
            .getResponseBuilder()
            .getPagingInfoBuilder()
            .setPagingId(apiPagingId);
      }
      if (updateSdkPagingId) {
        combinedDeliveryLogBuilder
            .getSdkBuilder()
            .getResponseBuilder()
            .getPagingInfoBuilder()
            .setPagingId(sdkPagingId);
      }
      combinedDeliveryLog = combinedDeliveryLogBuilder.build();
    }
    return combinedDeliveryLog;
  }

  /**
   * This does not need to 100% match the output from Delivery. It should ideally factor in the same
   * states.
   */
  @VisibleForTesting
  static String generatePagingId(Request request) throws NoSuchAlgorithmException {
    // It's built into Java and is reasonably fast.
    MessageDigest md = MessageDigest.getInstance("MD5");

    // TODO - add a similar comment to delivery-cpp
    // If you update this function, make sure to update delivery-cpp.
    md.update(toBytes(request.getPlatformId()));
    md.update(request.getUserInfo().getLogUserId().getBytes(StandardCharsets.UTF_8));
    md.update(toBytes(request.getClientInfo().getClientTypeValue()));
    md.update(toBytes(request.getClientInfo().getTrafficTypeValue()));
    md.update(toBytes(request.getUseCaseValue()));
    md.update(request.getSearchQuery().getBytes(StandardCharsets.UTF_8));

    // TODO - add blender config.

    // TODO - support nonKeyProperties.
    // Simplify the code by using TextFormat to generate a hash of Request.properties.
    // TextFormat sorts the keys for each map.
    md.update(
        TextFormat.printer()
            .printToString(request.getProperties())
            .getBytes(StandardCharsets.UTF_8));
    return Base64.getEncoder().encodeToString(md.digest());
  }

  private static byte[] toBytes(long value) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(value);
    return buffer.array();
  }

  private static byte[] toBytes(int value) {
    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
    buffer.putInt(value);
    return buffer.array();
  }
}
