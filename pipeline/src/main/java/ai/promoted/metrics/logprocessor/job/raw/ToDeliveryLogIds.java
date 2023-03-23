package ai.promoted.metrics.logprocessor.job.raw;

import ai.promoted.metrics.common.DeliveryLogIds;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.ExecutionServer;
import ai.promoted.proto.delivery.Request;
import com.google.common.annotations.VisibleForTesting;
import java.util.EnumMap;
import org.apache.flink.api.common.functions.MapFunction;

/** Converts DeliveryLog to join smaller DeliveryLogIds records. */
class ToDeliveryLogIds implements MapFunction<DeliveryLog, DeliveryLogIds> {

  @VisibleForTesting
  static EnumMap<ExecutionServer, ai.promoted.metrics.common.ExecutionServer> protoToAvroServer =
      createAvroExecutionServer();

  @Override
  public DeliveryLogIds map(DeliveryLog deliveryLog) throws Exception {
    Request request = deliveryLog.getRequest();
    return DeliveryLogIds.newBuilder()
        .setPlatformId(deliveryLog.getPlatformId())
        .setEventApiTimestamp(request.getTiming().getEventApiTimestamp())
        .setLogUserId(request.getUserInfo().getLogUserId())
        .setViewId(request.getViewId())
        .setRequestId(request.getRequestId())
        .setClientRequestId(request.getClientRequestId())
        .setExecutionServer(toAvroExecutionServer(deliveryLog.getExecution().getExecutionServer()))
        .setSearchQuery(request.getSearchQuery())
        .setUserAgent(request.getDevice().getBrowser().getUserAgent())
        .build();
  }

  private static EnumMap<ExecutionServer, ai.promoted.metrics.common.ExecutionServer>
      createAvroExecutionServer() {
    EnumMap<ExecutionServer, ai.promoted.metrics.common.ExecutionServer> map =
        new EnumMap(ExecutionServer.class);
    for (ExecutionServer server : ExecutionServer.values()) {
      map.put(server, ai.promoted.metrics.common.ExecutionServer.valueOf(server.name()));
    }
    return map;
  }

  private ai.promoted.metrics.common.ExecutionServer toAvroExecutionServer(ExecutionServer server) {
    ai.promoted.metrics.common.ExecutionServer avroValue = protoToAvroServer.get(server);
    if (avroValue != null) {
      return avroValue;
    } else {
      return ai.promoted.metrics.common.ExecutionServer.UNKNOWN_EXECUTION_SERVER;
    }
  }
}
