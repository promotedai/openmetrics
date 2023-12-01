package ai.promoted.metrics.logprocessor.common.queryablestate.metrics;

import io.grpc.Metadata;
import io.grpc.Status.Code;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;

/** The metrics reporter used for server-side monitoring of grpc services. */
class ServerMetricsReporter {
  public static final String GRPC_TYPE = "grpc_type";
  public static final String GRPC_SERVICE = "grpc_service";
  public static final String GRPC_METHOD = "grpc_method";
  public static final String GRPC_CODE = "grpc_code";

  /** Total number of RPCs started on the server. */
  private final Counter serverStarted;

  /** Total number of RPCs completed on the server, regardless of success or failure. */
  private final Map<String, Counter> serverHandled;

  /** Total number of stream messages received from the client. */
  private final Counter serverStreamMessagesReceived;

  /** Total number of stream messages sent by the server. */
  private final Counter serverStreamMessagesSent;

  /**
   * Histogram of response latency (ms) of gRPC that had been application-level handled by the
   * server
   */
  private final Map<String, Histogram> serverHandledLatencyHistogram;

  private final MetricGroup metricGroup;

  private ServerMetricsReporter(MetricGroup metricGroup) {
    this.metricGroup = metricGroup;
    serverStarted = metricGroup.counter("started");
    serverHandled = new HashMap<>();
    serverStreamMessagesReceived = metricGroup.counter("msg_received");
    serverStreamMessagesSent = metricGroup.counter("msg_sent");
    serverHandledLatencyHistogram = new HashMap<>();
  }

  public void recordCallStarted(Metadata metadata) {
    serverStarted.inc();
  }

  public void recordServerHandled(Code code, Metadata metadata) {
    serverHandled
        .computeIfAbsent(
            code.toString(), key -> metricGroup.addGroup(GRPC_CODE, key).counter("handled"))
        .inc();
  }

  public void recordStreamMessageSent(Metadata metadata) {
    serverStreamMessagesSent.inc();
  }

  public void recordStreamMessageReceived(Metadata metadata) {
    serverStreamMessagesReceived.inc();
  }

  public void recordLatency(long latency, Metadata metadata, Code code) {
    serverHandledLatencyHistogram
        .computeIfAbsent(
            code.toString(),
            key ->
                metricGroup
                    .addGroup(GRPC_CODE, key)
                    .histogram("handled_latency", new DescriptiveStatisticsHistogram(10000)))
        .update(latency);
  }

  /** Knows how to produce {@link ServerMetricsReporter} instances for individual methods. */
  static class Factory {
    private final MetricGroup baseMetricGroup;

    /** A map to cache reporters in the same operator task. */
    private final Map<GrpcMethod, ServerMetricsReporter> reporterMap = new ConcurrentHashMap<>();

    Factory(MetricGroup baseMetricGroup) {
      this.baseMetricGroup = baseMetricGroup;
    }

    /** Creates a {@link ServerMetricsReporter} for the supplied gRPC method. */
    ServerMetricsReporter getMetricsForMethod(GrpcMethod grpcMethod) {
      return reporterMap.computeIfAbsent(
          grpcMethod,
          key -> new ServerMetricsReporter(metricGroupForGrpcMethod(baseMetricGroup, key)));
    }

    private MetricGroup metricGroupForGrpcMethod(
        MetricGroup baseMetricGroup, GrpcMethod grpcMethod) {
      return baseMetricGroup
          .addGroup(GRPC_TYPE, grpcMethod.type())
          .addGroup(GRPC_SERVICE, grpcMethod.serviceName())
          .addGroup(GRPC_METHOD, grpcMethod.methodName());
    }
  }
}
