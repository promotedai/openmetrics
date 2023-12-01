package ai.promoted.metrics.logprocessor.common.queryablestate.metrics;

import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import java.time.Clock;
import org.apache.flink.metrics.MetricGroup;

/** A {@link ServerInterceptor} which sends stats about incoming grpc calls to metrics reporter. */
public class MonitoringServerInterceptor implements ServerInterceptor {
  private final Clock clock;
  private final ServerMetricsReporter.Factory serverMetricsFactory;

  public static MonitoringServerInterceptor create(MetricGroup baseMetricGroup) {
    return new MonitoringServerInterceptor(
        Clock.systemDefaultZone(), new ServerMetricsReporter.Factory(baseMetricGroup));
  }

  private MonitoringServerInterceptor(
      Clock clock, ServerMetricsReporter.Factory serverMetricsFactory) {
    this.clock = clock;
    this.serverMetricsFactory = serverMetricsFactory;
  }

  @Override
  public <R, S> ServerCall.Listener<R> interceptCall(
      ServerCall<R, S> call, Metadata requestMetadata, ServerCallHandler<R, S> next) {
    MethodDescriptor<R, S> methodDescriptor = call.getMethodDescriptor();
    GrpcMethod grpcMethod = GrpcMethod.of(methodDescriptor);
    ServerMetricsReporter metricsReporter = serverMetricsFactory.getMetricsForMethod(grpcMethod);
    ServerCall<R, S> monitoringCall =
        new MonitoringServerCall<>(call, clock, grpcMethod, metricsReporter, requestMetadata);
    return new MonitoringServerCallListener<>(
        next.startCall(monitoringCall, requestMetadata),
        metricsReporter,
        grpcMethod,
        requestMetadata);
  }
}
