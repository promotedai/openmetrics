package ai.promoted.metrics.logprocessor.common.queryablestate.metrics;

import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;

/**
 * A {@link ForwardingServerCallListener} which updates Prometheus metrics for a single rpc based on
 * updates received from grpc.
 */
public class MonitoringServerCallListener<R> extends ForwardingServerCallListener<R> {
  private final ServerCall.Listener<R> delegate;
  private final GrpcMethod grpcMethod;
  private final ServerMetricsReporter serverMetricsReporter;
  private final Metadata requestMetadata;

  MonitoringServerCallListener(
      ServerCall.Listener<R> delegate,
      ServerMetricsReporter serverMetricsReporter,
      GrpcMethod grpcMethod,
      Metadata requestMetadata) {
    this.delegate = delegate;
    this.serverMetricsReporter = serverMetricsReporter;
    this.grpcMethod = grpcMethod;
    this.requestMetadata = requestMetadata;
  }

  @Override
  protected ServerCall.Listener<R> delegate() {
    return delegate;
  }

  @Override
  public void onMessage(R request) {
    serverMetricsReporter.recordStreamMessageReceived(requestMetadata);
    super.onMessage(request);
  }
}
