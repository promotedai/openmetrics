package ai.promoted.metrics.logprocessor.common.queryablestate.metrics;

import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.Status;
import java.time.Clock;
import java.time.Instant;

/**
 * A {@link ForwardingServerCall} which updates Prometheus metrics based on the server-side actions
 * taken for a single rpc, e.g., messages sent, latency, etc.
 */
class MonitoringServerCall<R, S> extends ForwardingServerCall.SimpleForwardingServerCall<R, S> {
  private final Clock clock;
  private final GrpcMethod grpcMethod;
  private final ServerMetricsReporter serverMetricsReporter;
  private final Instant startInstant;
  private final Metadata requestMetadata;

  MonitoringServerCall(
      ServerCall<R, S> delegate,
      Clock clock,
      GrpcMethod grpcMethod,
      ServerMetricsReporter serverMetricsReporter,
      Metadata requestMetadata) {
    super(delegate);
    this.clock = clock;
    this.grpcMethod = grpcMethod;
    this.serverMetricsReporter = serverMetricsReporter;
    this.startInstant = clock.instant();
    this.requestMetadata = requestMetadata;

    // TODO(dino): Consider doing this in the onReady() method of the listener instead.
    reportStartMetrics();
  }

  @Override
  public void close(Status status, Metadata responseHeaders) {
    reportEndMetrics(status);
    super.close(status, responseHeaders);
  }

  @Override
  public void sendMessage(S message) {
    serverMetricsReporter.recordStreamMessageSent(requestMetadata);
    super.sendMessage(message);
  }

  private void reportStartMetrics() {
    serverMetricsReporter.recordCallStarted(requestMetadata);
  }

  private void reportEndMetrics(Status status) {
    Status.Code code = status.getCode();
    serverMetricsReporter.recordServerHandled(code, requestMetadata);
    long latencySec = (clock.millis() - startInstant.toEpochMilli());
    serverMetricsReporter.recordLatency(latencySec, requestMetadata, code);
  }
}
