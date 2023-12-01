package ai.promoted.metrics.logprocessor.common.queryablestate;

import ai.promoted.metrics.logprocessor.common.queryablestate.metrics.MonitoringServerInterceptor;
import ai.promoted.proto.flinkqueryablestate.BatchEndPointRequest;
import ai.promoted.proto.flinkqueryablestate.BatchResult;
import ai.promoted.proto.flinkqueryablestate.BatchStateRequest;
import ai.promoted.proto.flinkqueryablestate.BatchTaskIndex;
import ai.promoted.proto.flinkqueryablestate.TaskEndpoints;
import ai.promoted.proto.flinkqueryablestate.TaskGrpc;
import com.google.protobuf.ByteString;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.util.function.BiFunctionWithException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TaskServer {
  private static final Logger LOG = LogManager.getLogger(TaskServer.class);
  private final OperatorInfo targetOperatorInfo;
  private final BiFunctionWithException<String, byte[], byte[], Throwable> querier;
  private final OperatorMetricGroup operatorMetricGroup;
  private final Server server;
  private transient Counter successCount;
  private transient Counter failCount;
  private transient Histogram queryLatencyHist;
  private transient Histogram getEndpointLatencyHist;
  private final ExecutorService threadPool;

  public TaskServer(
      OperatorInfo targetOperatorInfo,
      BiFunctionWithException<String, byte[], byte[], Throwable> querier,
      @Nullable OperatorMetricGroup operatorMetricGroup) {
    this.targetOperatorInfo = targetOperatorInfo;
    this.querier = querier;
    this.operatorMetricGroup = operatorMetricGroup;
    this.threadPool = Executors.newCachedThreadPool();
    if (null != operatorMetricGroup) {
      LOG.info("Register gRPC metrics to Prometheus");
      MonitoringServerInterceptor monitoringInterceptor =
          MonitoringServerInterceptor.create(operatorMetricGroup);
      this.server =
          Grpc.newServerBuilderForPort(0, InsecureServerCredentials.create())
              .addService(ServerInterceptors.intercept(new TaskService(), monitoringInterceptor))
              .build();
    } else {
      this.server =
          Grpc.newServerBuilderForPort(0, InsecureServerCredentials.create())
              .addService(new TaskService())
              .build();
    }
  }

  public void start() throws IOException {
    server.start();
    if (null != operatorMetricGroup) {
      successCount = operatorMetricGroup.counter("SuccessCount");
      failCount = operatorMetricGroup.counter("FailCount");
      queryLatencyHist =
          operatorMetricGroup.histogram(
              "QueryLatencyHist", new DescriptiveStatisticsHistogram(10000));
      getEndpointLatencyHist =
          operatorMetricGroup.histogram(
              "GetEndpointLatencyHist", new DescriptiveStatisticsHistogram(10000));
    }
    LOG.info("The task server has started at {}", server.getListenSockets());
  }

  public void stop() throws InterruptedException {
    if (server != null) {
      server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    }
    threadPool.shutdown();
  }

  public TaskEndpoints.Endpoint getAddress() {
    SocketAddress address = server.getListenSockets().get(0);
    String host = HostUtils.getFirstNonLocalIp();

    return TaskEndpoints.Endpoint.newBuilder()
        .setHost(host)
        .setPort(((InetSocketAddress) address).getPort())
        .build();
  }

  private class TaskService extends TaskGrpc.TaskImplBase {
    /** TODO Remove this after the client-side hashing works well */
    @Override
    @Deprecated
    public void getEndpoint(
        BatchEndPointRequest request, StreamObserver<BatchTaskIndex> responseObserver) {
      long getEndpointBeginTime = System.currentTimeMillis();
      // Computes the subtask index for this key with the consistent hashing method by Flink
      BatchTaskIndex.Builder resultBuilder = BatchTaskIndex.newBuilder();
      try {
        List<Integer> indexList =
            threadPool
                .submit(
                    () ->
                        request.getKeysList().stream()
                            .map(
                                k -> {
                                  if (k.isEmpty()) {
                                    return -1; // Failed to get the endpoint index.
                                  }
                                  int endpointIndex = -1;
                                  DataInputDeserializer deserializer =
                                      new DataInputDeserializer(k.toByteArray());
                                  try {
                                    Object key =
                                        targetOperatorInfo
                                            .getKeySerializer()
                                            .deserialize(deserializer);
                                    int keyGroup =
                                        KeyGroupRangeAssignment.assignToKeyGroup(
                                            key, targetOperatorInfo.getMaxParallelism());
                                    endpointIndex =
                                        KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
                                            targetOperatorInfo.getMaxParallelism(),
                                            targetOperatorInfo.getParallelism(),
                                            keyGroup);
                                  } catch (Exception e) {
                                    LOG.error("Failed to get endpoint for " + k, e);
                                  }
                                  return endpointIndex;
                                })
                            .collect(Collectors.toList()))
                .get();
        resultBuilder.addAllIndex(indexList);
        responseObserver.onNext(resultBuilder.build());
        responseObserver.onCompleted();
        long getEndpointTime = System.currentTimeMillis() - getEndpointBeginTime;
        updateHistogramWhenNotNull(getEndpointLatencyHist, getEndpointTime);
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Error while getting endpoints", e);
        responseObserver.onError(e);
      }
    }

    @Override
    public void queryState(
        BatchStateRequest batchRequest, StreamObserver<BatchResult> responseObserver) {
      String stateName = batchRequest.getStateName();
      BatchResult.Builder resultBuilder = BatchResult.newBuilder();
      try {
        List<ByteString> queryResults =
            threadPool
                .submit(
                    () ->
                        batchRequest.getKeysList().stream()
                            .map(
                                queryKey -> {
                                  try {
                                    long queryBeginTime = System.currentTimeMillis();
                                    byte[] value = querier.apply(stateName, queryKey.toByteArray());
                                    ByteString result;
                                    if (value == null) { // state not found for the given key
                                      increaseCounterWhenNotNull(failCount, 1);
                                      LOG.debug(
                                          "Value not found for key {}",
                                          () -> Arrays.toString(queryKey.toByteArray()));
                                      result = ByteString.EMPTY;
                                    } else {
                                      increaseCounterWhenNotNull(successCount, 1);
                                      result = ByteString.copyFrom(value);
                                    }
                                    long queryTime = System.currentTimeMillis() - queryBeginTime;
                                    updateHistogramWhenNotNull(queryLatencyHist, queryTime);
                                    return result;
                                  } catch (Throwable t) {
                                    LOG.error("Query failed for request " + queryKey, t);
                                  }
                                  return ByteString.EMPTY;
                                })
                            .collect(Collectors.toList()))
                .get();
        resultBuilder.addAllResults(queryResults);
        responseObserver.onNext(resultBuilder.build());
        responseObserver.onCompleted();
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Error while querying state", e);
        responseObserver.onError(e);
      }
    }
  }

  private void updateHistogramWhenNotNull(Histogram histogram, long value) {
    if (null != histogram) {
      histogram.update(value);
    }
  }

  private void increaseCounterWhenNotNull(Counter counter, long value) {
    if (null != counter) {
      counter.inc(value);
    }
  }
}
