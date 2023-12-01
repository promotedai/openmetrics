package ai.promoted.metrics.logprocessor.common.queryablestate;

import static ai.promoted.metrics.logprocessor.common.queryablestate.ClientUtils.doWithRetry;
import static org.apache.flink.util.Preconditions.checkArgument;

import ai.promoted.proto.flinkqueryablestate.BatchResult;
import ai.promoted.proto.flinkqueryablestate.BatchStateRequest;
import ai.promoted.proto.flinkqueryablestate.CoordinatorGrpc;
import ai.promoted.proto.flinkqueryablestate.TaskEndpoints;
import ai.promoted.proto.flinkqueryablestate.TaskGrpc;
import ai.promoted.proto.flinkqueryablestate.Uid;
import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class OperatorStateQueryClient {
  private static final Logger LOG = LogManager.getLogger(OperatorStateQueryClient.class);

  private final String uid;

  private final AddressRetriever coordinatorAddressRetriever;

  private final List<Channel> taskChannels = new ArrayList<>();

  private final List<TaskGrpc.TaskBlockingStub> taskStubs = new ArrayList<>();

  private int parallelism;

  private int maxParallelism;

  private boolean portforwardToLocalhost;

  public OperatorStateQueryClient(String uid, AddressRetriever coordinatorAddressRetriever) {
    this(uid, coordinatorAddressRetriever, false);
  }

  public OperatorStateQueryClient(
      String uid, AddressRetriever coordinatorAddressRetriever, boolean portforwardToLocalhost) {
    this.uid = uid;
    this.coordinatorAddressRetriever = coordinatorAddressRetriever;
    this.portforwardToLocalhost = portforwardToLocalhost;
  }

  public void connect() {
    doWithRetry(
        () -> {
          internalConnect(portforwardToLocalhost);
          return null;
        },
        1000,
        10,
        () -> {},
        () -> {
          throw new RuntimeException(
              "Could not connect to " + coordinatorAddressRetriever.getAddress());
        });
  }

  private void internalConnect(boolean portforwardToLocalhost) {
    // let's clear the existing connections
    Channel coordinatorChannel;
    CoordinatorGrpc.CoordinatorBlockingStub coordinatorStub;
    taskChannels.clear();
    taskStubs.clear();

    String target = coordinatorAddressRetriever.getAddress();
    LOG.info("Trying to start connecting {}", target);

    coordinatorChannel =
        Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();
    coordinatorStub = CoordinatorGrpc.newBlockingStub(coordinatorChannel);

    TaskEndpoints taskEndpoints =
        coordinatorStub.listTaskEndpoints(Uid.newBuilder().setUid(uid).build());

    List<TaskEndpoints.Endpoint> endpoints = taskEndpoints.getEndpointsList();
    if (portforwardToLocalhost) {
      endpoints =
          endpoints.stream()
              .map(endpoint -> endpoint.toBuilder().setHost("localhost").build())
              .collect(Collectors.toList());
    }
    parallelism = taskEndpoints.getParallelism();
    maxParallelism = taskEndpoints.getMaxParallelism();
    if (endpoints.isEmpty()) {
      throw new RuntimeException("Endpoints not ready");
    }

    // let's now connect to all the tasks
    for (TaskEndpoints.Endpoint endpoint : endpoints) {
      String taskTarget = endpoint.getHost() + ":" + endpoint.getPort();
      Channel taskChannel =
          Grpc.newChannelBuilder(taskTarget, InsecureChannelCredentials.create()).build();
      TaskGrpc.TaskBlockingStub taskStub = TaskGrpc.newBlockingStub(taskChannel);
      taskChannels.add(taskChannel);
      taskStubs.add(taskStub);
    }
  }

  public List<byte[]> query(
      List<byte[]> keyBytesList, List<Integer> keyHashesList, String stateName) {
    return doWithRetry(
        () -> internalQuery(keyBytesList, keyHashesList, stateName),
        1000,
        10,
        () -> internalConnect(portforwardToLocalhost),
        () -> {
          throw new RuntimeException("Failed to query");
        });
  }

  private List<byte[]> internalQuery(
      List<byte[]> keyBytesList, List<Integer> keyHashesList, String stateName) {
    checkArgument(keyBytesList.size() == keyHashesList.size());
    List<Integer> targetSubtasks = new ArrayList<>();
    for (int i = 0; i < keyHashesList.size(); i++) {
      targetSubtasks.add(
          KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
              maxParallelism,
              parallelism,
              KeyGroupRangeAssignment.computeKeyGroupForKeyHash(
                  keyHashesList.get(i), maxParallelism)));
    }
    List<ByteString> keyByteStreamList =
        keyBytesList.stream().map(ByteString::copyFrom).collect(Collectors.toList());

    int requestSize = keyBytesList.size();
    Map<Integer, List<Tuple3<Integer, Integer, ByteString>>> groupedRequests =
        IntStream.range(0, requestSize)
            // (endpoint index, request index, key)
            .mapToObj(i -> new Tuple3<>(targetSubtasks.get(i), i, keyByteStreamList.get(i)))
            .collect(Collectors.groupingBy(integerIntegerTuple3 -> integerIntegerTuple3.f0));

    byte[][] result = new byte[keyBytesList.size()][];
    groupedRequests.entrySet().parallelStream()
        .filter(entry -> entry.getKey() >= 0) // index < 0 means failed to get the endpoint index.
        .forEach(
            entry -> {
              BatchStateRequest.Builder requestBuilder =
                  BatchStateRequest.newBuilder().setStateName(stateName);
              requestBuilder.addAllKeys(
                  entry.getValue().stream().map(tuple -> (tuple.f2)).collect(Collectors.toList()));
              BatchResult queryResult =
                  taskStubs.get(entry.getKey()).queryState(requestBuilder.build());
              IntStream.range(0, entry.getValue().size())
                  .forEach(
                      i ->
                          result[entry.getValue().get(i).f1] =
                              queryResult.getResults(i).toByteArray());
            });
    for (int i = 0; i < result.length; ++i) {
      if (null == result[i]) {
        result[i] = new byte[0];
      }
    }
    return Arrays.asList(result);
  }
}
