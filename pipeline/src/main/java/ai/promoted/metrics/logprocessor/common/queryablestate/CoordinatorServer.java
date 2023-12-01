package ai.promoted.metrics.logprocessor.common.queryablestate;

import static org.apache.flink.util.Preconditions.checkState;

import ai.promoted.proto.flinkqueryablestate.CoordinatorGrpc;
import ai.promoted.proto.flinkqueryablestate.TaskEndpoints;
import ai.promoted.proto.flinkqueryablestate.Uid;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Provides the list of task endpoints. */
public class CoordinatorServer {

  private static CoordinatorServer coordinatorServer;

  private static int count;

  public static CoordinatorServer getCoordinatorServer(int port) throws IOException {
    synchronized (CoordinatorServer.class) {
      if (coordinatorServer == null) {
        coordinatorServer = new CoordinatorServer(port);
        coordinatorServer.start();
      }
      count += 1;
    }

    return coordinatorServer;
  }

  public static void release() throws Exception {
    synchronized (CoordinatorServer.class) {
      count -= 1;

      if (count == 0) {
        coordinatorServer.stop();
      }
    }
  }

  private static final Logger LOG = LogManager.getLogger(CoordinatorServer.class);

  private final Map<String, Tuple3<List<TaskEndpoints.Endpoint>, Integer, Integer>> endpoints =
      new HashMap<>();

  private final Server server;

  private CoordinatorServer(int port) {
    this.server =
        Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
            .addService(new CoordinatorService())
            .build();
  }

  public void start() throws IOException {
    server.start();
    LOG.info("The coordinator server has started {}", server.getListenSockets());
  }

  public void stop() throws InterruptedException {
    if (server != null) {
      server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  public void update(
      String uid, List<TaskEndpoints.Endpoint> uidEndpoints, int parallelism, int maxParallelism) {
    LOG.info("Update endpoints {} for {}.", uidEndpoints, uid);
    synchronized (endpoints) {
      endpoints.put(uid, new Tuple3<>(uidEndpoints, parallelism, maxParallelism));
    }
  }

  /**
   * Update the endpoints if some tasks restarted.
   *
   * <p>Use synchronization since it is not expected to be in the critical path.
   */
  public void update(String uid, int index, TaskEndpoints.Endpoint endpoint) {
    LOG.info("{} Update endpoints-{} to {}", uid, index, endpoint);
    synchronized (endpoints) {
      Tuple3<List<TaskEndpoints.Endpoint>, Integer, Integer> uidEndpointInfo = endpoints.get(uid);
      checkState(uidEndpointInfo != null, "Please add the list before fine-grained update");

      uidEndpointInfo.f0.set(index, endpoint);
    }
  }

  private class CoordinatorService extends CoordinatorGrpc.CoordinatorImplBase {

    @Override
    public void listTaskEndpoints(Uid request, StreamObserver<TaskEndpoints> responseObserver) {
      List<TaskEndpoints.Endpoint> copyOfUidEndpoints = null;
      int parallelism = 0;
      int maxParallelism = 0;
      synchronized (endpoints) {
        Tuple3<List<TaskEndpoints.Endpoint>, Integer, Integer> uidEndpointInfo =
            endpoints.get(request.getUid());
        if (uidEndpointInfo != null) {
          copyOfUidEndpoints = new ArrayList<>(uidEndpointInfo.f0);
          parallelism = uidEndpointInfo.f1;
          maxParallelism = uidEndpointInfo.f2;
        }
      }

      if (copyOfUidEndpoints == null) {
        responseObserver.onNext(TaskEndpoints.newBuilder().build());
        responseObserver.onCompleted();
        return;
      }

      responseObserver.onNext(
          TaskEndpoints.newBuilder()
              .addAllEndpoints(copyOfUidEndpoints)
              .setParallelism(parallelism)
              .setMaxParallelism(maxParallelism)
              .build());
      responseObserver.onCompleted();
    }
  }
}
