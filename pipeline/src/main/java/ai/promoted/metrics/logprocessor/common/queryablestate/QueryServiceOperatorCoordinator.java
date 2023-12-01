package ai.promoted.metrics.logprocessor.common.queryablestate;

import ai.promoted.proto.flinkqueryablestate.TaskEndpoints;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

public class QueryServiceOperatorCoordinator implements OperatorCoordinator {

  private final String uid;

  private final int parallelism;

  private final int maxParallelism;

  private final TreeMap<Integer, TaskEndpoints.Endpoint> endpoints = new TreeMap<>();

  private CoordinatorServer coordinatorServer;

  public QueryServiceOperatorCoordinator(String uid, int parallelism, int maxParallelism) {
    this.uid = uid;
    this.parallelism = parallelism;
    this.maxParallelism = maxParallelism;
  }

  @Override
  public void start() throws Exception {}

  @Override
  public void close() throws Exception {
    CoordinatorServer.release();
  }

  @Override
  public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData) {
    endpoints.clear();
  }

  @Override
  public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event)
      throws Exception {
    if (!(event instanceof OperatorRegistrationEvent)) {
      return;
    }

    OperatorRegistrationEvent registrationEvent = (OperatorRegistrationEvent) event;
    if (coordinatorServer == null) {
      coordinatorServer =
          CoordinatorServer.getCoordinatorServer(registrationEvent.getCoordinatorPort());
    }

    int oldCount = endpoints.size();
    if (oldCount >= parallelism) {
      endpoints.put(subtask, registrationEvent.getEndpoint());
      coordinatorServer.update(uid, subtask, registrationEvent.getEndpoint());
      return;
    }

    endpoints.put(subtask, ((OperatorRegistrationEvent) event).getEndpoint());
    if (endpoints.size() == parallelism) {
      coordinatorServer.update(
          uid, new ArrayList<>(endpoints.values()), parallelism, maxParallelism);
    }
  }

  @Override
  public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture)
      throws Exception {
    resultFuture.complete(new byte[0]);
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {}

  @Override
  public void subtaskReset(int subtask, long checkpointId) {}

  @Override
  public void executionAttemptFailed(int subtask, int attemptNumber, @Nullable Throwable reason) {}

  @Override
  public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {}

  public static class QueryServiceOperatorCoordinatorProvider implements Provider {

    private final OperatorID operatorId;

    private final String uid;

    private final int parallelism;

    private final int maxParallelism;

    public QueryServiceOperatorCoordinatorProvider(
        OperatorID operatorId, String uid, int parallelism, int maxParallelism) {
      this.operatorId = operatorId;
      this.uid = uid;
      this.parallelism = parallelism;
      this.maxParallelism = maxParallelism;
    }

    @Override
    public OperatorID getOperatorId() {
      return operatorId;
    }

    @Override
    public OperatorCoordinator create(Context context) throws Exception {
      return new QueryServiceOperatorCoordinator(uid, parallelism, maxParallelism);
    }
  }
}
