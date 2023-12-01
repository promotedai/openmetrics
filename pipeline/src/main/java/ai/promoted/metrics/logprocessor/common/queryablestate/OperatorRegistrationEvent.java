package ai.promoted.metrics.logprocessor.common.queryablestate;

import ai.promoted.proto.flinkqueryablestate.TaskEndpoints;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

public class OperatorRegistrationEvent implements OperatorEvent {

  private final TaskEndpoints.Endpoint endpoint;

  private final int coordinatorPort;

  public OperatorRegistrationEvent(TaskEndpoints.Endpoint endpoint, int coordinatorPort) {
    this.endpoint = endpoint;
    this.coordinatorPort = coordinatorPort;
  }

  public TaskEndpoints.Endpoint getEndpoint() {
    return endpoint;
  }

  public int getCoordinatorPort() {
    return coordinatorPort;
  }
}
