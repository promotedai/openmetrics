package ai.promoted.metrics.logprocessor.common.queryablestate;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

public class QueryServiceOperatorFactory extends AbstractStreamOperator<Void>
    implements OneInputStreamOperatorFactory<Void, Void>, CoordinatedOperatorFactory<Void> {

  private final String targetUid;

  private final OperatorInfo targetOperatorInfo;

  private OperatorID targetOperatorId;

  public QueryServiceOperatorFactory(String targetUid, OperatorInfo targetOperatorInfo) {
    this.targetUid = targetUid;
    this.targetOperatorInfo = targetOperatorInfo;
  }

  public void setTargetOperatorId(OperatorID targetOperatorId) {
    this.targetOperatorId = targetOperatorId;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends StreamOperator<Void>> T createStreamOperator(
      StreamOperatorParameters<Void> streamOperatorParameters) {
    QueryServiceOperator queryServiceOperator =
        new QueryServiceOperator(
            targetOperatorId,
            createOperatorEventGateway(streamOperatorParameters),
            targetOperatorInfo);
    queryServiceOperator.setup(
        streamOperatorParameters.getContainingTask(),
        streamOperatorParameters.getStreamConfig(),
        streamOperatorParameters.getOutput());
    streamOperatorParameters
        .getOperatorEventDispatcher()
        .registerEventHandler(
            streamOperatorParameters.getStreamConfig().getOperatorID(), queryServiceOperator);
    return (T) queryServiceOperator;
  }

  private OperatorEventGateway createOperatorEventGateway(
      StreamOperatorParameters<Void> streamOperatorParameters) {
    return streamOperatorParameters
        .getOperatorEventDispatcher()
        .getOperatorEventGateway(streamOperatorParameters.getStreamConfig().getOperatorID());
  }

  @Override
  public Class<? extends StreamOperator<Void>> getStreamOperatorClass(ClassLoader classLoader) {
    return QueryServiceOperator.class;
  }

  @Override
  public OperatorCoordinator.Provider getCoordinatorProvider(String s, OperatorID operatorID) {
    return new QueryServiceOperatorCoordinator.QueryServiceOperatorCoordinatorProvider(
        operatorID,
        targetUid,
        targetOperatorInfo.getParallelism(),
        targetOperatorInfo.getMaxParallelism());
  }
}
