package ai.promoted.metrics.logprocessor.common.queryablestate;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This operator provide the ability to query states of the specified operator.
 *
 * <p>It works as follows:
 *
 * <ul>
 *   <li>The queryable service starts in the OperatorCoordinator. Different Queryable API is
 *       supported, like long-connection TCP or HTTP.
 *   <li>On startup, all the tasks will reported the assigned states range to the
 *       OperatorCoordinator.
 *   <li>The client will get the list of the TM's via coordinator. It will then connection to all of
 *       them for querying.
 * </ul>
 */
public class QueryServiceOperator extends AbstractStreamOperator<Void>
    implements OneInputStreamOperator<Void, Void>, OperatorEventHandler {
  private static final Logger LOGGER = LogManager.getLogger(QueryServiceOperator.class);

  private final OperatorID targetOperatorId;

  private final OperatorEventGateway operatorEventGateway;

  private final OperatorInfo targetOperatorInfo;

  private transient ThreadLocal<RocksdbStateBackendQuerier> stateBackendQuerier;

  private transient TaskServer taskServer;

  public QueryServiceOperator(
      OperatorID targetOperatorId,
      OperatorEventGateway operatorEventGateway,
      OperatorInfo targetOperatorInfo) {
    this.targetOperatorId = targetOperatorId;
    this.operatorEventGateway = operatorEventGateway;
    this.targetOperatorInfo = targetOperatorInfo;
  }

  @Override
  public void open() throws Exception {
    super.open();

    StreamOperator<?> targetOperator =
        OperatorRegistry.get()
            .queryBlocking(targetOperatorId, getRuntimeContext().getIndexOfThisSubtask());
    LOGGER.info("Acquired {}-{}", targetOperatorId, getRuntimeContext().getIndexOfThisSubtask());

    // Now let's get the rocksdb instance
    stateBackendQuerier =
        ThreadLocal.withInitial(() -> new RocksdbStateBackendQuerier(targetOperator));
    taskServer =
        new TaskServer(
            targetOperatorInfo,
            (stateName, stateKey) -> stateBackendQuerier.get().query(stateName, stateKey),
            getRuntimeContext().getMetricGroup());
    taskServer.start();

    Configuration envConfig = getRuntimeContext().getTaskManagerRuntimeInfo().getConfiguration();

    // get the ip of this tm
    LOGGER.info(
        "----"
            + getContainingTask()
                .getEnvironment()
                .getTaskManagerInfo()
                .getTaskManagerExternalAddress());

    // let's now acquire the endpoint
    operatorEventGateway.sendEventToCoordinator(
        new OperatorRegistrationEvent(
            taskServer.getAddress(), envConfig.get(StreamGraphRewriterOptions.STATE_QUERY_PORT)));
  }

  @Override
  public void processElement(StreamRecord<Void> streamRecord) throws Exception {}

  @Override
  public void handleOperatorEvent(OperatorEvent evt) {}

  @Override
  public void close() throws Exception {
    super.close();
    taskServer.stop();
  }
}
