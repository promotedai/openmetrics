package ai.promoted.metrics.logprocessor.common.queryablestate;

import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask;

public class SelfRegisterTwoInputStreamTask<IN1, IN2, OUT>
    extends TwoInputStreamTask<IN1, IN2, OUT> {

  public SelfRegisterTwoInputStreamTask(Environment env) throws Exception {
    super(env);
  }

  @Override
  public void init() throws Exception {
    super.init();
    StreamTaskUtil.waitToRegister(
        operatorChain.getAllOperators(), getIndexInSubtaskGroup(), this::isRunning);
  }
}
