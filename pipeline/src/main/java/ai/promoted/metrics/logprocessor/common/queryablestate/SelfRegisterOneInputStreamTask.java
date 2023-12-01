package ai.promoted.metrics.logprocessor.common.queryablestate;

import javax.annotation.Nullable;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.TimerService;

public class SelfRegisterOneInputStreamTask<IN, OUT> extends OneInputStreamTask<IN, OUT> {

  public SelfRegisterOneInputStreamTask(Environment env) throws Exception {
    super(env);
  }

  public SelfRegisterOneInputStreamTask(Environment env, @Nullable TimerService timeProvider)
      throws Exception {
    super(env, timeProvider);
  }

  @Override
  public void init() throws Exception {
    super.init();
    StreamTaskUtil.waitToRegister(
        operatorChain.getAllOperators(), getIndexInSubtaskGroup(), this::isRunning);
  }
}
