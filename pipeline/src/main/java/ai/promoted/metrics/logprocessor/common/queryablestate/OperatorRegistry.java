package ai.promoted.metrics.logprocessor.common.queryablestate;

import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.StreamOperator;

/** Exchanging the operators during runtime. */
public class OperatorRegistry {

  private static final long BACKOFF = 1000;

  private static final OperatorRegistry instance;

  private OperatorRegistry() {}

  static {
    instance = new OperatorRegistry();
  }

  public static OperatorRegistry get() {
    return instance;
  }

  private final ConcurrentHashMap<Tuple2<OperatorID, Integer>, StreamOperator<?>> operators =
      new ConcurrentHashMap<>();

  public void register(OperatorID operatorId, int subtaskIndex, StreamOperator<?> operator) {
    operators.put(new Tuple2<>(operatorId, subtaskIndex), operator);
  }

  /** TODO Restrict the blocking time by fetching a timeout value from Flink config. */
  public StreamOperator<?> queryBlocking(OperatorID operatorId, int subtaskIndex)
      throws InterruptedException {
    while (true) {
      StreamOperator<?> operator = operators.get(new Tuple2<>(operatorId, subtaskIndex));
      if (operator != null) {
        return operator;
      }

      Thread.sleep(BACKOFF);
    }
  }
}
