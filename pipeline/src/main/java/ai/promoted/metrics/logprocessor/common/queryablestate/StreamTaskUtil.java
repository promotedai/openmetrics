package ai.promoted.metrics.logprocessor.common.queryablestate;

import ai.promoted.metrics.logprocessor.common.functions.base.SerializableSupplier;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.flink.streaming.runtime.tasks.StreamOperatorWrapper;

public class StreamTaskUtil {
  public static void waitToRegister(
      Iterable<StreamOperatorWrapper<?, ?>> operators,
      int subtaskIndex,
      SerializableSupplier<Boolean> isRunning) {
    ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
    CompletableFuture.runAsync(
            () -> {
              while (!isRunning.get()) {
                try {
                  TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              }
            },
            singleThreadExecutor)
        .thenRun(
            () ->
                operators.forEach(
                    o ->
                        OperatorRegistry.get()
                            .register(
                                o.getStreamOperator().getOperatorID(),
                                subtaskIndex,
                                o.getStreamOperator())))
        .thenRunAsync(singleThreadExecutor::shutdown);
  }
}
