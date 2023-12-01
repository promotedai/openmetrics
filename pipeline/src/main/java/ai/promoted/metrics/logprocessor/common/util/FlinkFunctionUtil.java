package ai.promoted.metrics.logprocessor.common.util;

import java.lang.reflect.Field;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.runtime.state.DefaultKeyedStateStore;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Preconditions;

public class FlinkFunctionUtil {

  /** Uses reflection to verify that RocksDB state backend is used. */
  public static void validateRocksDBBackend(RuntimeContext runtimeContext)
      throws NoSuchFieldException, IllegalAccessException {
    Field keyedStateStoreField = StreamingRuntimeContext.class.getDeclaredField("keyedStateStore");
    keyedStateStoreField.setAccessible(true);
    KeyedStateStore stateStore = (KeyedStateStore) keyedStateStoreField.get(runtimeContext);
    Field stateBackendField = DefaultKeyedStateStore.class.getDeclaredField("keyedStateBackend");
    stateBackendField.setAccessible(true);
    Object stateBackend = stateBackendField.get(stateStore);
    Preconditions.checkArgument(
        (stateBackend instanceof RocksDBKeyedStateBackend),
        "For efficiency, this function only works for RocksDB backend.  stateBackend={}",
        stateBackend.getClass().getSimpleName());
  }
}
