package ai.promoted.metrics.logprocessor.common.queryablestate;

import static ai.promoted.metrics.logprocessor.common.util.FlinkFunctionUtil.validateRocksDBBackend;
import static org.apache.flink.util.Preconditions.checkState;

import ai.promoted.proto.flinkqueryablestate.MetricsValues;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.AbstractRocksDBState;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.DefaultKeyedStateStore;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public abstract class KeyedProcessFunctionWithQueryableState<K, I, O>
    extends KeyedProcessFunction<K, I, O> {
  private final boolean queryableStateEnabled;

  private transient ValueState<MetricsValues> queryableState;

  protected transient RocksdbStateAccesser rocksdbStateAccesser;

  public KeyedProcessFunctionWithQueryableState(boolean queryableStateEnabled) {
    this.queryableStateEnabled = queryableStateEnabled;
  }

  @Override
  public void open(Configuration confi) throws Exception {
    if (queryableStateEnabled) {
      validateRocksDBBackend(getRuntimeContext());
    }
    queryableState =
        getRuntimeContext()
            .getState(
                new ValueStateDescriptor<>("QueryableState", new MetricsValuesTypeInformation()));
  }

  protected boolean isQueryableStateEnabled() {
    return queryableStateEnabled;
  }

  /**
   * @return true if the state is updated; false if the state is cleared.
   */
  protected boolean updateOrClearQueryableState(MetricsValues stateValue) throws IOException {
    if (!queryableStateEnabled) {
      queryableState.clear();
      return false;
    }
    queryableState.update(stateValue);
    return true;
  }

  /** Put this to the end of open() after all states have been declared for debug purposes. */
  protected void initRocksdbAccessor() throws Exception {
    Field keyedStateStoreField = StreamingRuntimeContext.class.getDeclaredField("keyedStateStore");
    keyedStateStoreField.setAccessible(true);
    KeyedStateStore stateStore = (KeyedStateStore) keyedStateStoreField.get(getRuntimeContext());
    Field stateBackendField = DefaultKeyedStateStore.class.getDeclaredField("keyedStateBackend");
    stateBackendField.setAccessible(true);
    rocksdbStateAccesser =
        new RocksdbStateAccesser((RocksDBKeyedStateBackend<?>) stateBackendField.get(stateStore));
  }

  public static class RocksdbStateAccesser {
    private static final Logger LOG = LogManager.getLogger(RocksdbStateAccesser.class);
    private final Reflector reflector = new Reflector();

    private final RocksDB db;

    private final int numberOfKeyGroups;

    private final Map<String, State> createdKVStates;

    private final KeyGroupRange keyGroupRange;

    private final TypeSerializer<?> keySerializer;

    SerializedCompositeKeyBuilder sharedRocksKeyBuilder;

    public RocksdbStateAccesser(RocksDBKeyedStateBackend<?> stateBackend) {
      numberOfKeyGroups =
          reflector.getFieldValue(
              stateBackend, AbstractKeyedStateBackend.class, "numberOfKeyGroups");
      db = reflector.getFieldValue(stateBackend, "db");
      createdKVStates = new HashMap<>(reflector.getFieldValue(stateBackend, "createdKVStates"));
      keyGroupRange =
          reflector.getFieldValue(stateBackend, AbstractKeyedStateBackend.class, "keyGroupRange");

      SerializedCompositeKeyBuilder<?> rocksKeyBuilder =
          reflector.getFieldValue(stateBackend, "sharedRocksKeyBuilder");
      // Need to clone this class since it cloud not be accessed across the threads
      keySerializer =
          ((TypeSerializer<?>) reflector.getFieldValue(rocksKeyBuilder, "keySerializer"))
              .duplicate();
      sharedRocksKeyBuilder =
          new SerializedCompositeKeyBuilder<>(
              keySerializer, reflector.getFieldValue(rocksKeyBuilder, "keyGroupPrefixBytes"), 32);
    }

    @SuppressWarnings("unchecked")
    public byte[] query(byte[] keyBytes) {
      try {
        State state = createdKVStates.get("QueryableState");
        checkState(state != null, "The requested QueryableState was not found");

        Object key = keySerializer.deserialize(new DataInputDeserializer(keyBytes));
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, numberOfKeyGroups);
        if (!keyGroupRange.contains(keyGroup)) {
          throw new RuntimeException(
              "key " + key + " in group " + keyGroup + " not found in range " + keyGroupRange);
        }

        sharedRocksKeyBuilder.setKeyAndKeyGroup(key, keyGroup);
        if (Class.forName("org.apache.flink.contrib.streaming.state.RocksDBValueState")
            .isInstance(state)) {
          return queryValueState(state);
        }

        throw new UnsupportedOperationException("Not supported type: " + state.getClass());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public TypeSerializer<?> getKeySerializer() {
      return keySerializer.duplicate();
    }

    public KeyGroupRange getKeyGroupRange() {
      return keyGroupRange;
    }

    public int getNumberOfKeyGroups() {
      return numberOfKeyGroups;
    }

    @SuppressWarnings("unchecked")
    private byte[] queryValueState(State state) throws RocksDBException {
      ColumnFamilyHandle handler =
          reflector.getFieldValue(state, AbstractRocksDBState.class, "columnFamily");
      byte[] rocksdbKey =
          sharedRocksKeyBuilder.buildCompositeKeyNamespace(
              reflector.getFieldValue(state, AbstractRocksDBState.class, "currentNamespace"),
              ((AbstractRocksDBState) state).getNamespaceSerializer());
      return db.get(handler, rocksdbKey);
    }
  }
}
