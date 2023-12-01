package ai.promoted.metrics.logprocessor.common.queryablestate;

import static org.apache.flink.util.Preconditions.checkState;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.contrib.streaming.state.AbstractRocksDBState;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/** Queries directly from rocksdb state in a separate thread. */
@SuppressWarnings("rawtypes")
public class RocksdbStateBackendQuerier {

  private static final Logger LOG = LogManager.getLogger(RocksdbStateBackendQuerier.class);

  private final Reflector reflector = new Reflector();

  private final RocksDB db;

  private final int numberOfKeyGroups;

  private final Map<String, State> createdKVStates;

  private final KeyGroupRange keyGroupRange;

  private final TypeSerializer<?> keySerializer;

  SerializedCompositeKeyBuilder sharedRocksKeyBuilder;

  public RocksdbStateBackendQuerier(StreamOperator<?> targetOperator) {
    RocksDBKeyedStateBackend<?> stateBackend;
    if (targetOperator instanceof AbstractStreamOperator) {
      stateBackend =
          (RocksDBKeyedStateBackend<?>)
              ((AbstractStreamOperator<?>) targetOperator).getKeyedStateBackend();
    } else if (targetOperator instanceof AbstractStreamOperatorV2<?>) {
      stateBackend =
          (RocksDBKeyedStateBackend<?>)
              ((AbstractStreamOperatorV2<?>) targetOperator).getKeyedStateBackend();
    } else {
      throw new RuntimeException("Not supported operator class: " + targetOperator.getClass());
    }

    numberOfKeyGroups =
        reflector.getFieldValue(stateBackend, AbstractKeyedStateBackend.class, "numberOfKeyGroups");
    db = reflector.getFieldValue(stateBackend, "db");
    createdKVStates = new HashMap<>(reflector.getFieldValue(stateBackend, "createdKVStates"));
    keyGroupRange =
        reflector.getFieldValue(stateBackend, AbstractKeyedStateBackend.class, "keyGroupRange");
    LOG.info("states = {}, range = {}", createdKVStates.keySet(), keyGroupRange);

    SerializedCompositeKeyBuilder<?> rocksKeyBuilder =
        reflector.getFieldValue(stateBackend, "sharedRocksKeyBuilder");
    // Need to clone this class since it cloud not be accessed across the threads
    keySerializer =
        ((TypeSerializer<?>) reflector.getFieldValue(rocksKeyBuilder, "keySerializer")).duplicate();
    sharedRocksKeyBuilder =
        new SerializedCompositeKeyBuilder<>(
            keySerializer, reflector.getFieldValue(rocksKeyBuilder, "keyGroupPrefixBytes"), 32);
  }

  @SuppressWarnings("unchecked")
  public byte[] query(String stateName, byte[] keyBytes) {
    try {
      State state = createdKVStates.get(stateName);
      checkState(state != null, "The requested state " + stateName + " not found ");

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
