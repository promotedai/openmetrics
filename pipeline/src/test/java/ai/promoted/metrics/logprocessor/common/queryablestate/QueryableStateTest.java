package ai.promoted.metrics.logprocessor.common.queryablestate;

import ai.promoted.proto.flinkqueryablestate.MetricsValues;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class QueryableStateTest {

  private static final MetricsValuesSerializer metricsValuesSerializer =
      new MetricsValuesSerializer();
  private static final IntSerializer intSerializer = new IntSerializer();
  private static final OperatorInfo operatorInfo = Mockito.mock(OperatorInfo.class);

  @BeforeAll
  public static void init() {
    Mockito.when(operatorInfo.getMaxParallelism()).thenReturn(5);
    Mockito.when(operatorInfo.getParallelism()).thenReturn(1);
    Mockito.when(operatorInfo.getKeySerializer())
        .then(invocationOnMock -> Types.INT.createSerializer(new ExecutionConfig()));
  }

  private static class MockQuerier {
    public MockQuerier() {}

    public byte[] query(String stateName, byte[] keyBytes)
        throws IOException, InterruptedException {
      int key = deserializeInt(keyBytes);
      if (key > 0) {
        // Double the key and put it as a double value in the result
        MetricsValues metricsValues =
            MetricsValues.newBuilder()
                .addDoubleValues(MetricsValues.DoubleEntry.newBuilder().setValue(key * 2))
                .build();
        return serializeMetricsValues(metricsValues);
      } else {
        // No state is found for a negative key
        return ByteString.EMPTY.toByteArray();
      }
    }

    private int deserializeInt(byte[] value) throws IOException {
      DataInputDeserializer dataInputDeserializer = new DataInputDeserializer(value);
      return intSerializer.deserialize(dataInputDeserializer);
    }

    private byte[] serializeMetricsValues(MetricsValues metricsValues) throws IOException {
      DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(1024);
      metricsValuesSerializer.serialize(metricsValues, dataOutputSerializer);
      return dataOutputSerializer.getCopyOfBuffer();
    }
  }

  @Test
  public void testStateQuery() throws IOException, InterruptedException {
    ThreadLocal<MockQuerier> stateBackendQuerier = ThreadLocal.withInitial(MockQuerier::new);
    TaskServer taskServer =
        new TaskServer(operatorInfo, (s, bytes) -> stateBackendQuerier.get().query(s, bytes), null);
    taskServer.start();
    CoordinatorServer coordinatorServer = CoordinatorServer.getCoordinatorServer(12345);
    coordinatorServer.update("uid", List.of(taskServer.getAddress()), 1, 5);
    OperatorStateQueryClient client =
        new OperatorStateQueryClient("uid", () -> "localhost:12345", true);
    client.connect();

    List<Integer> keys = Arrays.asList(-1, 1, 2, 3);
    List<byte[]> requestList = keys.stream().map(this::serializeInt).collect(Collectors.toList());
    List<Integer> requestHashList =
        keys.stream().map(Object::hashCode).collect(Collectors.toList());
    List<Double> resultList =
        client.query(requestList, requestHashList, "fakeStateName").stream()
            .map(
                resultBytes -> {
                  if (resultBytes.length > 0) {
                    return deserializeMetricsValues(resultBytes).getDoubleValues(0).getValue();
                  } else {
                    return -1d;
                  }
                })
            .collect(Collectors.toList());
    taskServer.stop();
    // input * 2
    Assertions.assertEquals(List.of(-1.0, 2.0, 4.0, 6.0), resultList);
  }

  private byte[] serializeInt(Integer value) {
    try {
      DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(1024);
      intSerializer.serialize(value, dataOutputSerializer);
      return dataOutputSerializer.getCopyOfBuffer();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private MetricsValues deserializeMetricsValues(byte[] bytes) {
    DataInputDeserializer dataInputDeserializer = new DataInputDeserializer(bytes);
    try {
      return metricsValuesSerializer.deserialize(dataInputDeserializer);
    } catch (IOException e) {
      e.printStackTrace();
      return MetricsValues.newBuilder().build();
    }
  }
}
