package ai.promoted.metrics.logprocessor.common.queryablestate;

import static ai.promoted.proto.delivery.internal.features.AggMetric.COUNT_CHECKOUT;
import static ai.promoted.proto.delivery.internal.features.AggMetric.COUNT_IMPRESSION;

import ai.promoted.metrics.logprocessor.common.counter.PlatformContentDeviceEvent;
import ai.promoted.metrics.logprocessor.common.counter.PlatformDeviceEvent;
import ai.promoted.metrics.logprocessor.common.counter.PlatformLogUserContentEvent;
import ai.promoted.metrics.logprocessor.common.counter.PlatformLogUserQueryEvent;
import ai.promoted.proto.flinkqueryablestate.BatchProxyRequest;
import ai.promoted.proto.flinkqueryablestate.BatchResult;
import ai.promoted.proto.flinkqueryablestate.BatchStateRequest;
import ai.promoted.proto.flinkqueryablestate.MetricsValues;
import ai.promoted.proto.flinkqueryablestate.ProxyGrpc;
import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/** TODO Use the following methods for manual tests only. Will automate them in a follow-up PR. */
public class StateQueryJobClientTest {
  private static Logger LOG = LogManager.getLogger(StateQueryJobClientTest.class);

  public static class StateQueryHelper<KEY extends Tuple> {
    AddressRetriever addressRetriever =
        () -> "ad5a23e2ed3f441d29184f329f2177d7-162e7435de7483eb.elb.us-east-1.amazonaws.com:12345";
    private String job = "queryable-state-service-queryable";
    private String uid;
    private TupleSerializer<KEY> tupleSerializer;
    private OperatorStateQueryClient client;
    private volatile boolean connected = false;
    Channel channel =
        Grpc.newChannelBuilder(addressRetriever.getAddress(), InsecureChannelCredentials.create())
            .build();
    ProxyGrpc.ProxyBlockingStub stub = ProxyGrpc.newBlockingStub(channel);

    public StateQueryHelper(String uid, Class<KEY> keyType) {
      this.uid = uid;
      tupleSerializer =
          (TupleSerializer<KEY>)
              TypeInformation.of(keyType).createSerializer(new ExecutionConfig());
      client = new OperatorStateQueryClient(uid, () -> "localhost:12345", true);
    }

    public List<MetricsValues> proxyRequest(List<KEY> keyList) throws IOException {

      BatchProxyRequest.Builder proxyRequestBuilder =
          BatchProxyRequest.newBuilder().setJob(job).setUid(uid);
      BatchStateRequest.Builder batchStateRequestBuilder =
          BatchStateRequest.newBuilder().setStateName("QueryableState");
      DataOutputSerializer serializer = new DataOutputSerializer(1024);
      List<byte[]> keyBytesList =
          keyList.stream()
              .map(
                  key -> {
                    try {
                      tupleSerializer.serialize(key, serializer);
                      return serializer.getCopyOfBuffer();
                    } catch (IOException e) {
                      throw new RuntimeException(e);
                    } finally {
                      serializer.clear();
                    }
                  })
              .collect(Collectors.toList());
      for (byte[] keyBytes : keyBytesList) {
        batchStateRequestBuilder.addKeys(ByteString.copyFrom(keyBytes));
      }
      proxyRequestBuilder.setStateRequest(batchStateRequestBuilder.build());
      proxyRequestBuilder.addAllKeyHashes(
          keyList.stream().map(Objects::hashCode).collect(Collectors.toList()));

      DataInputDeserializer deserializer = new DataInputDeserializer();
      MetricsValuesSerializer resultDeserializer = new MetricsValuesSerializer();
      long time = System.currentTimeMillis();
      BatchResult batchResult = stub.proxyQuery(proxyRequestBuilder.build());
      LOG.info("Proxy Query finished in " + (System.currentTimeMillis() - time));
      return batchResult.getResultsList().stream()
          .map(
              r -> {
                byte[] resultBytes = r.toByteArray();
                MetricsValues result = null;
                if (resultBytes.length > 0) {
                  deserializer.setBuffer(resultBytes);
                  try {
                    result = resultDeserializer.deserialize(deserializer);
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                  LOG.info(result);
                } else {
                  LOG.info("Empty results");
                }
                return result;
              })
          .collect(Collectors.toList());
    }

    private synchronized void initConnection() {
      if (!connected) {
        client.connect();
        connected = true;
      }
    }

    public List<MetricsValues> portforwardRequest(List<KEY> keyList) throws IOException {
      initConnection();

      DataOutputSerializer serializer = new DataOutputSerializer(1024);
      DataInputDeserializer deserializer = new DataInputDeserializer();
      long time = System.currentTimeMillis();
      List<byte[]> queryBytesList = new ArrayList<>();
      List<Integer> queryHashList = new ArrayList<>();
      for (KEY key : keyList) {
        tupleSerializer.serialize(key, serializer);
        queryBytesList.add(serializer.getCopyOfBuffer());
        queryHashList.add(key.hashCode());
        serializer.clear();
      }
      List<byte[]> resultBytesList = client.query(queryBytesList, queryHashList, "QueryableState");
      LOG.info("Portforward Query finished in " + (System.currentTimeMillis() - time));
      List<MetricsValues> result = new ArrayList<>();
      for (byte[] resultBytes : resultBytesList) {
        if (resultBytes.length > 0) {
          MetricsValuesSerializer resultDeserializer = new MetricsValuesSerializer();
          deserializer.setBuffer(resultBytes);
          result.add(resultDeserializer.deserialize(deserializer));
          LOG.info(result);
        } else {
          LOG.info("Empty results");
        }
        serializer.clear();
      }
      return result;
    }
  }

  @Test
  @Disabled
  public void testLastUserQueryWithProxy() throws IOException {
    StateQueryHelper<PlatformLogUserQueryEvent> stateQueryHelper =
        new StateQueryHelper<>(
            "LASTTIME-impression-BY-plat-logUsr-qry_$evt", PlatformLogUserQueryEvent.class);
    PlatformLogUserQueryEvent platformLogUserQueryEvent =
        new PlatformLogUserQueryEvent(120L, "", -1205034819632174695L, COUNT_IMPRESSION.name());
    stateQueryHelper.proxyRequest(List.of(platformLogUserQueryEvent));
  }

  @Test
  @Disabled
  public void testLastUserEventImpression() throws IOException {
    StateQueryHelper<PlatformLogUserContentEvent> stateQueryHelper =
        new StateQueryHelper<>(
            "LASTTIME-impression-BY-plat-logUsr-cont_$evt", PlatformLogUserContentEvent.class);
    PlatformLogUserContentEvent lastPlatformLogUserContentEvent =
        new PlatformLogUserContentEvent(
            120L,
            "59a6da96-5ccf-45fe-aeda-17d34ddf2c84",
            "f32f7b92-0011-4546-9d71-521e232858e0",
            COUNT_IMPRESSION.name());
    stateQueryHelper.proxyRequest(List.of(lastPlatformLogUserContentEvent));
  }

  @Test
  @Disabled
  public void testCountImpressionContentDvcWithProxy() throws IOException {
    StateQueryHelper<PlatformContentDeviceEvent> stateQueryHelper =
        new StateQueryHelper<>(
            "COUNT-impression-BY-plat-cont_dvc-$evt$hourly", PlatformContentDeviceEvent.class);
    List<String> devices = List.of("mweb", "nios", "nandroid", "web", "other");
    List<PlatformContentDeviceEvent> keyList = new ArrayList<>();
    for (String device : devices) {
      PlatformContentDeviceEvent tupleKey =
          new PlatformContentDeviceEvent(
              120L, "6a8bf0d7-f3be-491a-af8f-caf451706886", device, COUNT_IMPRESSION.name());
      keyList.add(tupleKey);
    }
    stateQueryHelper.proxyRequest(keyList);
  }

  @Test
  @Disabled
  public void testQueryImpressionPlatformDvcWithProxy() throws IOException {
    StateQueryHelper<PlatformDeviceEvent> stateQueryHelper =
        new StateQueryHelper<>(
            "COUNT-impression-BY-plat_dvc-$evt$daily", PlatformDeviceEvent.class);
    List<String> devices = List.of("mweb", "nios", "nandroid", "web", "other");
    List<PlatformDeviceEvent> keyList = new ArrayList<>();
    for (String device : devices) {
      PlatformDeviceEvent tupleKey = new PlatformDeviceEvent(120L, device, COUNT_IMPRESSION.name());
      keyList.add(tupleKey);
    }
    stateQueryHelper.proxyRequest(keyList);
  }

  @Test
  @Disabled
  public void testQueryActionPlatformDvcWithProxy() throws IOException {
    StateQueryHelper<PlatformDeviceEvent> stateQueryHelper =
        new StateQueryHelper<>("COUNT-action-BY-plat_dvc-$evt$hourly", PlatformDeviceEvent.class);
    List<String> devices = List.of("mweb", "nios", "nandroid", "web", "other");
    for (int i = 0; i < 10; ++i) {
      for (String device : devices) {
        PlatformDeviceEvent tupleKey = new PlatformDeviceEvent(120L, device, COUNT_CHECKOUT.name());
        //      List<MetricsValues> result = stateQueryHelper.portforwardRequest(List.of(tupleKey));
        List<MetricsValues> result = stateQueryHelper.proxyRequest(List.of(tupleKey));
        LOG.info(result);
      }
    }
  }
}
