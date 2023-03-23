package ai.promoted.metrics.logprocessor.common.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.promoted.metrics.logprocessor.common.util.DebugIds;
import ai.promoted.proto.common.ClientInfo;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.delivery.DeliveryExecution;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.ExecutionServer;
import ai.promoted.proto.delivery.PagingInfo;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.delivery.Response;
import ai.promoted.proto.event.CombinedDeliveryLog;
import com.google.common.collect.ImmutableList;
import com.twitter.chill.protobuf.ProtobufSerializer;
import java.time.Duration;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

public class CombineDeliveryLogTest {
  private static DeliveryLog createDeliveryLog(
      String requestId,
      String clientRequestId,
      ExecutionServer executionServer,
      ClientInfo.TrafficType trafficType,
      String pagingId) {
    Response.Builder responseBuilder = Response.newBuilder();
    if (!pagingId.isEmpty()) {
      responseBuilder.setPagingInfo(PagingInfo.newBuilder().setPagingId(pagingId));
    }

    return DeliveryLog.newBuilder()
        .setPlatformId(1L)
        .setRequest(
            Request.newBuilder()
                .setPlatformId(1L)
                .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1").build())
                .setClientInfo(ClientInfo.newBuilder().setTrafficType(trafficType).build())
                .setRequestId(requestId)
                .setClientRequestId(clientRequestId))
        .setExecution(DeliveryExecution.newBuilder().setExecutionServer(executionServer).build())
        .setResponse(responseBuilder)
        .build();
  }

  private static DeliveryLog createDeliveryLog(
      String requestId,
      String clientRequestId,
      ExecutionServer executionServer,
      ClientInfo.TrafficType trafficType) {
    return createDeliveryLog(requestId, clientRequestId, executionServer, trafficType, "");
  }

  private static DeliveryLog createDeliveryLog(
      String requestId, String clientRequestId, ExecutionServer executionServer, String pagingId) {
    return createDeliveryLog(
        requestId, clientRequestId, executionServer, ClientInfo.TrafficType.PRODUCTION, pagingId);
  }

  private static DeliveryLog createDeliveryLog(
      String requestId, String clientRequestId, ExecutionServer executionServer) {
    return createDeliveryLog(
        requestId, clientRequestId, executionServer, ClientInfo.TrafficType.PRODUCTION);
  }

  private KeyedOneInputStreamOperatorTestHarness<
          Tuple2<String, Long>, DeliveryLog, CombinedDeliveryLog>
      createHarness(CombineDeliveryLog combineDeliveryLog) throws Exception {
    KeyedOneInputStreamOperatorTestHarness harness =
        ProcessFunctionTestHarnesses.forKeyedProcessFunction(
            combineDeliveryLog,
            KeyUtil.deliveryLogLogUserIdKey,
            Types.TUPLE(Types.LONG, Types.STRING));
    harness.setTimeCharacteristic(TimeCharacteristic.EventTime);
    harness
        .getExecutionConfig()
        .registerTypeWithKryoSerializer(DeliveryLog.class, ProtobufSerializer.class);
    return harness;
  }

  @Test
  public void noEvents() throws Exception {
    CombineDeliveryLog combineDeliveryLog =
        new CombineDeliveryLog(Duration.ofMillis(5000L), DebugIds.empty());
    KeyedOneInputStreamOperatorTestHarness harness = createHarness(combineDeliveryLog);
    harness.processWatermark(new Watermark(10000));
    assertEquals(ImmutableList.of(), harness.extractOutputValues());
    assertEquals(0, harness.numEventTimeTimers());
  }

  @Test
  public void oneSdk() throws Exception {
    CombineDeliveryLog combineDeliveryLog =
        new CombineDeliveryLog(Duration.ofMillis(5000L), DebugIds.empty());
    KeyedOneInputStreamOperatorTestHarness harness = createHarness(combineDeliveryLog);
    harness.processElement(createDeliveryLog("req1", "clientReq1", ExecutionServer.SDK), 100);
    assertEquals(1, harness.numEventTimeTimers());
    harness.processWatermark(new Watermark(10000));
    assertEquals(
        ImmutableList.of(
            CombinedDeliveryLog.newBuilder()
                .setSdk(createDeliveryLog("req1", "clientReq1", ExecutionServer.SDK))
                .build()),
        harness.extractOutputValues());
    assertEquals(0, harness.numEventTimeTimers());
  }

  @Test
  public void oneApi() throws Exception {
    CombineDeliveryLog combineDeliveryLog =
        new CombineDeliveryLog(Duration.ofMillis(5000L), DebugIds.empty());
    KeyedOneInputStreamOperatorTestHarness harness = createHarness(combineDeliveryLog);
    harness.processElement(
        createDeliveryLog("req1", "clientReq1", ExecutionServer.API, "pagingId1"), 100);
    assertEquals(1, harness.numEventTimeTimers());
    harness.processWatermark(new Watermark(10000));
    assertEquals(
        ImmutableList.of(
            CombinedDeliveryLog.newBuilder()
                .setApi(createDeliveryLog("req1", "clientReq1", ExecutionServer.API, "pagingId1"))
                .build()),
        harness.extractOutputValues());
    assertEquals(0, harness.numEventTimeTimers());
  }

  @Test
  public void oneSdkAndOneApi() throws Exception {
    CombineDeliveryLog combineDeliveryLog =
        new CombineDeliveryLog(Duration.ofMillis(5000L), DebugIds.empty());
    KeyedOneInputStreamOperatorTestHarness harness = createHarness(combineDeliveryLog);
    harness.processElement(
        createDeliveryLog("req1", "clientReq1", ExecutionServer.API, "pagingId1"), 100);
    harness.processElement(createDeliveryLog("req2", "clientReq1", ExecutionServer.SDK), 500);
    assertEquals(1, harness.numEventTimeTimers());
    harness.processWatermark(new Watermark(10000));
    assertEquals(
        ImmutableList.of(
            CombinedDeliveryLog.newBuilder()
                .setApi(createDeliveryLog("req1", "clientReq1", ExecutionServer.API, "pagingId1"))
                .setSdk(createDeliveryLog("req2", "clientReq1", ExecutionServer.SDK, "pagingId1"))
                .build()),
        harness.extractOutputValues());
    assertEquals(0, harness.numEventTimeTimers());
  }

  @Test
  public void oneSdkAndOneApi_withoutPagingId() throws Exception {
    CombineDeliveryLog combineDeliveryLog =
        new CombineDeliveryLog(Duration.ofMillis(5000L), DebugIds.empty());
    KeyedOneInputStreamOperatorTestHarness harness = createHarness(combineDeliveryLog);
    harness.processElement(createDeliveryLog("req1", "clientReq1", ExecutionServer.API), 100);
    harness.processElement(createDeliveryLog("req2", "clientReq1", ExecutionServer.SDK), 500);
    assertEquals(1, harness.numEventTimeTimers());
    harness.processWatermark(new Watermark(10000));
    assertEquals(
        ImmutableList.of(
            CombinedDeliveryLog.newBuilder()
                .setApi(createDeliveryLog("req1", "clientReq1", ExecutionServer.API))
                .setSdk(createDeliveryLog("req2", "clientReq1", ExecutionServer.SDK))
                .build()),
        harness.extractOutputValues());
    assertEquals(0, harness.numEventTimeTimers());
  }

  @Test
  public void expectedMultipleSdkAndMultipleApi() throws Exception {
    // This case can happen when we log duplicate records.
    CombineDeliveryLog combineDeliveryLog =
        new CombineDeliveryLog(Duration.ofMillis(5000L), DebugIds.empty());
    KeyedOneInputStreamOperatorTestHarness harness = createHarness(combineDeliveryLog);
    harness.processElement(createDeliveryLog("req1", "clientReq1", ExecutionServer.API), 100);
    harness.processElement(createDeliveryLog("req1", "clientReq1", ExecutionServer.API), 200);
    harness.processElement(createDeliveryLog("req2", "clientReq1", ExecutionServer.SDK), 600);
    harness.processElement(createDeliveryLog("req2", "clientReq1", ExecutionServer.SDK), 700);
    assertEquals(1, harness.numEventTimeTimers());
    harness.processWatermark(new Watermark(10000));
    assertEquals(
        ImmutableList.of(
            CombinedDeliveryLog.newBuilder()
                .setApi(createDeliveryLog("req1", "clientReq1", ExecutionServer.API))
                .setSdk(createDeliveryLog("req2", "clientReq1", ExecutionServer.SDK))
                .build()),
        harness.extractOutputValues());
    assertEquals(0, harness.numEventTimeTimers());
  }

  @Test
  public void unexpectedMultipleSdkAndMultipleApi() throws Exception {
    // This could happen if the API is used incorrectly or if the client code has bugs.
    CombineDeliveryLog combineDeliveryLog =
        new CombineDeliveryLog(Duration.ofMillis(5000L), DebugIds.empty());
    KeyedOneInputStreamOperatorTestHarness harness = createHarness(combineDeliveryLog);
    harness.processElement(createDeliveryLog("req1", "clientReq1", ExecutionServer.API), 100);
    harness.processElement(createDeliveryLog("req2", "clientReq1", ExecutionServer.API), 200);
    harness.processElement(createDeliveryLog("req3", "clientReq1", ExecutionServer.SDK), 600);
    harness.processElement(createDeliveryLog("req4", "clientReq1", ExecutionServer.SDK), 700);
    assertEquals(1, harness.numEventTimeTimers());
    harness.processWatermark(new Watermark(10000));
    assertEquals(
        ImmutableList.of(
            CombinedDeliveryLog.newBuilder()
                .setApi(createDeliveryLog("req2", "clientReq1", ExecutionServer.API))
                .setSdk(createDeliveryLog("req4", "clientReq1", ExecutionServer.SDK))
                .build()),
        harness.extractOutputValues());
    assertEquals(0, harness.numEventTimeTimers());
  }

  @Test
  public void lowerPriDoesNotGetSet() throws Exception {
    // This case can happen when we log duplicate records.
    CombineDeliveryLog combineDeliveryLog =
        new CombineDeliveryLog(Duration.ofMillis(5000L), DebugIds.empty());
    KeyedOneInputStreamOperatorTestHarness harness = createHarness(combineDeliveryLog);
    harness.processElement(createDeliveryLog("req1", "clientReq1", ExecutionServer.API), 100);
    harness.processElement(
        createDeliveryLog("req2", "clientReq1", ExecutionServer.API, ClientInfo.TrafficType.SHADOW),
        200);
    harness.processElement(createDeliveryLog("req3", "clientReq1", ExecutionServer.SDK), 600);
    harness.processElement(
        createDeliveryLog("req4", "clientReq1", ExecutionServer.SDK, ClientInfo.TrafficType.REPLAY),
        700);
    assertEquals(1, harness.numEventTimeTimers());
    harness.processWatermark(new Watermark(10000));
    assertEquals(
        ImmutableList.of(
            CombinedDeliveryLog.newBuilder()
                .setApi(createDeliveryLog("req1", "clientReq1", ExecutionServer.API))
                .setSdk(createDeliveryLog("req3", "clientReq1", ExecutionServer.SDK))
                .build()),
        harness.extractOutputValues());
    assertEquals(0, harness.numEventTimeTimers());
  }

  @Test
  public void variety() throws Exception {
    // This could happen if the API is used incorrectly or if the client code has bugs.
    CombineDeliveryLog combineDeliveryLog =
        new CombineDeliveryLog(Duration.ofMillis(5000L), DebugIds.empty());
    KeyedOneInputStreamOperatorTestHarness harness = createHarness(combineDeliveryLog);
    harness.processElement(createDeliveryLog("req1", "clientReq1", ExecutionServer.API), 100);
    harness.processElement(createDeliveryLog("req2", "clientReq2", ExecutionServer.API), 200);
    harness.processElement(createDeliveryLog("req3", "clientReq2", ExecutionServer.SDK), 600);
    harness.processElement(createDeliveryLog("req4", "clientReq3", ExecutionServer.SDK), 700);
    assertEquals(3, harness.numEventTimeTimers());
    harness.processWatermark(new Watermark(10000));
    assertEquals(
        ImmutableList.of(
            CombinedDeliveryLog.newBuilder()
                .setApi(createDeliveryLog("req1", "clientReq1", ExecutionServer.API))
                .build(),
            CombinedDeliveryLog.newBuilder()
                .setApi(createDeliveryLog("req2", "clientReq2", ExecutionServer.API))
                .setSdk(createDeliveryLog("req3", "clientReq2", ExecutionServer.SDK))
                .build(),
            CombinedDeliveryLog.newBuilder()
                .setSdk(createDeliveryLog("req4", "clientReq3", ExecutionServer.SDK))
                .build()),
        harness.extractOutputValues());
    assertEquals(0, harness.numEventTimeTimers());
  }

  @Test
  public void canReuseClientRequestIdAfterWindow() throws Exception {
    CombineDeliveryLog combineDeliveryLog =
        new CombineDeliveryLog(Duration.ofMillis(5000L), DebugIds.empty());
    KeyedOneInputStreamOperatorTestHarness harness = createHarness(combineDeliveryLog);
    harness.processElement(createDeliveryLog("req1", "clientReq1", ExecutionServer.API), 100);
    harness.processElement(createDeliveryLog("req2", "clientReq1", ExecutionServer.SDK), 500);
    assertEquals(1, harness.numEventTimeTimers());
    harness.processWatermark(new Watermark(10000));
    CombinedDeliveryLog expectedCombinedDeliveryLog =
        CombinedDeliveryLog.newBuilder()
            .setApi(createDeliveryLog("req1", "clientReq1", ExecutionServer.API))
            .setSdk(createDeliveryLog("req2", "clientReq1", ExecutionServer.SDK))
            .build();
    assertEquals(ImmutableList.of(expectedCombinedDeliveryLog), harness.extractOutputValues());
    assertEquals(0, harness.numEventTimeTimers());

    harness.processElement(createDeliveryLog("req1", "clientReq1", ExecutionServer.API), 10100);
    harness.processElement(createDeliveryLog("req2", "clientReq1", ExecutionServer.SDK), 10500);
    assertEquals(1, harness.numEventTimeTimers());
    harness.processWatermark(new Watermark(20000));
    assertEquals(
        ImmutableList.of(expectedCombinedDeliveryLog, expectedCombinedDeliveryLog),
        harness.extractOutputValues());
    assertEquals(0, harness.numEventTimeTimers());
  }

  @Test
  public void catchNewExecutionServers() throws Exception {
    assertEquals(
        ImmutableList.of(
            ExecutionServer.UNKNOWN_EXECUTION_SERVER,
            ExecutionServer.API,
            ExecutionServer.SDK,
            ExecutionServer.SIMPLE_API,
            ExecutionServer.UNRECOGNIZED),
        ImmutableList.copyOf(ExecutionServer.values()),
        "Need to update CombineDeliveryLog for changes to ExecutionServer");
  }
}
