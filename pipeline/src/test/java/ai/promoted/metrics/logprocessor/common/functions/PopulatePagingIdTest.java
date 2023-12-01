package ai.promoted.metrics.logprocessor.common.functions;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.promoted.proto.common.ClientInfo;
import ai.promoted.proto.common.ClientInfo.ClientType;
import ai.promoted.proto.common.ClientInfo.TrafficType;
import ai.promoted.proto.common.Properties;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.delivery.DeliveryExecution;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.ExecutionServer;
import ai.promoted.proto.delivery.PagingInfo;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.delivery.Response;
import ai.promoted.proto.delivery.UseCase;
import ai.promoted.proto.event.CombinedDeliveryLog;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Test;

public class PopulatePagingIdTest {
  private static final PopulatePagingId fn = new PopulatePagingId();

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
      String requestId, ExecutionServer executionServer, String pagingId) {
    return createDeliveryLog(
        requestId, "c" + requestId, executionServer, TrafficType.PRODUCTION, pagingId);
  }

  private DeliveryLog.Builder setPagingId(DeliveryLog.Builder builder, String pagingId) {
    builder.getResponseBuilder().getPagingInfoBuilder().setPagingId(pagingId);
    return builder;
  }

  @Test
  public void badInput() throws Exception {
    assertThrows(
        IllegalArgumentException.class, () -> fn.map(CombinedDeliveryLog.getDefaultInstance()));
  }

  @Test
  public void hasApiNoPagingId() throws Exception {
    CombinedDeliveryLog combinedDeliveryLog =
        CombinedDeliveryLog.newBuilder()
            .setApi(createDeliveryLog("req1", ExecutionServer.API, ""))
            .build();

    CombinedDeliveryLog.Builder expectedBuilder = combinedDeliveryLog.toBuilder();
    setPagingId(expectedBuilder.getApiBuilder(), "rlizOJOeVHE5tOZcXWHnYA==");
    assertThat(fn.map(combinedDeliveryLog)).isEqualTo(expectedBuilder.build());
  }

  @Test
  public void hasApiHasPagingId() throws Exception {
    CombinedDeliveryLog combinedDeliveryLog =
        CombinedDeliveryLog.newBuilder()
            .setApi(createDeliveryLog("req1", ExecutionServer.API, "paging1"))
            .build();

    assertThat(fn.map(combinedDeliveryLog)).isEqualTo(combinedDeliveryLog);
  }

  @Test
  public void hasSdkNoPagingId() throws Exception {
    CombinedDeliveryLog combinedDeliveryLog =
        CombinedDeliveryLog.newBuilder()
            .setSdk(createDeliveryLog("req1", ExecutionServer.SDK, ""))
            .build();

    CombinedDeliveryLog.Builder expectedBuilder = combinedDeliveryLog.toBuilder();
    setPagingId(expectedBuilder.getSdkBuilder(), "rlizOJOeVHE5tOZcXWHnYA==");
    assertThat(fn.map(combinedDeliveryLog)).isEqualTo(expectedBuilder.build());
  }

  @Test
  public void hasSdkHasPagingId() throws Exception {
    CombinedDeliveryLog combinedDeliveryLog =
        CombinedDeliveryLog.newBuilder()
            .setSdk(createDeliveryLog("req1", ExecutionServer.SDK, "paging1"))
            .build();

    assertThat(fn.map(combinedDeliveryLog)).isEqualTo(combinedDeliveryLog);
  }

  @Test
  public void hasBothNoPagingId() throws Exception {
    CombinedDeliveryLog combinedDeliveryLog =
        CombinedDeliveryLog.newBuilder()
            .setSdk(createDeliveryLog("req1", ExecutionServer.SDK, ""))
            .setApi(createDeliveryLog("req2", ExecutionServer.API, ""))
            .build();

    CombinedDeliveryLog.Builder expectedBuilder = combinedDeliveryLog.toBuilder();
    setPagingId(expectedBuilder.getSdkBuilder(), "rlizOJOeVHE5tOZcXWHnYA==");
    setPagingId(expectedBuilder.getApiBuilder(), "rlizOJOeVHE5tOZcXWHnYA==");
    assertThat(fn.map(combinedDeliveryLog)).isEqualTo(expectedBuilder.build());
  }

  @Test
  public void hasBothHasApiPagingId() throws Exception {
    CombinedDeliveryLog combinedDeliveryLog =
        CombinedDeliveryLog.newBuilder()
            .setSdk(createDeliveryLog("req1", ExecutionServer.SDK, ""))
            .setApi(createDeliveryLog("req2", ExecutionServer.API, "apipaging"))
            .build();

    CombinedDeliveryLog.Builder expectedBuilder = combinedDeliveryLog.toBuilder();
    setPagingId(expectedBuilder.getSdkBuilder(), "apipaging");
    assertThat(fn.map(combinedDeliveryLog)).isEqualTo(expectedBuilder.build());
  }

  @Test
  public void hasBothHasSdkPagingId() throws Exception {
    CombinedDeliveryLog combinedDeliveryLog =
        CombinedDeliveryLog.newBuilder()
            .setSdk(createDeliveryLog("req1", ExecutionServer.SDK, "sdkpaging"))
            .setApi(createDeliveryLog("req2", ExecutionServer.API, ""))
            .build();

    CombinedDeliveryLog.Builder expectedBuilder = combinedDeliveryLog.toBuilder();
    setPagingId(expectedBuilder.getApiBuilder(), "sdkpaging");
    assertThat(fn.map(combinedDeliveryLog)).isEqualTo(expectedBuilder.build());
  }

  @Test
  public void hasBothBothHasPagingId() throws Exception {
    CombinedDeliveryLog combinedDeliveryLog =
        CombinedDeliveryLog.newBuilder()
            .setSdk(createDeliveryLog("req1", ExecutionServer.SDK, "sdkpaging"))
            .setApi(createDeliveryLog("req2", ExecutionServer.API, "apipaging"))
            .build();

    assertThat(fn.map(combinedDeliveryLog)).isEqualTo(combinedDeliveryLog);
  }

  @Test
  public void generatePagingId_all() throws Exception {
    Request request = createRequest();
    assertThat(PopulatePagingId.generatePagingId(request)).isEqualTo("hSu/AKkvsmJdYsSKQRmLWQ==");
  }

  // Shouldn't change the hash.
  @Test
  public void generatePagingId_reorderProperties() throws Exception {
    Request request =
        createRequest().toBuilder()
            .setProperties(
                Properties.newBuilder()
                    .setStruct(
                        Struct.newBuilder()
                            .putFields("beta", Value.newBuilder().setNumberValue(2.0).build())
                            .putFields("alpha", Value.newBuilder().setNumberValue(1.0).build())))
            .build();
    assertThat(PopulatePagingId.generatePagingId(request)).isEqualTo("hSu/AKkvsmJdYsSKQRmLWQ==");
  }

  @Test
  public void generatePagingId_clearPlatformId() throws Exception {
    Request request = createRequest().toBuilder().setPlatformId(10L).build();
    assertThat(PopulatePagingId.generatePagingId(request)).isEqualTo("pQcBY404ATbO5ii6cKarNw==");
  }

  @Test
  public void generatePagingId_clearLogUserId() throws Exception {
    Request request = createRequest().toBuilder().clearUserInfo().build();
    assertThat(PopulatePagingId.generatePagingId(request)).isEqualTo("YIGA2NmuNFxJaEs4Z2qdDA==");
  }

  @Test
  public void generatePagingId_clearClientTraffic() throws Exception {
    Request.Builder requestBuilder = createRequest().toBuilder();
    requestBuilder.getClientInfoBuilder().clearClientType();
    assertThat(PopulatePagingId.generatePagingId(requestBuilder.build()))
        .isEqualTo("JO3bfTk0A2yFUuyipEow/Q==");
  }

  @Test
  public void generatePagingId_clearTrafficType() throws Exception {
    Request.Builder requestBuilder = createRequest().toBuilder();
    requestBuilder.getClientInfoBuilder().clearTrafficType();
    assertThat(PopulatePagingId.generatePagingId(requestBuilder.build()))
        .isEqualTo("w+0t2bC6CxzEc2urKxmxfQ==");
  }

  @Test
  public void generatePagingId_clearUseCase() throws Exception {
    Request request = createRequest().toBuilder().clearUseCase().build();
    assertThat(PopulatePagingId.generatePagingId(request)).isEqualTo("lXmenjKYfNS1meSdsst9vw==");
  }

  @Test
  public void generatePagingId_clearSearchQuery() throws Exception {
    Request request = createRequest().toBuilder().clearSearchQuery().build();
    assertThat(PopulatePagingId.generatePagingId(request)).isEqualTo("6vt/a+C37sGb3isSgNjiHw==");
  }

  @Test
  public void generatePagingId_clearProperties() throws Exception {
    Request request = createRequest().toBuilder().clearProperties().build();
    assertThat(PopulatePagingId.generatePagingId(request)).isEqualTo("t7R0nuUA8Ehxza0jtVDqSg==");
  }

  private static Request createRequest() {
    return Request.newBuilder()
        .setPlatformId(1L)
        .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1").build())
        .setClientInfo(
            ClientInfo.newBuilder()
                .setClientType(ClientType.PLATFORM_CLIENT)
                .setTrafficType(TrafficType.PRODUCTION)
                .build())
        .setUseCase(UseCase.SEARCH)
        .setRequestId("req1")
        .setClientRequestId("creq1")
        .setSearchQuery("search query")
        .setProperties(
            Properties.newBuilder()
                .setStruct(
                    Struct.newBuilder()
                        .putFields("alpha", Value.newBuilder().setNumberValue(1.0).build())
                        .putFields("beta", Value.newBuilder().setNumberValue(2.0).build())))
        .build();
  }
}
