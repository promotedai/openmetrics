package ai.promoted.metrics.logprocessor.common.functions;

import ai.promoted.proto.delivery.DeliveryExecution;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.ExecutionServer;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.delivery.Response;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FixDeliveryLogTest {

    @Test
    public void hasResponseInsertionIds() throws Exception {
        DeliveryLog input = DeliveryLog.newBuilder()
                .setRequest(Request.newBuilder()
                        .setRequestId("request1"))
                .setResponse(Response.newBuilder()
                        .addInsertion(createInsertion("insertion2", "content2", 0))
                        .addInsertion(createInsertion("insertion1", "content1", 1)))
                .build();
        assertEquals(
                input,
                new FixDeliveryLog().map(input));
    }

    @Test
    public void doesNotHaveResponseInsertionIds() throws Exception {
        DeliveryLog input = DeliveryLog.newBuilder()
                .setRequest(Request.newBuilder()
                        .setRequestId("request1"))
                .setResponse(Response.newBuilder()
                        .addInsertion(createInsertion("", "content2", 0))
                        .addInsertion(createInsertion("", "content1", 1)))
                .build();
        assertEquals(
                DeliveryLog.newBuilder()
                        .setRequest(Request.newBuilder()
                                .setRequestId("request1"))
                        .setResponse(Response.newBuilder()
                                .addInsertion(createInsertion("request1-0-content2", "content2", 0))
                                .addInsertion(createInsertion("request1-1-content1", "content1", 1)))
                        .build(),
                new FixDeliveryLog().map(input));
    }

    private DeliveryLog createV1OptimizedSdkDeliveryLogInput() {
        return DeliveryLog.newBuilder()
                .setRequest(Request.newBuilder()
                        .setRequestId("request1"))
                .setExecution(DeliveryExecution.newBuilder().setExecutionServer(ExecutionServer.SDK))
                .setResponse(Response.newBuilder()
                        .addInsertion(Insertion.newBuilder()
                                .setInsertionId("insertion1")
                                .setContentId("content1")
                                .setPosition(0)
                                .setRetrievalRank(0)
                                .setRetrievalScore(1))
                        .addInsertion(Insertion.newBuilder()
                                .setInsertionId("insertion2")
                                .setContentId("content2")
                                .setPosition(1)
                                .setRetrievalRank(1)
                                .setRetrievalScore(0.9f)))
                .build();
    }

    @Test
    public void v1OptimizeSdkDeliveryLog_actuallySDK() throws Exception {
        DeliveryLog input = createV1OptimizedSdkDeliveryLogInput();
        assertEquals(
                DeliveryLog.newBuilder()
                        .setRequest(Request.newBuilder()
                                .setRequestId("request1")
                                .addInsertion(Insertion.newBuilder()
                                        .setInsertionId("insertion1")
                                        .setContentId("content1")
                                        .setRetrievalRank(0)
                                        .setRetrievalScore(1))
                                .addInsertion(Insertion.newBuilder()
                                        .setInsertionId("insertion2")
                                        .setContentId("content2")
                                        .setRetrievalRank(1)
                                        .setRetrievalScore(0.9f)))
                        .setExecution(DeliveryExecution.newBuilder().setExecutionServer(ExecutionServer.SDK))
                        .setResponse(Response.newBuilder()
                                .addInsertion(Insertion.newBuilder()
                                        .setInsertionId("insertion1")
                                        .setContentId("content1")
                                        .setPosition(0))
                                .addInsertion(Insertion.newBuilder()
                                        .setInsertionId("insertion2")
                                        .setContentId("content2")
                                        .setPosition(1)))
                        .build(),
                new FixDeliveryLog().map(input));
    }

    @Test
    public void v1OptimizeSdkDeliveryLog_notSdk() throws Exception {
        DeliveryLog input = createV1OptimizedSdkDeliveryLogInput();
        input = input.toBuilder()
                .setExecution(DeliveryExecution.newBuilder().setExecutionServer(ExecutionServer.API))
                .build();
        assertEquals(input, new FixDeliveryLog().map(input));
    }

    private static Insertion createInsertion(String insertionId, String contentId, long position) {
        return Insertion.newBuilder()
                .setInsertionId(insertionId)
                .setContentId(contentId)
                .setPosition(position)
                .build();
    }
}
