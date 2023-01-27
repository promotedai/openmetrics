package ai.promoted.metrics.logprocessor.job.raw;

import ai.promoted.metrics.common.ResponseInsertionIds;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.delivery.Response;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class ToResponseInsertionIdsTest {
    protected Collector<ResponseInsertionIds> mockOut;
    protected ToResponseInsertionIds operator;

    @BeforeEach
    public void setUp() {
        mockOut = Mockito.mock(Collector.class);
        operator = new ToResponseInsertionIds();
    }

    @Test
    public void flatMap() throws Exception {
        operator.flatMap(
                DeliveryLog.newBuilder()
                        .setPlatformId(2L)
                        .setRequest(Request.newBuilder()
                                .setPlatformId(2L)
                                .setTiming(Timing.newBuilder()
                                        .setEventApiTimestamp(1235))
                                .setUserInfo(UserInfo.newBuilder()
                                        .setLogUserId("logUserId1")
                                        .setUserId("userId1"))
                                .setRequestId("req1"))
                        .setResponse(Response.newBuilder()
                                .addInsertion(Insertion.newBuilder()
                                        .setInsertionId("ins0")
                                        .setContentId("content0")
                                        .setPosition(0))
                                .addInsertion(Insertion.newBuilder()
                                        .setInsertionId("ins1")
                                        .setContentId("content1")
                                        .setPosition(1))
                                .addInsertion(Insertion.newBuilder()
                                        .setInsertionId("ins2")
                                        .setContentId("content2")
                                        .setPosition(2)))
                        .build(),
                mockOut);
        verify(mockOut).collect(
                ResponseInsertionIds.newBuilder()
                        .setPlatformId(2L)
                        .setEventApiTimestamp(1235)
                        .setRequestId("req1")
                        .setInsertionId("ins0")
                        .setContentId("content0")
                        .setPosition(0L)
                        .build());
        verify(mockOut).collect(
                ResponseInsertionIds.newBuilder()
                        .setPlatformId(2L)
                        .setEventApiTimestamp(1235)
                        .setRequestId("req1")
                        .setInsertionId("ins1")
                        .setContentId("content1")
                        .setPosition(1L)
                        .build());
        verify(mockOut).collect(
                ResponseInsertionIds.newBuilder()
                        .setPlatformId(2L)
                        .setEventApiTimestamp(1235)
                        .setRequestId("req1")
                        .setInsertionId("ins2")
                        .setContentId("content2")
                        .setPosition(2L)
                        .build());
        verifyNoMoreInteractions(mockOut);
    }
}
