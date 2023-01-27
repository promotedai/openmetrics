package ai.promoted.metrics.logprocessor.job.raw;

import ai.promoted.metrics.common.RequestInsertionIds;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.delivery.Request;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class ToRequestInsertionIdsTest {
    protected Collector<RequestInsertionIds> mockOut;
    protected ToRequestInsertionIds operator;

    @BeforeEach
    public void setUp() {
        mockOut = Mockito.mock(Collector.class);
        operator = new ToRequestInsertionIds();
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
                                .setRequestId("req1")
                                .addInsertion(Insertion.newBuilder()
                                        .setContentId("content0")
                                        .setRetrievalRank(0))
                                .addInsertion(Insertion.newBuilder()
                                        .setContentId("content1")
                                        .setRetrievalRank(1))
                                .addInsertion(Insertion.newBuilder()
                                        .setContentId("content2")
                                        .setRetrievalRank(2)))
                        .build(),
                mockOut);
        verify(mockOut).collect(
                RequestInsertionIds.newBuilder()
                        .setPlatformId(2L)
                        .setEventApiTimestamp(1235)
                        .setRequestId("req1")
                        .setContentId("content0")
                        .setRetrievalRank(0L)
                        .build());
        verify(mockOut).collect(
                RequestInsertionIds.newBuilder()
                        .setPlatformId(2L)
                        .setEventApiTimestamp(1235)
                        .setRequestId("req1")
                        .setContentId("content1")
                        .setRetrievalRank(1L)
                        .build());
        verify(mockOut).collect(
                RequestInsertionIds.newBuilder()
                        .setPlatformId(2L)
                        .setEventApiTimestamp(1235)
                        .setRequestId("req1")
                        .setContentId("content2")
                        .setRetrievalRank(2L)
                        .build());
        verifyNoMoreInteractions(mockOut);
    }

    @Test
    public void flatMap_noRetrievalRank() throws Exception {
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
                                .setRequestId("req1")
                                .addInsertion(Insertion.newBuilder()
                                        .setContentId("content0"))
                                .addInsertion(Insertion.newBuilder()
                                        .setContentId("content1"))
                                .addInsertion(Insertion.newBuilder()
                                        .setContentId("content2")))
                        .build(),
                mockOut);
        verify(mockOut).collect(
                RequestInsertionIds.newBuilder()
                        .setPlatformId(2L)
                        .setEventApiTimestamp(1235)
                        .setRequestId("req1")
                        .setContentId("content0")
                        .setRetrievalRank(null)
                        .build());
        verify(mockOut).collect(
                RequestInsertionIds.newBuilder()
                        .setPlatformId(2L)
                        .setEventApiTimestamp(1235)
                        .setRequestId("req1")
                        .setContentId("content1")
                        .setRetrievalRank(null)
                        .build());
        verify(mockOut).collect(
                RequestInsertionIds.newBuilder()
                        .setPlatformId(2L)
                        .setEventApiTimestamp(1235)
                        .setRequestId("req1")
                        .setContentId("content2")
                        .setRetrievalRank(null)
                        .build());
        verifyNoMoreInteractions(mockOut);
    }
}
