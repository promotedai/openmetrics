package ai.promoted.metrics.logprocessor.job.raw;

import ai.promoted.metrics.common.DeliveryLogIds;
import ai.promoted.proto.common.Browser;
import ai.promoted.proto.common.Device;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.ExecutionServer;
import ai.promoted.proto.delivery.Request;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ToDeliveryLogIdsTest {

    @Test
    public void flatMap() throws Exception {
        assertEquals(
                DeliveryLogIds.newBuilder()
                        .setPlatformId(2L)
                        .setEventApiTimestamp(1235)
                        .setLogUserId("logUserId1")
                        .setRequestId("req1")
                        .setViewId("view1")
                        .setClientRequestId("clientreq1")
                        .setSearchQuery("query1")
                        .setUserAgent("userAgent1")
                        .build(),
                new ToDeliveryLogIds().map(DeliveryLog.newBuilder()
                        .setPlatformId(2L)
                        .setRequest(Request.newBuilder()
                                .setPlatformId(2L)
                                .setTiming(Timing.newBuilder()
                                        .setEventApiTimestamp(1235))
                                .setUserInfo(UserInfo.newBuilder()
                                        .setLogUserId("logUserId1")
                                        .setUserId("userId1"))
                                .setRequestId("req1")
                                .setViewId("view1")
                                .setClientRequestId("clientreq1")
                                .setSearchQuery("query1")
                                .setDevice(Device.newBuilder()
                                        .setBrowser(Browser.newBuilder()
                                                .setUserAgent("userAgent1"))))
                        .build()));
    }

    @Test
    public void catchNewEnumValues() {
        for (ExecutionServer server : ExecutionServer.values()) {
            assertTrue(ToDeliveryLogIds.protoToAvroServer.containsKey(server),
                    () -> "protoToAvroServer needs to include ExecutionServer." + server);
        }
    }
}
