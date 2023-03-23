package ai.promoted.metrics.logprocessor.job.raw;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.promoted.metrics.common.LogUserUser;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.event.CohortMembership;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.junit.jupiter.api.Test;

public class RawKeysTest {
  @Test
  public void logUserUser() throws Exception {
    assertEquals(
        Tuple4.of(2L, 1659600000L, "userId1", "logUserId1"),
        RawKeys.logUserUserKeySelector.getKey(
            LogUserUser.newBuilder()
                .setPlatformId(2L)
                .setEventApiTimestamp(1663099208L)
                .setLogUserId("logUserId1")
                .setUserId("userId1")
                .build()));
  }

  @Test
  public void deliveryLog() throws Exception {
    assertEquals(
        Tuple2.of(2L, "req1"),
        RawKeys.deliveryLogKeySelector.getKey(
            DeliveryLog.newBuilder()
                .setPlatformId(2L)
                .setRequest(Request.newBuilder().setRequestId("req1"))
                .build()));
  }

  @Test
  public void cohortMembership() throws Exception {
    assertEquals(
        Tuple2.of(2L, "cm1"),
        RawKeys.cohortMembershipKeySelector.getKey(
            CohortMembership.newBuilder().setPlatformId(2L).setMembershipId("cm1").build()));
  }
}
