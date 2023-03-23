package ai.promoted.metrics.logprocessor.job.join;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.promoted.proto.common.Properties;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.TinyEvent;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Test;

public class ToTinyImpressionUnitTest {

  @Test
  public void map() {
    Impression impression =
        Impression.newBuilder()
            .setPlatformId(1L)
            .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1").build())
            .setTiming(Timing.newBuilder().setLogTimestamp(1))
            .setRequestId("req1")
            .setViewId("view1")
            .setInsertionId("ins1")
            .setContentId("content1")
            .setImpressionId("imp1")
            .build();
    assertEquals(
        TinyEvent.newBuilder()
            .setPlatformId(1L)
            .setLogUserId("logUserId1")
            .setLogTimestamp(1L)
            .setViewId("view1")
            .setRequestId("req1")
            .setInsertionId("ins1")
            .setContentId("content1")
            .setImpressionId("imp1")
            .build(),
        new ToTinyImpression(ImmutableList.of()).map(impression));
  }

  @Test
  public void map_withOtherContentIds() {
    Impression impression =
        Impression.newBuilder()
            .setPlatformId(1L)
            .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1").build())
            .setTiming(Timing.newBuilder().setLogTimestamp(1))
            .setRequestId("req1")
            .setViewId("view1")
            .setInsertionId("ins1")
            .setContentId("content1")
            .setImpressionId("imp1")
            .setProperties(
                Properties.newBuilder()
                    .setStruct(
                        Struct.newBuilder()
                            .putFields(
                                "store", Value.newBuilder().setStringValue("store1").build())))
            .build();
    assertEquals(
        TinyEvent.newBuilder()
            .setPlatformId(1L)
            .setLogUserId("logUserId1")
            .setLogTimestamp(1L)
            .setViewId("view1")
            .setRequestId("req1")
            .setInsertionId("ins1")
            .setContentId("content1")
            .setImpressionId("imp1")
            .putOtherContentIds(109770977, "store1")
            .build(),
        new ToTinyImpression(ImmutableList.of("store")).map(impression));
  }
}
