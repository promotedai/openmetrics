package ai.promoted.metrics.logprocessor.job.join;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.promoted.proto.common.Properties;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.delivery.Response;
import ai.promoted.proto.event.CombinedDeliveryLog;
import ai.promoted.proto.event.TinyCommonInfo;
import ai.promoted.proto.event.TinyDeliveryLog;
import ai.promoted.proto.event.TinyInsertionCore;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Test;

public class ToTinyDeliveryLogUnitTest {

  @Test
  public void map() {
    DeliveryLog deliveryLog =
        DeliveryLog.newBuilder()
            .setPlatformId(1L)
            .setRequest(
                Request.newBuilder()
                    .setPlatformId(1L)
                    .setUserInfo(UserInfo.newBuilder().setAnonUserId("anonUserId1").build())
                    .setTiming(Timing.newBuilder().setEventApiTimestamp(1))
                    .setRequestId("req1")
                    .setViewId("view1"))
            .setResponse(
                Response.newBuilder()
                    .addInsertion(
                        Insertion.newBuilder().setInsertionId("ins1").setContentId("content1")))
            .build();
    CombinedDeliveryLog combinedDeliveryLog =
        CombinedDeliveryLog.newBuilder().setApi(deliveryLog).build();
    assertEquals(
        TinyDeliveryLog.newBuilder()
            .setCommon(
                TinyCommonInfo.newBuilder()
                    .setPlatformId(1L)
                    .setAnonUserId("anonUserId1")
                    .setEventApiTimestamp(1L))
            .setRequestId("req1")
            .setViewId("view1")
            .addResponseInsertion(
                TinyInsertionCore.newBuilder().setInsertionId("ins1").setContentId("content1"))
            .build(),
        new ToTinyDeliveryLog(ImmutableList.of()).map(combinedDeliveryLog));
  }

  @Test
  public void mapEmpty() {
    DeliveryLog deliveryLog = DeliveryLog.getDefaultInstance();
    CombinedDeliveryLog combinedDeliveryLog =
        CombinedDeliveryLog.newBuilder().setApi(deliveryLog).build();
    assertEquals(
        TinyDeliveryLog.newBuilder().setCommon(TinyCommonInfo.getDefaultInstance()).build(),
        new ToTinyDeliveryLog(ImmutableList.of()).map(combinedDeliveryLog));
  }

  @Test
  public void map_insertionProperties() {
    DeliveryLog deliveryLog =
        DeliveryLog.newBuilder()
            .setPlatformId(1L)
            .setRequest(
                Request.newBuilder()
                    .setPlatformId(1L)
                    .setUserInfo(UserInfo.newBuilder().setAnonUserId("anonUserId1").build())
                    .setTiming(Timing.newBuilder().setEventApiTimestamp(1))
                    .setRequestId("req1")
                    .setViewId("view1")
                    .addInsertion(
                        Insertion.newBuilder()
                            .setContentId("content1")
                            .setProperties(
                                Properties.newBuilder()
                                    .setStruct(
                                        Struct.newBuilder()
                                            .putFields(
                                                "store",
                                                Value.newBuilder()
                                                    .setStringValue("store1")
                                                    .build()))))
                    // content2 tests if we do not have a Request Insertion.
                    .addInsertion(
                        Insertion.newBuilder()
                            .setContentId("content3")
                            .setProperties(
                                Properties.newBuilder()
                                    .setStruct(
                                        Struct.newBuilder()
                                            .putFields(
                                                "owner",
                                                Value.newBuilder().setStringValue("owner1").build())
                                            .putFields(
                                                "notKeyed",
                                                Value.newBuilder()
                                                    .setStringValue("category2")
                                                    .build()))))
                    .addInsertion(
                        Insertion.newBuilder()
                            .setContentId("content4")
                            .setProperties(
                                Properties.newBuilder()
                                    .setStruct(
                                        Struct.newBuilder()
                                            .putFields(
                                                "store",
                                                Value.newBuilder().setStringValue("store2").build())
                                            .putFields(
                                                "owner",
                                                Value.newBuilder()
                                                    .setStringValue("owner2")
                                                    .build())))))
            .setResponse(
                Response.newBuilder()
                    .addInsertion(
                        Insertion.newBuilder().setInsertionId("ins1").setContentId("content1"))
                    .addInsertion(
                        Insertion.newBuilder().setInsertionId("ins2").setContentId("content2"))
                    .addInsertion(
                        Insertion.newBuilder().setInsertionId("ins3").setContentId("content3"))
                    .addInsertion(
                        Insertion.newBuilder().setInsertionId("ins4").setContentId("content4")))
            .build();
    CombinedDeliveryLog combinedDeliveryLog =
        CombinedDeliveryLog.newBuilder().setApi(deliveryLog).build();
    assertEquals(
        TinyDeliveryLog.newBuilder()
            .setCommon(
                TinyCommonInfo.newBuilder()
                    .setPlatformId(1L)
                    .setAnonUserId("anonUserId1")
                    .setEventApiTimestamp(1L))
            .setRequestId("req1")
            .setViewId("view1")
            .addResponseInsertion(
                TinyInsertionCore.newBuilder()
                    .setInsertionId("ins1")
                    .setContentId("content1")
                    .putOtherContentIds(109770977, "store1"))
            .addResponseInsertion(
                TinyInsertionCore.newBuilder().setInsertionId("ins2").setContentId("content2"))
            .addResponseInsertion(
                TinyInsertionCore.newBuilder()
                    .setInsertionId("ins3")
                    .setContentId("content3")
                    .putOtherContentIds(106164915, "owner1"))
            .addResponseInsertion(
                TinyInsertionCore.newBuilder()
                    .setInsertionId("ins4")
                    .setContentId("content4")
                    .putOtherContentIds(106164915, "owner2")
                    .putOtherContentIds(109770977, "store2"))
            .build(),
        new ToTinyDeliveryLog(ImmutableList.of("store", "owner")).map(combinedDeliveryLog));
  }

  @Test
  public void map_requestProperties() {
    DeliveryLog deliveryLog =
        DeliveryLog.newBuilder()
            .setPlatformId(1L)
            .setRequest(
                Request.newBuilder()
                    .setPlatformId(1L)
                    .setUserInfo(UserInfo.newBuilder().setAnonUserId("anonUserId1").build())
                    .setTiming(Timing.newBuilder().setEventApiTimestamp(1))
                    .setRequestId("req1")
                    .setViewId("view1")
                    .setProperties(
                        Properties.newBuilder()
                            .setStruct(
                                Struct.newBuilder()
                                    .putFields(
                                        "store",
                                        Value.newBuilder().setStringValue("store1").build()))))
            .setResponse(
                Response.newBuilder()
                    .addInsertion(
                        Insertion.newBuilder().setInsertionId("ins1").setContentId("content1")))
            .build();
    CombinedDeliveryLog combinedDeliveryLog =
        CombinedDeliveryLog.newBuilder().setApi(deliveryLog).build();
    assertEquals(
        TinyDeliveryLog.newBuilder()
            .setCommon(
                TinyCommonInfo.newBuilder()
                    .setPlatformId(1L)
                    .setAnonUserId("anonUserId1")
                    .setEventApiTimestamp(1L))
            .setRequestId("req1")
            .setViewId("view1")
            .addResponseInsertion(
                TinyInsertionCore.newBuilder()
                    .setInsertionId("ins1")
                    .setContentId("content1")
                    .putOtherContentIds(109770977, "store1"))
            .build(),
        new ToTinyDeliveryLog(ImmutableList.of("store", "owner")).map(combinedDeliveryLog));
  }

  @Test
  public void map_requestAndInsertionProperties() {
    DeliveryLog deliveryLog =
        DeliveryLog.newBuilder()
            .setPlatformId(1L)
            .setRequest(
                Request.newBuilder()
                    .setPlatformId(1L)
                    .setUserInfo(UserInfo.newBuilder().setAnonUserId("anonUserId1").build())
                    .setTiming(Timing.newBuilder().setEventApiTimestamp(1))
                    .setRequestId("req1")
                    .setViewId("view1")
                    .setProperties(
                        Properties.newBuilder()
                            .setStruct(
                                Struct.newBuilder()
                                    .putFields(
                                        "store",
                                        Value.newBuilder().setStringValue("store1").build())))
                    .addInsertion(
                        Insertion.newBuilder()
                            .setContentId("content1")
                            .setProperties(
                                Properties.newBuilder()
                                    .setStruct(
                                        Struct.newBuilder()
                                            .putFields(
                                                "store",
                                                Value.newBuilder()
                                                    .setStringValue("store2")
                                                    .build())))))
            .setResponse(
                Response.newBuilder()
                    .addInsertion(
                        Insertion.newBuilder().setInsertionId("ins1").setContentId("content1")))
            .build();
    CombinedDeliveryLog combinedDeliveryLog =
        CombinedDeliveryLog.newBuilder().setApi(deliveryLog).build();
    assertEquals(
        TinyDeliveryLog.newBuilder()
            .setCommon(
                TinyCommonInfo.newBuilder()
                    .setPlatformId(1L)
                    .setAnonUserId("anonUserId1")
                    .setEventApiTimestamp(1L))
            .setRequestId("req1")
            .setViewId("view1")
            .addResponseInsertion(
                TinyInsertionCore.newBuilder()
                    .setInsertionId("ins1")
                    .setContentId("content1")
                    .putOtherContentIds(109770977, "store2"))
            .build(),
        new ToTinyDeliveryLog(ImmutableList.of("store", "owner")).map(combinedDeliveryLog));
  }
}
