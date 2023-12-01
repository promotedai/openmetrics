package ai.promoted.metrics.logprocessor.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.promoted.proto.event.TinyAction;
import ai.promoted.proto.event.TinyCommonInfo;
import ai.promoted.proto.event.TinyDeliveryLog;
import ai.promoted.proto.event.TinyInsertion;
import ai.promoted.proto.event.TinyInsertionCore;
import ai.promoted.proto.event.TinyJoinedImpression;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import org.junit.jupiter.api.Test;

public class TinyFlatUtilTest {
  @Test
  public void createTinyFlatResponseInsertions_withoutTimestamps() {
    List<TinyInsertion.Builder> builders =
        TinyFlatUtil.createTinyFlatResponseInsertions(
            TinyInsertion.newBuilder()
                .setCommon(TinyCommonInfo.newBuilder().setPlatformId(1L).setAnonUserId("anonUser1"))
                .setViewId("view1"),
            TinyDeliveryLog.newBuilder()
                .setViewId("view1")
                .setRequestId("request1")
                .addResponseInsertion(
                    TinyInsertionCore.newBuilder()
                        .setInsertionId("insertion1")
                        .setContentId("content1"))
                .addResponseInsertion(
                    TinyInsertionCore.newBuilder()
                        .setInsertionId("insertion2")
                        .setContentId("content2"))
                .build());

    assertEquals(2, builders.size());
    assertEquals(
        TinyInsertion.newBuilder()
            .setCommon(TinyCommonInfo.newBuilder().setPlatformId(1L).setAnonUserId("anonUser1"))
            .setViewId("view1")
            .setRequestId("request1")
            .setCore(
                TinyInsertionCore.newBuilder()
                    .setInsertionId("insertion1")
                    .setContentId("content1"))
            .build(),
        builders.get(0).build());
    assertEquals(
        TinyInsertion.newBuilder()
            .setCommon(TinyCommonInfo.newBuilder().setPlatformId(1L).setAnonUserId("anonUser1"))
            .setViewId("view1")
            .setRequestId("request1")
            .setCore(
                TinyInsertionCore.newBuilder()
                    .setInsertionId("insertion2")
                    .setContentId("content2"))
            .build(),
        builders.get(1).build());
  }

  @Test
  public void createTinyFlatResponseInsertions_withoutTimestamps_noLhs() {
    List<TinyInsertion.Builder> builders =
        TinyFlatUtil.createTinyFlatResponseInsertions(
            TinyInsertion.newBuilder(),
            TinyDeliveryLog.newBuilder()
                .setCommon(TinyCommonInfo.newBuilder().setPlatformId(1L).setAnonUserId("anonUser1"))
                .setViewId("view1")
                .setRequestId("request1")
                .addResponseInsertion(
                    TinyInsertionCore.newBuilder()
                        .setInsertionId("insertion1")
                        .setContentId("content1"))
                .addResponseInsertion(
                    TinyInsertionCore.newBuilder()
                        .setInsertionId("insertion2")
                        .setContentId("content2"))
                .build());

    assertEquals(2, builders.size());
    assertEquals(
        TinyInsertion.newBuilder()
            .setCommon(TinyCommonInfo.newBuilder().setPlatformId(1L).setAnonUserId("anonUser1"))
            .setViewId("view1")
            .setRequestId("request1")
            .setCore(
                TinyInsertionCore.newBuilder()
                    .setInsertionId("insertion1")
                    .setContentId("content1"))
            .build(),
        builders.get(0).build());
    assertEquals(
        TinyInsertion.newBuilder()
            .setCommon(TinyCommonInfo.newBuilder().setPlatformId(1L).setAnonUserId("anonUser1"))
            .setViewId("view1")
            .setRequestId("request1")
            .setCore(
                TinyInsertionCore.newBuilder()
                    .setInsertionId("insertion2")
                    .setContentId("content2"))
            .build(),
        builders.get(1).build());
  }

  @Test
  public void createTinyFlatResponseInsertions_withTimestamps() {
    List<TinyInsertion.Builder> builders =
        TinyFlatUtil.createTinyFlatResponseInsertions(
            TinyInsertion.newBuilder()
                .setCommon(
                    TinyCommonInfo.newBuilder()
                        .setPlatformId(1L)
                        .setAnonUserId("anonUser1")
                        .setEventApiTimestamp(1L))
                .setViewId("view1"),
            TinyDeliveryLog.newBuilder()
                .setCommon(
                    TinyCommonInfo.newBuilder()
                        .setPlatformId(1L)
                        .setAnonUserId("anonUser1")
                        .setEventApiTimestamp(2L))
                .setViewId("view1")
                .setRequestId("request1")
                .addResponseInsertion(
                    TinyInsertionCore.newBuilder()
                        .setInsertionId("insertion1")
                        .setContentId("content1"))
                .addResponseInsertion(
                    TinyInsertionCore.newBuilder()
                        .setInsertionId("insertion2")
                        .setContentId("content2"))
                .build());

    assertEquals(2, builders.size());
    assertEquals(
        TinyInsertion.newBuilder()
            .setCommon(
                TinyCommonInfo.newBuilder()
                    .setPlatformId(1L)
                    .setAnonUserId("anonUser1")
                    .setEventApiTimestamp(2L))
            .setViewId("view1")
            .setRequestId("request1")
            .setCore(
                TinyInsertionCore.newBuilder()
                    .setInsertionId("insertion1")
                    .setContentId("content1"))
            .build(),
        builders.get(0).build());
    assertEquals(
        TinyInsertion.newBuilder()
            .setCommon(
                TinyCommonInfo.newBuilder()
                    .setPlatformId(1L)
                    .setAnonUserId("anonUser1")
                    .setEventApiTimestamp(2L))
            .setViewId("view1")
            .setRequestId("request1")
            .setCore(
                TinyInsertionCore.newBuilder()
                    .setInsertionId("insertion2")
                    .setContentId("content2"))
            .build(),
        builders.get(1).build());
  }

  @Test
  public void createTinyFlatResponseInsertions_withTimestamps_noLhs() {
    List<TinyInsertion.Builder> builders =
        TinyFlatUtil.createTinyFlatResponseInsertions(
            TinyInsertion.newBuilder(),
            TinyDeliveryLog.newBuilder()
                .setCommon(
                    TinyCommonInfo.newBuilder()
                        .setPlatformId(1L)
                        .setAnonUserId("anonUser1")
                        .setEventApiTimestamp(2L))
                .setViewId("view1")
                .setRequestId("request1")
                .addResponseInsertion(
                    TinyInsertionCore.newBuilder()
                        .setInsertionId("insertion1")
                        .setContentId("content1"))
                .addResponseInsertion(
                    TinyInsertionCore.newBuilder()
                        .setInsertionId("insertion2")
                        .setContentId("content2"))
                .build());

    assertEquals(2, builders.size());
    assertEquals(
        TinyInsertion.newBuilder()
            .setCommon(
                TinyCommonInfo.newBuilder()
                    .setPlatformId(1L)
                    .setAnonUserId("anonUser1")
                    .setEventApiTimestamp(2L))
            .setViewId("view1")
            .setRequestId("request1")
            .setCore(
                TinyInsertionCore.newBuilder()
                    .setInsertionId("insertion1")
                    .setContentId("content1"))
            .build(),
        builders.get(0).build());
    assertEquals(
        TinyInsertion.newBuilder()
            .setCommon(
                TinyCommonInfo.newBuilder()
                    .setPlatformId(1L)
                    .setAnonUserId("anonUser1")
                    .setEventApiTimestamp(2L))
            .setViewId("view1")
            .setRequestId("request1")
            .setCore(
                TinyInsertionCore.newBuilder()
                    .setInsertionId("insertion2")
                    .setContentId("content2"))
            .build(),
        builders.get(1).build());
  }

  @Test
  public void getAllContentIds_TinyInsertion() {
    assertEquals(
        ImmutableSet.of(), TinyFlatUtil.getAllContentIds(TinyInsertion.getDefaultInstance()));
    assertEquals(
        ImmutableSet.of("content1"),
        TinyFlatUtil.getAllContentIds(
            TinyInsertion.newBuilder()
                .setCore(TinyInsertionCore.newBuilder().setContentId("content1"))
                .build()));
    assertEquals(
        ImmutableSet.of("value1", "value2"),
        TinyFlatUtil.getAllContentIds(
            TinyInsertion.newBuilder()
                .setCore(
                    TinyInsertionCore.newBuilder()
                        .putOtherContentIds(StringUtil.hash("ownerId"), "value1")
                        .putOtherContentIds(StringUtil.hash("storeId"), "value2"))
                .build()));
    assertEquals(
        ImmutableSet.of("content1", "value1", "value2"),
        TinyFlatUtil.getAllContentIds(
            TinyInsertion.newBuilder()
                .setCore(
                    TinyInsertionCore.newBuilder()
                        .setContentId("content1")
                        .putOtherContentIds(StringUtil.hash("ownerId"), "value1")
                        .putOtherContentIds(StringUtil.hash("storeId"), "value2"))
                .build()));
  }

  @Test
  public void getAllContentIds_TinyJoinedImpression() {
    assertEquals(
        ImmutableSet.of("content1", "value1", "value2"),
        TinyFlatUtil.getAllContentIds(
            TinyJoinedImpression.newBuilder()
                .setInsertion(
                    TinyInsertion.newBuilder()
                        .setCore(
                            TinyInsertionCore.newBuilder()
                                .setContentId("content1")
                                .putOtherContentIds(StringUtil.hash("ownerId"), "value1")
                                .putOtherContentIds(StringUtil.hash("storeId"), "value2")))
                .build()));
  }

  @Test
  public void getAllContentIds_TinyAction() {
    assertEquals(ImmutableSet.of(), TinyFlatUtil.getAllContentIds(TinyAction.getDefaultInstance()));
    assertEquals(
        ImmutableSet.of("content1"),
        TinyFlatUtil.getAllContentIds(TinyAction.newBuilder().setContentId("content1").build()));
    assertEquals(
        ImmutableSet.of("value1", "value2"),
        TinyFlatUtil.getAllContentIds(
            TinyAction.newBuilder()
                .putOtherContentIds(StringUtil.hash("ownerId"), "value1")
                .putOtherContentIds(StringUtil.hash("storeId"), "value2")
                .build()));
    assertEquals(
        ImmutableSet.of("content1", "value1", "value2"),
        TinyFlatUtil.getAllContentIds(
            TinyAction.newBuilder()
                .setContentId("content1")
                .putOtherContentIds(StringUtil.hash("ownerId"), "value1")
                .putOtherContentIds(StringUtil.hash("storeId"), "value2")
                .build()));
  }
}
