package ai.promoted.metrics.logprocessor.job.join;

import ai.promoted.proto.common.Timing;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.Cart;
import ai.promoted.proto.event.CartContent;
import ai.promoted.proto.event.TinyAction;
import ai.promoted.proto.event.TinyCommonInfo;
import com.google.common.collect.ImmutableList;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ToTinyActionUnitTest {

  @Test
  public void flatMap_notPurchase() throws Exception {
    Action action =
        Action.newBuilder()
            .setPlatformId(1L)
            .setUserInfo(UserInfo.newBuilder().setAnonUserId("anonUserId1").build())
            .setTiming(Timing.newBuilder().setEventApiTimestamp(1))
            .setRequestId("req1")
            .setViewId("view1")
            .setInsertionId("ins1")
            .setContentId("content1")
            .setImpressionId("imp1")
            .setActionId("act1")
            .setActionType(ActionType.NAVIGATE)
            .build();
    Collector<TinyAction> out = Mockito.mock(Collector.class);
    new ToTinyAction(ImmutableList.of()).flatMap(action, out);
    Mockito.verify(out)
        .collect(
            TinyAction.newBuilder()
                .setCommon(
                    TinyCommonInfo.newBuilder()
                        .setPlatformId(1L)
                        .setAnonUserId("anonUserId1")
                        .setEventApiTimestamp(1L))
                .setViewId("view1")
                .setRequestId("req1")
                .setInsertionId("ins1")
                .setImpressionId("imp1")
                .setActionId("act1")
                .setActionType(ActionType.NAVIGATE)
                .setContentId("content1")
                .build());
    Mockito.verifyNoMoreInteractions(out);
  }

  @Test
  public void flatMap_purchase_noCart() throws Exception {
    Action action =
        Action.newBuilder()
            .setPlatformId(1L)
            .setUserInfo(UserInfo.newBuilder().setAnonUserId("anonUserId1").build())
            .setTiming(Timing.newBuilder().setEventApiTimestamp(1))
            .setRequestId("req1")
            .setViewId("view1")
            .setInsertionId("ins1")
            .setContentId("content1")
            .setImpressionId("imp1")
            .setActionId("act1")
            .setActionType(ActionType.PURCHASE)
            .build();
    Collector<TinyAction> out = Mockito.mock(Collector.class);
    new ToTinyAction(ImmutableList.of()).flatMap(action, out);
    Mockito.verify(out)
        .collect(
            TinyAction.newBuilder()
                .setCommon(
                    TinyCommonInfo.newBuilder()
                        .setPlatformId(1L)
                        .setAnonUserId("anonUserId1")
                        .setEventApiTimestamp(1L))
                .setViewId("view1")
                .setRequestId("req1")
                .setInsertionId("ins1")
                .setContentId("content1")
                .setImpressionId("imp1")
                .setActionId("act1")
                .setActionType(ActionType.PURCHASE)
                .build());
    Mockito.verifyNoMoreInteractions(out);
  }

  @Test
  public void flatMap_purchase_cart_oneItems() throws Exception {
    Action action =
        Action.newBuilder()
            .setPlatformId(1L)
            .setUserInfo(UserInfo.newBuilder().setAnonUserId("anonUserId1").build())
            .setTiming(Timing.newBuilder().setEventApiTimestamp(1))
            .setRequestId("req1")
            .setViewId("view1")
            .setInsertionId("ins1")
            .setImpressionId("imp1")
            .setActionId("act1")
            .setActionType(ActionType.PURCHASE)
            .setCart(
                Cart.newBuilder().addContents(CartContent.newBuilder().setContentId("content1")))
            .build();
    Collector<TinyAction> out = Mockito.mock(Collector.class);
    new ToTinyAction(ImmutableList.of()).flatMap(action, out);
    Mockito.verify(out)
        .collect(
            TinyAction.newBuilder()
                .setCommon(
                    TinyCommonInfo.newBuilder()
                        .setPlatformId(1L)
                        .setAnonUserId("anonUserId1")
                        .setEventApiTimestamp(1L))
                .setContentId("content1")
                .setActionId("act1")
                .setActionType(ActionType.PURCHASE)
                .build());
    Mockito.verifyNoMoreInteractions(out);
  }

  @Test
  public void flatMap_purchase_cart_twoItems() throws Exception {
    Action action =
        Action.newBuilder()
            .setPlatformId(1L)
            .setUserInfo(UserInfo.newBuilder().setAnonUserId("anonUserId1").build())
            .setTiming(Timing.newBuilder().setEventApiTimestamp(1))
            .setRequestId("req1")
            .setViewId("view1")
            .setInsertionId("ins1")
            .setImpressionId("imp1")
            .setActionId("act1")
            .setActionType(ActionType.PURCHASE)
            .setCart(
                Cart.newBuilder()
                    .addContents(CartContent.newBuilder().setContentId("content1"))
                    .addContents(CartContent.newBuilder().setContentId("content2")))
            .build();
    Collector<TinyAction> out = Mockito.mock(Collector.class);
    new ToTinyAction(ImmutableList.of()).flatMap(action, out);
    Mockito.verify(out)
        .collect(
            TinyAction.newBuilder()
                .setCommon(
                    TinyCommonInfo.newBuilder()
                        .setPlatformId(1L)
                        .setAnonUserId("anonUserId1")
                        .setEventApiTimestamp(1L))
                .setContentId("content1")
                .setActionId("act1")
                .setActionType(ActionType.PURCHASE)
                .build());
    Mockito.verify(out)
        .collect(
            TinyAction.newBuilder()
                .setCommon(
                    TinyCommonInfo.newBuilder()
                        .setPlatformId(1L)
                        .setAnonUserId("anonUserId1")
                        .setEventApiTimestamp(1L))
                .setContentId("content2")
                .setActionId("act1")
                .setActionType(ActionType.PURCHASE)
                .build());
    Mockito.verifyNoMoreInteractions(out);
  }
}
