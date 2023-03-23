package ai.promoted.metrics.logprocessor.common.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.AutoView;
import ai.promoted.proto.event.CombinedDeliveryLog;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.TinyEvent;
import ai.promoted.proto.event.UnionEvent;
import ai.promoted.proto.event.View;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.Test;

public class KeyUtilTest {
  @Test
  public void impression() throws Exception {
    assertEquals(
        Tuple2.of(2L, "i1"),
        KeyUtil.impressionKeySelector.getKey(
            Impression.newBuilder().setPlatformId(2L).setImpressionId("i1").build()));
  }

  @Test
  public void action() throws Exception {
    assertEquals(
        Tuple2.of(2L, "a1"),
        KeyUtil.actionKeySelector.getKey(
            Action.newBuilder().setPlatformId(2L).setActionId("a1").build()));
  }

  @Test
  public void view() throws Exception {
    assertEquals(
        Tuple2.of(2L, "v1"),
        KeyUtil.viewKeySelector.getKey(
            View.newBuilder().setPlatformId(2L).setViewId("v1").build()));
  }

  @Test
  public void autoView() throws Exception {
    assertEquals(
        Tuple2.of(2L, "av1"),
        KeyUtil.autoViewKeySelector.getKey(
            AutoView.newBuilder().setPlatformId(2L).setAutoViewId("av1").build()));
  }

  @Test
  public void tinyEventContentIdKey() throws Exception {
    assertEquals(
        Tuple2.of(1L, "content1"),
        KeyUtil.TinyEventContentIdKey.getKey(
            TinyEvent.newBuilder().setPlatformId(1L).setContentId("content1").build()));
  }

  @Test
  public void unionEntityKeySelector_view() throws Exception {
    assertEquals(
        Tuple2.of(1L, "logUserId"),
        KeyUtil.unionEntityKeySelector.getKey(
            UnionEvent.newBuilder()
                .setView(
                    View.newBuilder().setPlatformId(1L).setUserInfo(createUserInfo("logUserId")))
                .build()));
  }

  @Test
  public void unionEntityKeySelector_combinedDeliveryLog() throws Exception {
    assertEquals(
        Tuple2.of(1L, "logUserId"),
        KeyUtil.unionEntityKeySelector.getKey(
            UnionEvent.newBuilder()
                .setCombinedDeliveryLog(
                    CombinedDeliveryLog.newBuilder()
                        .setSdk(
                            DeliveryLog.newBuilder()
                                .setPlatformId(1L)
                                .setRequest(
                                    Request.newBuilder().setUserInfo(createUserInfo("logUserId")))))
                .build()));
  }

  @Test
  public void unionEntityKeySelector_impression() throws Exception {
    assertEquals(
        Tuple2.of(1L, "logUserId"),
        KeyUtil.unionEntityKeySelector.getKey(
            UnionEvent.newBuilder()
                .setImpression(
                    Impression.newBuilder()
                        .setPlatformId(1L)
                        .setUserInfo(createUserInfo("logUserId")))
                .build()));
  }

  @Test
  public void unionEntityKeySelector_action() throws Exception {
    assertEquals(
        Tuple2.of(1L, "logUserId"),
        KeyUtil.unionEntityKeySelector.getKey(
            UnionEvent.newBuilder()
                .setAction(
                    Action.newBuilder().setPlatformId(1L).setUserInfo(createUserInfo("logUserId")))
                .build()));
  }

  private UserInfo createUserInfo(String logUserId) {
    return UserInfo.newBuilder().setLogUserId(logUserId).build();
  }
}
