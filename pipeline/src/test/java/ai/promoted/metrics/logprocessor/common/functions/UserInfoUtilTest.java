package ai.promoted.metrics.logprocessor.common.functions;

import static ai.promoted.metrics.logprocessor.common.functions.UserInfoUtil.clearUserId;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.AttributedAction;
import ai.promoted.proto.event.AutoView;
import ai.promoted.proto.event.CohortMembership;
import ai.promoted.proto.event.Diagnostics;
import ai.promoted.proto.event.FlatResponseInsertion;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.JoinedImpression;
import ai.promoted.proto.event.Session;
import ai.promoted.proto.event.SessionProfile;
import ai.promoted.proto.event.Touchpoint;
import ai.promoted.proto.event.User;
import ai.promoted.proto.event.View;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Test;

public class UserInfoUtilTest {

  // JoinedImpression.
  @Test
  public void clearUserId_JoinedImpression() {
    assertEquals(
        newJoinedImpression(UserInfo.newBuilder().setLogUserId("logUserId1").build()),
        clearUserId(
            newJoinedImpression(
                UserInfo.newBuilder().setUserId("userId1").setLogUserId("logUserId1").build())));
  }

  @Test
  public void clearUserId_JoinedImpression_NotSet() {
    assertEquals(
        newJoinedImpression(UserInfo.newBuilder().setLogUserId("logUserId1").build()),
        clearUserId(newJoinedImpression(UserInfo.newBuilder().setLogUserId("logUserId1").build())));
  }

  @Test
  public void clearUserId_JoinedImpression_NoUserInfo() {
    assertEquals(newJoinedImpression(null), clearUserId(newJoinedImpression(null)));
  }

  private JoinedImpression newJoinedImpression(@Nullable UserInfo userInfo) {
    Request.Builder requestBuilder = Request.newBuilder();
    if (userInfo != null) {
      requestBuilder.setUserInfo(userInfo);
    }
    return JoinedImpression.newBuilder().setRequest(requestBuilder).build();
  }

  // AttributedAction.
  @Test
  public void clearUserId_AttributedAction() {
    assertEquals(
        newAttributedAction(UserInfo.newBuilder().setLogUserId("logUserId1").build()),
        clearUserId(
            newAttributedAction(
                UserInfo.newBuilder().setUserId("userId1").setLogUserId("logUserId1").build())));
  }

  @Test
  public void clearUserId_AttributedAction_NotSet() {
    assertEquals(
        newAttributedAction(UserInfo.newBuilder().setLogUserId("logUserId1").build()),
        clearUserId(newAttributedAction(UserInfo.newBuilder().setLogUserId("logUserId1").build())));
  }

  @Test
  public void clearUserId_AttributedAction_NoUserInfo() {
    assertEquals(newAttributedAction(null), clearUserId(newAttributedAction(null)));
  }

  private AttributedAction newAttributedAction(@Nullable UserInfo userInfo) {
    Request.Builder requestBuilder = Request.newBuilder();
    if (userInfo != null) {
      requestBuilder.setUserInfo(userInfo);
    }
    return AttributedAction.newBuilder()
        .setTouchpoint(
            Touchpoint.newBuilder()
                .setJoinedImpression(JoinedImpression.newBuilder().setRequest(requestBuilder)))
        .build();
  }

  // FlatResponseInsertion.
  @Test
  public void clearUserId_FlatResponseInsertion() {
    assertEquals(
        FlatResponseInsertion.newBuilder()
            .setRequest(
                Request.newBuilder().setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1")))
            .build(),
        clearUserId(
            FlatResponseInsertion.newBuilder()
                .setRequest(
                    Request.newBuilder()
                        .setUserInfo(
                            UserInfo.newBuilder().setUserId("userId1").setLogUserId("logUserId1")))
                .build()));
  }

  @Test
  public void clearUserId_FlatResponseInsertion_NotSet() {
    assertEquals(
        FlatResponseInsertion.newBuilder()
            .setRequest(
                Request.newBuilder().setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1")))
            .build(),
        clearUserId(
            FlatResponseInsertion.newBuilder()
                .setRequest(
                    Request.newBuilder()
                        .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1")))
                .build()));
  }

  @Test
  public void clearUserId_FlatResponseInsertion_NoUserInfo() {
    assertEquals(
        FlatResponseInsertion.newBuilder().setRequest(Request.getDefaultInstance()).build(),
        clearUserId(
            FlatResponseInsertion.newBuilder().setRequest(Request.getDefaultInstance()).build()));
  }

  // User.
  @Test
  public void clearUserId_User() {
    assertEquals(
        User.newBuilder().setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1")).build(),
        clearUserId(
            User.newBuilder()
                .setUserInfo(UserInfo.newBuilder().setUserId("userId1").setLogUserId("logUserId1"))
                .build()));
  }

  @Test
  public void clearUserId_User_NotSet() {
    assertEquals(
        User.newBuilder().setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1")).build(),
        clearUserId(
            User.newBuilder()
                .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1"))
                .build()));
  }

  @Test
  public void clearUserId_User_NoUserInfo() {
    assertEquals(User.getDefaultInstance(), clearUserId(User.getDefaultInstance()));
  }

  // SessionProfile.
  @Test
  public void clearUserId_SessionProfile() {
    assertEquals(
        SessionProfile.newBuilder()
            .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1"))
            .build(),
        clearUserId(
            SessionProfile.newBuilder()
                .setUserInfo(UserInfo.newBuilder().setUserId("userId1").setLogUserId("logUserId1"))
                .build()));
  }

  @Test
  public void clearUserId_SessionProfile_NotSet() {
    assertEquals(
        SessionProfile.newBuilder()
            .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1"))
            .build(),
        clearUserId(
            SessionProfile.newBuilder()
                .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1"))
                .build()));
  }

  @Test
  public void clearUserId_SessionProfile_NoUserInfo() {
    assertEquals(
        SessionProfile.getDefaultInstance(), clearUserId(SessionProfile.getDefaultInstance()));
  }

  // Session.
  @Test
  public void clearUserId_Session() {
    assertEquals(
        Session.newBuilder().setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1")).build(),
        clearUserId(
            Session.newBuilder()
                .setUserInfo(UserInfo.newBuilder().setUserId("userId1").setLogUserId("logUserId1"))
                .build()));
  }

  @Test
  public void clearUserId_Session_NotSet() {
    assertEquals(
        Session.newBuilder().setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1")).build(),
        clearUserId(
            Session.newBuilder()
                .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1"))
                .build()));
  }

  @Test
  public void clearUserId_Session_NoUserInfo() {
    assertEquals(Session.getDefaultInstance(), clearUserId(Session.getDefaultInstance()));
  }

  // View.
  @Test
  public void clearUserId_View() {
    assertEquals(
        View.newBuilder().setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1")).build(),
        clearUserId(
            View.newBuilder()
                .setUserInfo(UserInfo.newBuilder().setUserId("userId1").setLogUserId("logUserId1"))
                .build()));
  }

  @Test
  public void clearUserId_View_NotSet() {
    assertEquals(
        View.newBuilder().setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1")).build(),
        clearUserId(
            View.newBuilder()
                .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1"))
                .build()));
  }

  @Test
  public void clearUserId_View_NoUserInfo() {
    assertEquals(View.getDefaultInstance(), clearUserId(View.getDefaultInstance()));
  }

  // AutoView.
  @Test
  public void clearUserId_AutoView() {
    assertEquals(
        AutoView.newBuilder().setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1")).build(),
        clearUserId(
            AutoView.newBuilder()
                .setUserInfo(UserInfo.newBuilder().setUserId("userId1").setLogUserId("logUserId1"))
                .build()));
  }

  @Test
  public void clearUserId_AutoView_NotSet() {
    assertEquals(
        AutoView.newBuilder().setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1")).build(),
        clearUserId(
            AutoView.newBuilder()
                .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1"))
                .build()));
  }

  @Test
  public void clearUserId_AutoView_NoUserInfo() {
    assertEquals(AutoView.getDefaultInstance(), clearUserId(AutoView.getDefaultInstance()));
  }

  // DeliveryLog.
  @Test
  public void clearUserId_DeliveryLog() {
    assertEquals(
        DeliveryLog.newBuilder()
            .setRequest(
                Request.newBuilder().setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1")))
            .build(),
        clearUserId(
            DeliveryLog.newBuilder()
                .setRequest(
                    Request.newBuilder()
                        .setUserInfo(
                            UserInfo.newBuilder().setUserId("userId1").setLogUserId("logUserId1")))
                .build()));
  }

  @Test
  public void clearUserId_DeliveryLog_NotSet() {
    assertEquals(
        DeliveryLog.newBuilder()
            .setRequest(
                Request.newBuilder().setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1")))
            .build(),
        clearUserId(
            DeliveryLog.newBuilder()
                .setRequest(
                    Request.newBuilder()
                        .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1")))
                .build()));
  }

  @Test
  public void clearUserId_DeliveryLog_NoUserInfo() {
    assertEquals(
        DeliveryLog.newBuilder().setRequest(Request.getDefaultInstance()).build(),
        clearUserId(DeliveryLog.newBuilder().setRequest(Request.getDefaultInstance()).build()));
  }

  // Impression.
  @Test
  public void clearUserId_Impression() {
    assertEquals(
        Impression.newBuilder()
            .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1"))
            .build(),
        clearUserId(
            Impression.newBuilder()
                .setUserInfo(UserInfo.newBuilder().setUserId("userId1").setLogUserId("logUserId1"))
                .build()));
  }

  @Test
  public void clearUserId_Impression_NotSet() {
    assertEquals(
        Impression.newBuilder()
            .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1"))
            .build(),
        clearUserId(
            Impression.newBuilder()
                .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1"))
                .build()));
  }

  @Test
  public void clearUserId_Impression_NoUserInfo() {
    assertEquals(Impression.getDefaultInstance(), clearUserId(Impression.getDefaultInstance()));
  }

  // Action.
  @Test
  public void clearUserId_Action() {
    assertEquals(
        Action.newBuilder().setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1")).build(),
        clearUserId(
            Action.newBuilder()
                .setUserInfo(UserInfo.newBuilder().setUserId("userId1").setLogUserId("logUserId1"))
                .build()));
  }

  @Test
  public void clearUserId_Action_NotSet() {
    assertEquals(
        Action.newBuilder().setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1")).build(),
        clearUserId(
            Action.newBuilder()
                .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1"))
                .build()));
  }

  @Test
  public void clearUserId_Action_NoUserInfo() {
    assertEquals(Action.getDefaultInstance(), clearUserId(Action.getDefaultInstance()));
  }

  // CohortMembership.
  @Test
  public void clearUserId_CohortMembership() {
    assertEquals(
        CohortMembership.newBuilder()
            .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1"))
            .build(),
        clearUserId(
            CohortMembership.newBuilder()
                .setUserInfo(UserInfo.newBuilder().setUserId("userId1").setLogUserId("logUserId1"))
                .build()));
  }

  @Test
  public void clearUserId_CohortMembership_NotSet() {
    assertEquals(
        CohortMembership.newBuilder()
            .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1"))
            .build(),
        clearUserId(
            CohortMembership.newBuilder()
                .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1"))
                .build()));
  }

  @Test
  public void clearUserId_CohortMembership_NoUserInfo() {
    assertEquals(
        CohortMembership.getDefaultInstance(), clearUserId(CohortMembership.getDefaultInstance()));
  }

  // Diagnostics.
  @Test
  public void clearUserId_Diagnostics() {
    assertEquals(
        Diagnostics.newBuilder()
            .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1"))
            .build(),
        clearUserId(
            Diagnostics.newBuilder()
                .setUserInfo(UserInfo.newBuilder().setUserId("userId1").setLogUserId("logUserId1"))
                .build()));
  }

  @Test
  public void clearUserId_Diagnostics_NotSet() {
    assertEquals(
        Diagnostics.newBuilder()
            .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1"))
            .build(),
        clearUserId(
            Diagnostics.newBuilder()
                .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1"))
                .build()));
  }

  @Test
  public void clearUserId_Diagnostics_NoUserInfo() {
    assertEquals(Diagnostics.getDefaultInstance(), clearUserId(Diagnostics.getDefaultInstance()));
  }
}
