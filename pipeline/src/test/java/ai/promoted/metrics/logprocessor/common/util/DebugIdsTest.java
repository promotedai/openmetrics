package ai.promoted.metrics.logprocessor.common.util;

import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.delivery.DeliveryExecution;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.delivery.Response;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.JoinedEvent;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.JoinedIdentifiers;
import ai.promoted.proto.event.TinyDeliveryLog;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DebugIdsTest {

    private static <T> ImmutableSet<T> set() {
        return ImmutableSet.of();
    }

    private static ImmutableSet<String> set(String... values) {
        return ImmutableSet.copyOf(values);
    }

    // matchesViewId()

    @Test
    public void matchesViewId_noIds() {
        DebugIds debugIds = DebugIds.empty();
        assertFalse(debugIds.matchesViewId("viewId1"));
    }

    @Test
    public void matchesViewId_matchIds() {
        DebugIds debugIds = DebugIds.builder().setViewIds(set("viewId1")).build();
        assertTrue(debugIds.matchesViewId("viewId1"));
    }

    @Test
    public void matchesViewId_noMatchIds() {
        DebugIds debugIds = DebugIds.builder().setViewIds(set("viewId2")).build();
        assertFalse(debugIds.matchesViewId("viewId1"));
    }

    // matchesRequestId()

    @Test
    public void matchesRequestId_noIds() {
        DebugIds debugIds = DebugIds.empty();
        assertFalse(debugIds.matchesRequestId("requestId1"));
    }

    @Test
    public void matchesRequestId_matchIds() {
        DebugIds debugIds = DebugIds.builder().setRequestIds(set("requestId1")).build();
        assertTrue(debugIds.matchesRequestId("requestId1"));
    }

    @Test
    public void matchesRequestId_noMatchIds() {
        DebugIds debugIds = DebugIds.builder().setRequestIds(set("requestId2")).build();
        assertFalse(debugIds.matchesRequestId("requestId1"));
    }

    // matchesImpressionId()

    @Test
    public void matchesImpressionId_noIds() {
        DebugIds debugIds = DebugIds.empty();
        assertFalse(debugIds.matchesImpressionId("impressionId1"));
    }

    @Test
    public void matchesImpressionId_matchIds() {
        DebugIds debugIds = DebugIds.builder().setImpressionIds(set("impressionId1")).build();
        assertTrue(debugIds.matchesImpressionId("impressionId1"));
    }

    @Test
    public void matchesImpressionId_noMatchIds() {
        DebugIds debugIds = DebugIds.builder().setImpressionIds(set("impressionId2")).build();
        assertFalse(debugIds.matchesImpressionId("impressionId1"));
    }

    // matchesActionId()

    @Test
    public void matchesActionId_noIds() {
        DebugIds debugIds = DebugIds.empty();
        assertFalse(debugIds.matchesImpressionId("impressionId1"));
    }

    @Test
    public void matchesActionId_matchIds() {
        DebugIds debugIds = DebugIds.builder().setImpressionIds(set("impressionId1")).build();
        assertTrue(debugIds.matchesImpressionId("impressionId1"));
    }

    @Test
    public void matchesActionId_noMatchIds() {
        DebugIds debugIds = DebugIds.builder().setImpressionIds(set("impressionId2")).build();
        assertFalse(debugIds.matchesImpressionId("impressionId1"));
    }

    // matches(JoinedEvent)

    @Test
    public void matchesJoinedEvent_noIds() {
        DebugIds debugIds = DebugIds.empty();
        assertFalse(debugIds.matches(
                JoinedEvent.newBuilder()
                        .setIds(JoinedIdentifiers.newBuilder()
                                .setUserId("userId"))
                        .build()));
    }

    @Test
    public void matchesJoinedEvent_matchUserId() {
        DebugIds debugIds = DebugIds.builder().setUserIds(set("userId1")).build();
        assertTrue(debugIds.matches(
                JoinedEvent.newBuilder()
                        .setIds(JoinedIdentifiers.newBuilder()
                                .setUserId("userId1"))
                        .build()));
    }

    @Test
    public void matchesJoinedEvent_noMatchUserId() {
        DebugIds debugIds = DebugIds.builder().setUserIds(set("userId1")).build();
        assertFalse(debugIds.matches(
                JoinedEvent.newBuilder()
                        .setIds(JoinedIdentifiers.newBuilder()
                                .setUserId("userId2"))
                        .build()));
    }

    @Test
    public void matchesJoinedEvent_matchLogUserId() {
        DebugIds debugIds = DebugIds.builder().setLogUserIds(set("logUserId1")).build();
        assertTrue(debugIds.matches(
                JoinedEvent.newBuilder()
                        .setIds(JoinedIdentifiers.newBuilder()
                                .setLogUserId("logUserId1"))
                        .build()));
    }

    @Test
    public void matchesJoinedEvent_noMatchLogUserId() {
        DebugIds debugIds = DebugIds.builder().setLogUserIds(set("logUserId1")).build();
        assertFalse(debugIds.matches(
                JoinedEvent.newBuilder()
                        .setIds(JoinedIdentifiers.newBuilder()
                                .setLogUserId("logUserId2"))
                        .build()));
    }
    
    @Test
    public void matchesJoinedEvent_matchSessionId() {
        DebugIds debugIds = DebugIds.builder().setSessionIds(set("sessionId1")).build();
        assertTrue(debugIds.matches(
                JoinedEvent.newBuilder()
                        .setIds(JoinedIdentifiers.newBuilder()
                                .setSessionId("sessionId1"))
                        .build()));
    }

    @Test
    public void matchesJoinedEvent_noMatchSessionId() {
        DebugIds debugIds = DebugIds.builder().setSessionIds(set("sessionId1")).build();
        assertFalse(debugIds.matches(
                JoinedEvent.newBuilder()
                        .setIds(JoinedIdentifiers.newBuilder()
                                .setSessionId("sessionId2"))
                        .build()));
    }
    
    @Test
    public void matchesJoinedEvent_matchViewId() {
        DebugIds debugIds = DebugIds.builder().setViewIds(set("viewId1")).build();
        assertTrue(debugIds.matches(
                JoinedEvent.newBuilder()
                        .setIds(JoinedIdentifiers.newBuilder()
                                .setViewId("viewId1"))
                        .build()));
    }

    @Test
    public void matchesJoinedEvent_noMatchViewId() {
        DebugIds debugIds = DebugIds.builder().setViewIds(set("viewId1")).build();
        assertFalse(debugIds.matches(
                JoinedEvent.newBuilder()
                        .setIds(JoinedIdentifiers.newBuilder()
                                .setViewId("viewId2"))
                        .build()));
    }

    @Test
    public void matchesJoinedEvent_matchRequestId() {
        DebugIds debugIds = DebugIds.builder().setRequestIds(set("requestId1")).build();
        assertTrue(debugIds.matches(
                JoinedEvent.newBuilder()
                        .setIds(JoinedIdentifiers.newBuilder()
                                .setRequestId("requestId1"))
                        .build()));
    }

    @Test
    public void matchesJoinedEvent_noMatchRequestId() {
        DebugIds debugIds = DebugIds.builder().setRequestIds(set("requestId1")).build();
        assertFalse(debugIds.matches(
                JoinedEvent.newBuilder()
                        .setIds(JoinedIdentifiers.newBuilder()
                                .setRequestId("requestId2"))
                        .build()));
    }

    @Test
    public void matchesJoinedEvent_matchInsertionId() {
        DebugIds debugIds = DebugIds.builder().setInsertionIds(set("insertionId1")).build();
        assertTrue(debugIds.matches(
                JoinedEvent.newBuilder()
                        .setIds(JoinedIdentifiers.newBuilder()
                                .setInsertionId("insertionId1"))
                        .build()));
    }

    @Test
    public void matchesJoinedEvent_noMatchInsertionId() {
        DebugIds debugIds = DebugIds.builder().setInsertionIds(set("insertionId1")).build();
        assertFalse(debugIds.matches(
                JoinedEvent.newBuilder()
                        .setIds(JoinedIdentifiers.newBuilder()
                                .setInsertionId("insertionId2"))
                        .build()));
    }

    @Test
    public void matchesJoinedEvent_matchImpressionId() {
        DebugIds debugIds = DebugIds.builder().setImpressionIds(set("impressionId1")).build();
        assertTrue(debugIds.matches(
                JoinedEvent.newBuilder()
                        .setIds(JoinedIdentifiers.newBuilder()
                                .setImpressionId("impressionId1"))
                        .build()));
    }

    @Test
    public void matchesJoinedEvent_noMatchImpressionId() {
        DebugIds debugIds = DebugIds.builder().setImpressionIds(set("impressionId1")).build();
        assertFalse(debugIds.matches(
                JoinedEvent.newBuilder()
                        .setIds(JoinedIdentifiers.newBuilder()
                                .setImpressionId("impressionId2"))
                        .build()));
    }

    @Test
    public void matchesJoinedEvent_matchActionId() {
        DebugIds debugIds = DebugIds.builder().setActionIds(set("actionId1")).build();
        assertTrue(debugIds.matches(
                JoinedEvent.newBuilder()
                        .setAction(Action.newBuilder()
                                .setActionId("actionId1"))
                        .build()));
    }

    @Test
    public void matchesJoinedEvent_noMatchActionId() {
        DebugIds debugIds = DebugIds.builder().setActionIds(set("actionId1")).build();
        assertFalse(debugIds.matches(
                JoinedEvent.newBuilder()
                        .setAction(Action.newBuilder()
                                .setActionId("actionId2"))
                        .build()));
    }

    // matches(TinyDeliveryLog)

    @Test
    public void matchesTinyFlatLog_noIds() {
        DebugIds debugIds = DebugIds.empty();
        assertFalse(debugIds.matches(TinyDeliveryLog.getDefaultInstance()));
    }

    @Test
    public void matchesTinyFlatLog_matchLogUserId() {
        DebugIds debugIds = DebugIds.builder().setLogUserIds(set("logUserId1")).build();
        assertTrue(debugIds.matches(TinyDeliveryLog.newBuilder().setLogUserId("logUserId1").build()));
    }

    @Test
    public void matchesTinyFlatLog_noMatchLogUserId() {
        DebugIds debugIds = DebugIds.builder().setLogUserIds(set("logUserId1")).build();
        assertFalse(debugIds.matches(TinyDeliveryLog.newBuilder().setLogUserId("logUserId2").build()));
    }

    @Test
    public void matchesTinyFlatLog_matchViewId() {
        DebugIds debugIds = DebugIds.builder().setViewIds(set("viewId1")).build();
        assertTrue(debugIds.matches(TinyDeliveryLog.newBuilder().setViewId("viewId1").build()));
    }

    @Test
    public void matchesTinyFlatLog_noMatchViewId() {
        DebugIds debugIds = DebugIds.builder().setViewIds(set("viewId1")).build();
        assertFalse(debugIds.matches(TinyDeliveryLog.newBuilder().setViewId("viewId2").build()));
    }

    @Test
    public void matchesTinyFlatLog_matchRequestId() {
        DebugIds debugIds = DebugIds.builder().setRequestIds(set("requestId1")).build();
        assertTrue(debugIds.matches(TinyDeliveryLog.newBuilder().setRequestId("requestId1").build()));
    }

    @Test
    public void matchesTinyFlatLog_noMatchRequestId() {
        DebugIds debugIds = DebugIds.builder().setRequestIds(set("requestId1")).build();
        assertFalse(debugIds.matches(TinyDeliveryLog.newBuilder().setRequestId("requestId2").build()));
    }

    @Test
    public void matchesTinyFlatLog_matchInsertionId() {
        DebugIds debugIds = DebugIds.builder().setInsertionIds(set("insertionId1")).build();
        assertTrue(debugIds.matches(TinyDeliveryLog.newBuilder().addResponseInsertion(
                TinyDeliveryLog.TinyInsertion.newBuilder().setInsertionId("insertionId1"))
                .build()));
    }

    @Test
    public void matchesTinyFlatLog_noMatchInsertionId() {
        DebugIds debugIds = DebugIds.builder().setInsertionIds(set("insertionId1")).build();
        assertFalse(debugIds.matches(TinyDeliveryLog.newBuilder().addResponseInsertion(
                TinyDeliveryLog.TinyInsertion.newBuilder().setInsertionId("insertionId2"))
                .build()));
    }

    // matches(String)

    @Test
    public void matchesString_noIds() {
        DebugIds debugIds = DebugIds.empty();
        assertFalse(debugIds.matches("userId"));
    }

    @Test
    public void matchesString_matchUserId() {
        DebugIds debugIds = DebugIds.builder().setUserIds(set("userId1")).build();
        assertTrue(debugIds.matches("userId1"));
    }

    @Test
    public void matchesString_noMatchUserId() {
        DebugIds debugIds = DebugIds.builder().setUserIds(set("userId1")).build();
        assertFalse(debugIds.matches("userId2"));
    }

    @Test
    public void matchesString_matchLogUserId() {
        DebugIds debugIds = DebugIds.builder().setLogUserIds(set("logUserId1")).build();
        assertTrue(debugIds.matches("logUserId1"));
    }

    @Test
    public void matchesString_noMatchLogUserId() {
        DebugIds debugIds = DebugIds.builder().setLogUserIds(set("logUserId1")).build();
        assertFalse(debugIds.matches("logUserId2"));
    }

    @Test
    public void matchesString_matchSessionId() {
        DebugIds debugIds = DebugIds.builder().setSessionIds(set("sessionId1")).build();
        assertTrue(debugIds.matches("sessionId1"));
    }

    @Test
    public void matchesString_noMatchSessionId() {
        DebugIds debugIds = DebugIds.builder().setSessionIds(set("sessionId1")).build();
        assertFalse(debugIds.matches("sessionId2"));
    }

    @Test
    public void matchesString_matchViewId() {
        DebugIds debugIds = DebugIds.builder().setViewIds(set("viewId1")).build();
        assertTrue(debugIds.matches("viewId1"));
    }

    @Test
    public void matchesString_noMatchViewId() {
        DebugIds debugIds = DebugIds.builder().setViewIds(set("viewId1")).build();
        assertFalse(debugIds.matches("viewId2"));
    }

    @Test
    public void matchesString_matchRequestId() {
        DebugIds debugIds = DebugIds.builder().setRequestIds(set("requestId1")).build();
        assertTrue(debugIds.matches("requestId1"));
    }

    @Test
    public void matchesString_noMatchRequestId() {
        DebugIds debugIds = DebugIds.builder().setRequestIds(set("requestId1")).build();
        assertFalse(debugIds.matches("requestId2"));
    }

    @Test
    public void matchesString_matchInsertionId() {
        DebugIds debugIds = DebugIds.builder().setInsertionIds(set("insertionId1")).build();
        assertTrue(debugIds.matches("insertionId1"));
    }

    @Test
    public void matchesString_noMatchInsertionId() {
        DebugIds debugIds = DebugIds.builder().setInsertionIds(set("insertionId1")).build();
        assertFalse(debugIds.matches("insertionId2"));
    }

    @Test
    public void matchesString_matchImpressionId() {
        DebugIds debugIds = DebugIds.builder().setImpressionIds(set("impressionId1")).build();
        assertTrue(debugIds.matches("impressionId1"));
    }

    @Test
    public void matchesString_noMatchImpressionId() {
        DebugIds debugIds = DebugIds.builder().setImpressionIds(set("impressionId1")).build();
        assertFalse(debugIds.matches("impressionId2"));
    }

    @Test
    public void matchesString_matchActionId() {
        DebugIds debugIds = DebugIds.builder().setActionIds(set("actionId1")).build();
        assertTrue(debugIds.matches("actionId1"));
    }

    @Test
    public void matchesString_noMatchActionId() {
        DebugIds debugIds = DebugIds.builder().setActionIds(set("actionId1")).build();
        assertFalse(debugIds.matches("actionId2"));
    }

    // matches(DeliveryLog)

    @Test
    public void matchesDeliveryLog_noIds() {
        DebugIds debugIds = DebugIds.empty();
        assertFalse(debugIds.matches(
                DeliveryLog.newBuilder().build()));
    }

    @Test
    public void matchesDeliveryLog_matchUserId() {
        DebugIds debugIds = DebugIds.builder().setUserIds(set("userId1")).build();
        assertTrue(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setRequest(Request.newBuilder()
                                .setUserInfo(UserInfo.newBuilder()
                                        .setUserId("userId1")))
                        .build()));
        assertTrue(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setRequest(Request.newBuilder()
                                .addInsertion(Insertion.newBuilder()
                                        .setUserInfo(UserInfo.newBuilder()
                                                .setUserId("userId1"))))
                        .build()));
        assertTrue(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setExecution(DeliveryExecution.newBuilder()
                                .addExecutionInsertion(Insertion.newBuilder()
                                        .setUserInfo(UserInfo.newBuilder()
                                                .setUserId("userId1"))))
                        .build()));
        assertTrue(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setResponse(Response.newBuilder()
                                .addInsertion(Insertion.newBuilder()
                                        .setUserInfo(UserInfo.newBuilder()
                                                .setUserId("userId1"))))
                        .build()));
    }

    @Test
    public void matchesDeliveryLog_noMatchUserId() {
        DebugIds debugIds = DebugIds.builder().setUserIds(set("userId1")).build();
        assertFalse(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setRequest(Request.newBuilder()
                                .setUserInfo(UserInfo.newBuilder()
                                        .setUserId("userId2")))
                        .build()));
        assertFalse(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setRequest(Request.newBuilder()
                                .addInsertion(Insertion.newBuilder()
                                        .setUserInfo(UserInfo.newBuilder()
                                                .setUserId("userId2"))))
                        .build()));
        assertFalse(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setExecution(DeliveryExecution.newBuilder()
                                .addExecutionInsertion(Insertion.newBuilder()
                                        .setUserInfo(UserInfo.newBuilder()
                                                .setUserId("userId2"))))
                        .build()));
        assertFalse(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setResponse(Response.newBuilder()
                                .addInsertion(Insertion.newBuilder()
                                        .setUserInfo(UserInfo.newBuilder()
                                                .setUserId("userId2"))))
                        .build()));
    }

    @Test
    public void matchesDeliveryLog_matchLogUserId() {
        DebugIds debugIds = DebugIds.builder().setLogUserIds(set("logUserId1")).build();
        assertTrue(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setRequest(Request.newBuilder()
                                .setUserInfo(UserInfo.newBuilder()
                                        .setLogUserId("logUserId1")))
                        .build()));
        assertTrue(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setRequest(Request.newBuilder()
                                .addInsertion(Insertion.newBuilder()
                                        .setUserInfo(UserInfo.newBuilder()
                                                .setLogUserId("logUserId1"))))
                        .build()));
        assertTrue(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setExecution(DeliveryExecution.newBuilder()
                                .addExecutionInsertion(Insertion.newBuilder()
                                        .setUserInfo(UserInfo.newBuilder()
                                                .setLogUserId("logUserId1"))))
                        .build()));
        assertTrue(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setResponse(Response.newBuilder()
                                .addInsertion(Insertion.newBuilder()
                                        .setUserInfo(UserInfo.newBuilder()
                                                .setLogUserId("logUserId1"))))
                        .build()));
    }

    @Test
    public void matchesDeliveryLog_noMatchLogUserId() {
        DebugIds debugIds = DebugIds.builder().setLogUserIds(set("logUserId1")).build();
        assertFalse(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setRequest(Request.newBuilder()
                                .setUserInfo(UserInfo.newBuilder()
                                        .setLogUserId("logUserId2")))
                        .build()));
        assertFalse(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setRequest(Request.newBuilder()
                                .addInsertion(Insertion.newBuilder()
                                        .setUserInfo(UserInfo.newBuilder()
                                                .setLogUserId("logUserId2"))))
                        .build()));
        assertFalse(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setExecution(DeliveryExecution.newBuilder()
                                .addExecutionInsertion(Insertion.newBuilder()
                                        .setUserInfo(UserInfo.newBuilder()
                                                .setLogUserId("logUserId2"))))
                        .build()));
        assertFalse(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setResponse(Response.newBuilder()
                                .addInsertion(Insertion.newBuilder()
                                        .setUserInfo(UserInfo.newBuilder()
                                                .setLogUserId("logUserId2"))))
                        .build()));
    }

    @Test
    public void matchesDeliveryLog_matchSessionId() {
        DebugIds debugIds = DebugIds.builder().setSessionIds(set("sessionId1")).build();
        assertTrue(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setRequest(Request.newBuilder()
                                .setSessionId("sessionId1"))
                        .build()));
        assertTrue(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setRequest(Request.newBuilder()
                                .addInsertion(Insertion.newBuilder()
                                        .setSessionId("sessionId1")))
                        .build()));
        assertTrue(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setExecution(DeliveryExecution.newBuilder()
                                .addExecutionInsertion(Insertion.newBuilder()
                                        .setSessionId("sessionId1")))
                        .build()));
        assertTrue(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setResponse(Response.newBuilder()
                                .addInsertion(Insertion.newBuilder()
                                        .setSessionId("sessionId1")))
                        .build()));
    }

    @Test
    public void matchesDeliveryLog_noMatchSessionId() {
        DebugIds debugIds = DebugIds.builder().setSessionIds(set("sessionId1")).build();
        assertFalse(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setRequest(Request.newBuilder()
                                .setSessionId("sessionId2"))
                        .build()));
        assertFalse(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setRequest(Request.newBuilder()
                                .addInsertion(Insertion.newBuilder()
                                        .setSessionId("sessionId2")))
                        .build()));
        assertFalse(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setExecution(DeliveryExecution.newBuilder()
                                .addExecutionInsertion(Insertion.newBuilder()
                                        .setSessionId("sessionId2")))
                        .build()));
        assertFalse(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setResponse(Response.newBuilder()
                                .addInsertion(Insertion.newBuilder()
                                        .setSessionId("sessionId2")))
                        .build()));
    }

    @Test
    public void matchesDeliveryLog_matchViewId() {
        DebugIds debugIds = DebugIds.builder().setViewIds(set("viewId1")).build();
        assertTrue(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setRequest(Request.newBuilder()
                                .setViewId("viewId1"))
                        .build()));
        assertTrue(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setRequest(Request.newBuilder()
                                .addInsertion(Insertion.newBuilder()
                                        .setViewId("viewId1")))
                        .build()));
        assertTrue(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setExecution(DeliveryExecution.newBuilder()
                                .addExecutionInsertion(Insertion.newBuilder()
                                        .setViewId("viewId1")))
                        .build()));
        assertTrue(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setResponse(Response.newBuilder()
                                .addInsertion(Insertion.newBuilder()
                                        .setViewId("viewId1")))
                        .build()));
    }

    @Test
    public void matchesDeliveryLog_noMatchViewId() {
        DebugIds debugIds = DebugIds.builder().setViewIds(set("viewId1")).build();
        assertFalse(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setRequest(Request.newBuilder()
                                .setViewId("viewId2"))
                        .build()));
        assertFalse(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setRequest(Request.newBuilder()
                                .addInsertion(Insertion.newBuilder()
                                        .setViewId("viewId2")))
                        .build()));
        assertFalse(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setExecution(DeliveryExecution.newBuilder()
                                .addExecutionInsertion(Insertion.newBuilder()
                                        .setViewId("viewId2")))
                        .build()));
        assertFalse(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setResponse(Response.newBuilder()
                                .addInsertion(Insertion.newBuilder()
                                        .setViewId("viewId2")))
                        .build()));
    }

    @Test
    public void matchesDeliveryLog_matchRequestId() {
        DebugIds debugIds = DebugIds.builder().setRequestIds(set("requestId1")).build();
        assertTrue(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setRequest(Request.newBuilder()
                                .setRequestId("requestId1"))
                        .build()));
        assertTrue(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setRequest(Request.newBuilder()
                                .addInsertion(Insertion.newBuilder()
                                        .setRequestId("requestId1")))
                        .build()));
        assertTrue(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setExecution(DeliveryExecution.newBuilder()
                                .addExecutionInsertion(Insertion.newBuilder()
                                        .setRequestId("requestId1")))
                        .build()));
        assertTrue(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setResponse(Response.newBuilder()
                                .addInsertion(Insertion.newBuilder()
                                        .setRequestId("requestId1")))
                        .build()));
    }

    @Test
    public void matchesDeliveryLog_noMatchRequestId() {
        DebugIds debugIds = DebugIds.builder().setRequestIds(set("requestId1")).build();
        assertFalse(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setRequest(Request.newBuilder()
                                .setRequestId("requestId2"))
                        .build()));
        assertFalse(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setRequest(Request.newBuilder()
                                .addInsertion(Insertion.newBuilder()
                                        .setRequestId("requestId2")))
                        .build()));
        assertFalse(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setExecution(DeliveryExecution.newBuilder()
                                .addExecutionInsertion(Insertion.newBuilder()
                                        .setRequestId("requestId2")))
                        .build()));
        assertFalse(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setResponse(Response.newBuilder()
                                .addInsertion(Insertion.newBuilder()
                                        .setRequestId("requestId2")))
                        .build()));
    }

    @Test
    public void matchesDeliveryLog_matchInsertionId() {
        DebugIds debugIds = DebugIds.builder().setInsertionIds(set("insertionId1")).build();
        assertTrue(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setRequest(Request.newBuilder()
                                .addInsertion(Insertion.newBuilder()
                                        .setInsertionId("insertionId1")))
                        .build()));
        assertTrue(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setExecution(DeliveryExecution.newBuilder()
                                .addExecutionInsertion(Insertion.newBuilder()
                                        .setInsertionId("insertionId1")))
                        .build()));
        assertTrue(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setResponse(Response.newBuilder()
                                .addInsertion(Insertion.newBuilder()
                                        .setInsertionId("insertionId1")))
                        .build()));
    }

    @Test
    public void matchesDeliveryLog_noMatchInsertionId() {
        DebugIds debugIds = DebugIds.builder().setInsertionIds(set("insertionId1")).build();
        assertFalse(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setRequest(Request.newBuilder()
                                .addInsertion(Insertion.newBuilder()
                                        .setInsertionId("insertionId2")))
                        .build()));
        assertFalse(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setExecution(DeliveryExecution.newBuilder()
                                .addExecutionInsertion(Insertion.newBuilder()
                                        .setInsertionId("insertionId2")))
                        .build()));
        assertFalse(debugIds.matches(
                DeliveryLog.newBuilder()
                        .setResponse(Response.newBuilder()
                                .addInsertion(Insertion.newBuilder()
                                        .setInsertionId("insertionId2")))
                        .build()));
    }

    // matches(Impression)

    @Test
    public void matchesImpression_noIds() {
        DebugIds debugIds = DebugIds.empty();
        assertFalse(debugIds.matches(Impression.getDefaultInstance()));
    }

    @Test
    public void matchesImpression_matchUserId() {
        DebugIds debugIds = DebugIds.builder().setUserIds(set("userId1")).build();
        assertTrue(debugIds.matches(
                Impression.newBuilder()
                        .setUserInfo(UserInfo.newBuilder().setUserId("userId1"))
                        .build()));
    }

    @Test
    public void matchesImpression_noMatchUserId() {
        DebugIds debugIds = DebugIds.builder().setUserIds(set("userId1")).build();
        assertFalse(debugIds.matches(
                Impression.newBuilder()
                        .setUserInfo(UserInfo.newBuilder().setUserId("userId2"))
                        .build()));
    }

    @Test
    public void matchesImpression_matchLogUserId() {
        DebugIds debugIds = DebugIds.builder().setLogUserIds(set("logUserId1")).build();
        assertTrue(debugIds.matches(
                Impression.newBuilder()
                        .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1"))
                        .build()));
    }

    @Test
    public void matchesImpression_noMatchLogUserId() {
        DebugIds debugIds = DebugIds.builder().setLogUserIds(set("logUserId1")).build();
        assertFalse(debugIds.matches(
                Impression.newBuilder()
                        .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId2"))
                        .build()));
    }

    @Test
    public void matchesImpression_matchSessionId() {
        DebugIds debugIds = DebugIds.builder().setSessionIds(set("session1")).build();
        assertTrue(debugIds.matches(Impression.newBuilder().setSessionId("session1").build()));
    }

    @Test
    public void matchesImpression_noMatchSessionId() {
        DebugIds debugIds = DebugIds.builder().setSessionIds(set("session1")).build();
        assertFalse(debugIds.matches(Impression.newBuilder().setSessionId("session2").build()));
    }

    @Test
    public void matchesImpression_matchViewId() {
        DebugIds debugIds = DebugIds.builder().setViewIds(set("view1")).build();
        assertTrue(debugIds.matches(Impression.newBuilder().setViewId("view1").build()));
    }

    @Test
    public void matchesImpression_noMatchViewId() {
        DebugIds debugIds = DebugIds.builder().setViewIds(set("view1")).build();
        assertFalse(debugIds.matches(Impression.newBuilder().setViewId("view2").build()));
    }

    @Test
    public void matchesImpression_matchRequestId() {
        DebugIds debugIds = DebugIds.builder().setRequestIds(set("request1")).build();
        assertTrue(debugIds.matches(Impression.newBuilder().setRequestId("request1").build()));
    }

    @Test
    public void matchesImpression_noMatchRequestId() {
        DebugIds debugIds = DebugIds.builder().setRequestIds(set("request1")).build();
        assertFalse(debugIds.matches(Impression.newBuilder().setRequestId("request2").build()));
    }

    @Test
    public void matchesImpression_matchInsertionId() {
        DebugIds debugIds = DebugIds.builder().setInsertionIds(set("insertion1")).build();
        assertTrue(debugIds.matches(Impression.newBuilder().setInsertionId("insertion1").build()));
    }

    @Test
    public void matchesImpression_noMatchInsertionId() {
        DebugIds debugIds = DebugIds.builder().setInsertionIds(set("insertion1")).build();
        assertFalse(debugIds.matches(Impression.newBuilder().setInsertionId("insertion2").build()));
    }

    @Test
    public void matchesImpression_matchImpressionId() {
        DebugIds debugIds = DebugIds.builder().setImpressionIds(set("impression1")).build();
        assertTrue(debugIds.matches(Impression.newBuilder().setImpressionId("impression1").build()));
    }

    @Test
    public void matchesImpression_noMatchImpressionId() {
        DebugIds debugIds = DebugIds.builder().setImpressionIds(set("impression1")).build();
        assertFalse(debugIds.matches(Impression.newBuilder().setImpressionId("impression2").build()));
    }


    // matches(Action)

    @Test
    public void matchesAction_noIds() {
        DebugIds debugIds = DebugIds.empty();
        assertFalse(debugIds.matches(Action.getDefaultInstance()));
    }

    @Test
    public void matchesAction_matchUserId() {
        DebugIds debugIds = DebugIds.builder().setUserIds(set("userId1")).build();
        assertTrue(debugIds.matches(
                Action.newBuilder()
                        .setUserInfo(UserInfo.newBuilder().setUserId("userId1"))
                        .build()));
    }

    @Test
    public void matchesAction_noMatchUserId() {
        DebugIds debugIds = DebugIds.builder().setUserIds(set("userId1")).build();
        assertFalse(debugIds.matches(
                Action.newBuilder()
                        .setUserInfo(UserInfo.newBuilder().setUserId("userId2"))
                        .build()));
    }

    @Test
    public void matchesAction_matchLogUserId() {
        DebugIds debugIds = DebugIds.builder().setLogUserIds(set("logUserId1")).build();
        assertTrue(debugIds.matches(
                Action.newBuilder()
                        .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId1"))
                        .build()));
    }

    @Test
    public void matchesAction_noMatchLogUserId() {
        DebugIds debugIds = DebugIds.builder().setLogUserIds(set("logUserId1")).build();
        assertFalse(debugIds.matches(
                Action.newBuilder()
                        .setUserInfo(UserInfo.newBuilder().setLogUserId("logUserId2"))
                        .build()));
    }

    @Test
    public void matchesAction_matchSessionId() {
        DebugIds debugIds = DebugIds.builder().setSessionIds(set("session1")).build();
        assertTrue(debugIds.matches(Action.newBuilder().setSessionId("session1").build()));
    }

    @Test
    public void matchesAction_noMatchSessionId() {
        DebugIds debugIds = DebugIds.builder().setSessionIds(set("session1")).build();
        assertFalse(debugIds.matches(Action.newBuilder().setSessionId("session2").build()));
    }

    @Test
    public void matchesAction_matchViewId() {
        DebugIds debugIds = DebugIds.builder().setViewIds(set("view1")).build();
        assertTrue(debugIds.matches(Action.newBuilder().setViewId("view1").build()));
    }

    @Test
    public void matchesAction_noMatchViewId() {
        DebugIds debugIds = DebugIds.builder().setViewIds(set("view1")).build();
        assertFalse(debugIds.matches(Action.newBuilder().setViewId("view2").build()));
    }

    @Test
    public void matchesAction_matchRequestId() {
        DebugIds debugIds = DebugIds.builder().setRequestIds(set("request1")).build();
        assertTrue(debugIds.matches(Action.newBuilder().setRequestId("request1").build()));
    }

    @Test
    public void matchesAction_noMatchRequestId() {
        DebugIds debugIds = DebugIds.builder().setRequestIds(set("request1")).build();
        assertFalse(debugIds.matches(Action.newBuilder().setRequestId("request2").build()));
    }

    @Test
    public void matchesAction_matchInsertionId() {
        DebugIds debugIds = DebugIds.builder().setInsertionIds(set("insertion1")).build();
        assertTrue(debugIds.matches(Action.newBuilder().setInsertionId("insertion1").build()));
    }

    @Test
    public void matchesAction_noMatchInsertionId() {
        DebugIds debugIds = DebugIds.builder().setInsertionIds(set("insertion1")).build();
        assertFalse(debugIds.matches(Action.newBuilder().setInsertionId("insertion2").build()));
    }

    @Test
    public void matchesAction_matchImpressionId() {
        DebugIds debugIds = DebugIds.builder().setImpressionIds(set("impression1")).build();
        assertTrue(debugIds.matches(Action.newBuilder().setImpressionId("impression1").build()));
    }

    @Test
    public void matchesAction_noMatchImpressionId() {
        DebugIds debugIds = DebugIds.builder().setImpressionIds(set("impression1")).build();
        assertFalse(debugIds.matches(Action.newBuilder().setImpressionId("impression2").build()));
    }

    @Test
    public void matchesAction_matchActionId() {
        DebugIds debugIds = DebugIds.builder().setActionIds(set("action1")).build();
        assertTrue(debugIds.matches(Action.newBuilder().setActionId("action1").build()));
    }

    @Test
    public void matchesAction_noMatchActionId() {
        DebugIds debugIds = DebugIds.builder().setActionIds(set("action1")).build();
        assertFalse(debugIds.matches(Action.newBuilder().setActionId("action2").build()));
    }


}
