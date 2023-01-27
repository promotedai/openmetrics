package ai.promoted.metrics.logprocessor.common.util;

import ai.promoted.metrics.common.Field;
import ai.promoted.metrics.common.RecordType;
import ai.promoted.metrics.error.LogFunctionName;
import ai.promoted.metrics.error.MismatchError;
import ai.promoted.proto.delivery.internal.features.AggMetric;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.FlatResponseInsertion;
import ai.promoted.proto.event.JoinedEvent;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.JoinedIdentifiers;
import com.google.common.collect.ImmutableList;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

public class FlatUtilTest {
    @Test
    public void getRequestId() {
        assertEquals("find me in request",
                FlatUtil.getRequestId(JoinedEvent.newBuilder()
                        .setIds(JoinedIdentifiers.newBuilder()
                                .setViewId("view1")
                                .setRequestId("")
                                .setInsertionId("insertion2"))
                        .setRequest(Request.newBuilder()
                                .setRequestId("find me in request"))
                        .setAction(Action.getDefaultInstance())
                        .build()));
        assertEquals("find me in action",
                FlatUtil.getRequestId(JoinedEvent.newBuilder()
                        .setIds(JoinedIdentifiers.newBuilder()
                                .setViewId("view1")
                                .setRequestId("")
                                .setInsertionId("insertion2"))
                        .setRequest(Request.getDefaultInstance())
                        .setAction(Action.newBuilder()
                                .setRequestId("find me in action"))
                        .build()));
    }

    // Instead of testing all mismatches, just test one.
    @Test
    public void mismatchErrorLogger() {
        BiConsumer<OutputTag<MismatchError>, MismatchError> errorLogger = Mockito.mock(BiConsumer.class);
        FlatUtil.setFlatImpression(
                JoinedEvent.newBuilder()
                        .setIds(JoinedIdentifiers.newBuilder()
                                .setPlatformId(1L)
                                .setViewId("view1")
                                .setRequestId("req1")
                                .setInsertionId("ins1")),
                Impression.newBuilder()
                        .setPlatformId(2L)
                        .setImpressionId("imp1")
                        .build(),
                errorLogger);
        verify(errorLogger).accept(any(), eq(MismatchError.newBuilder()
                .setRecordType(RecordType.IMPRESSION)
                .setField(Field.PLATFORM_ID)
                .setLhsIds(ai.promoted.metrics.common.JoinedIdentifiers.newBuilder()
                        .setPlatformId(1L)
                        .setViewId("view1")
                        .setRequestId("req1")
                        .setInsertionId("ins1")
                        .build())
                .setRhsRecordId("imp1")
                .setLhsLong(1L)
                .setRhsLong(2L)
                .setLogFunctionName(LogFunctionName.FLAT_UTIL_SET_FLAT_IMPRESSION)
                .build()));
    }

    @Test
    public void hasIgnoreUsage_joinedEvent() {
        assertFalse(FlatUtil.hasIgnoreUsage(JoinedEvent.getDefaultInstance()));
        assertTrue(FlatUtil.hasIgnoreUsage(JoinedEvent.newBuilder()
                .setRequest(Request.newBuilder()
                        .setUserInfo(UserInfo.newBuilder().setIgnoreUsage(true)))
                .build()));
    }

    @Test
    public void hasIgnoreUsage_flatResponseInsertion() {
        assertFalse(FlatUtil.hasIgnoreUsage(FlatResponseInsertion.getDefaultInstance()));
        assertTrue(FlatUtil.hasIgnoreUsage(FlatResponseInsertion.newBuilder()
                .setRequest(Request.newBuilder()
                        .setUserInfo(UserInfo.newBuilder().setIgnoreUsage(true)))
                .build()));
    }

    @Test
    public void createFlatResponseInsertion_primaryKeys() {
        JoinedEvent.Builder imp = JoinedEvent.newBuilder();
        imp.getIdsBuilder().setImpressionId("imp_id");
        JoinedEvent.Builder act = JoinedEvent.newBuilder();
        act.getActionBuilder().setActionId("act_id");
        FlatResponseInsertion.Builder actual =
                FlatUtil.createFlatResponseInsertion(ImmutableList.of(imp.build()), ImmutableList.of(act.build()));
        assertEquals("imp_id", actual.getImpression(0).getImpressionId());
        assertEquals("act_id", actual.getAction(0).getActionId());
    }

    @Test
    public void getCountAggValue_impression() {
        JoinedEvent.Builder joinedImpression = JoinedEvent.newBuilder();
        joinedImpression.getImpressionBuilder().setImpressionId("imp1");
        assertEquals(AggMetric.COUNT_IMPRESSION, FlatUtil.getCountAggValue(joinedImpression.build()));
    }

    @ParameterizedTest
    @EnumSource(ActionType.class)
    public void getCountAggValue_actionTypes(ActionType actionType) {
        AggMetric value = FlatUtil.getCountAggValue(createJoinedAction(actionType));
        switch (actionType) {
            case UNRECOGNIZED:
            case UNKNOWN_ACTION_TYPE:
            case CUSTOM_ACTION_TYPE:
                assertEquals(AggMetric.UNKNOWN_AGGREGATE, value);
                break;
            default:
                assertTrue(value.name().contains(actionType.name()));
        }
    }

    // Get the test cases from cespare/xxhash/blob/main/xxhash_test.go.
    @ParameterizedTest
    @CsvSource({
        ", ef46db3751d8e999",
        "a, d24ec4f1a98c6e5b",
        "as, 1c330fb2d66be179",
        "asd, 631c37ce72a97393",
        "asdf, 415872f599cea71e",
        "shoes, d5654a15f9b5efb1",
        "prom dresses, 2633a713effe6bda",
        "Call me Ishmael. Some years ago--never mind how long precisely-, 719e8cca0f55a204",
        "cALL ME iSHMAEL. sOME YEARS AGO--NEVER MIND HOW LONG PRECISELY-, 719e8cca0f55a204",
        "call me ishmael. some years ago--never mind how long precisely-, 719e8cca0f55a204",
    })
    public void getQueryHash(String query, String expectedHex) {
        JoinedEvent.Builder event = JoinedEvent.newBuilder();
        if (query != null) {
            event.getRequestBuilder().setSearchQuery(query);
        }
        assertEquals(Long.parseUnsignedLong(expectedHex, 16), FlatUtil.getQueryHash(event.build()));
    }

    @ParameterizedTest
    @CsvSource({
        ", ef46db3751d8e999",
        "a, d24ec4f1a98c6e5b",
        "as, 1c330fb2d66be179",
        "asd, 631c37ce72a97393",
        "asdf, 415872f599cea71e",
        "shoes, d5654a15f9b5efb1",
        "prom dresses, 2633a713effe6bda",
        "Call me Ishmael. Some years ago--never mind how long precisely-, 719e8cca0f55a204",
        "cALL ME iSHMAEL. sOME YEARS AGO--NEVER MIND HOW LONG PRECISELY-, 719e8cca0f55a204",
        "call me ishmael. some years ago--never mind how long precisely-, 719e8cca0f55a204",
    })
    public void getQueryHashHex(String query, String expectedHex) {
        JoinedEvent.Builder event = JoinedEvent.newBuilder();
        if (query != null) {
            event.getRequestBuilder().setSearchQuery(query);
        }
        assertEquals(expectedHex, FlatUtil.getQueryHashHex(event.build()));
    }

    private static JoinedEvent createJoinedAction(ActionType type) {
        JoinedEvent.Builder builder = JoinedEvent.newBuilder();
        if (ActionType.UNRECOGNIZED.equals(type)) {
            builder.getActionBuilder()
                    .setActionId(type.name())
                    .setActionType(ActionType.UNKNOWN_ACTION_TYPE);
        } else {
            builder.getActionBuilder()
                    .setActionId(type.name())
                    .setActionType(type);
        }
        return builder.build();
    }
}
