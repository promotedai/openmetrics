package ai.promoted.metrics.logprocessor.common.util;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import ai.promoted.metrics.common.Field;
import ai.promoted.metrics.common.RecordType;
import ai.promoted.metrics.error.LogFunctionName;
import ai.promoted.metrics.error.MismatchError;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.delivery.internal.features.AggMetric;
import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.AttributedAction;
import ai.promoted.proto.event.CartContent;
import ai.promoted.proto.event.FlatResponseInsertion;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.JoinedIdentifiers;
import ai.promoted.proto.event.JoinedImpression;
import com.google.common.collect.ImmutableList;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

public class FlatUtilTest {
  private static AttributedAction createAttributedAction(ActionType type) {
    AttributedAction.Builder builder = AttributedAction.newBuilder();
    if (ActionType.UNRECOGNIZED.equals(type)) {
      builder
          .getActionBuilder()
          .setActionId(type.name())
          .setActionType(ActionType.UNKNOWN_ACTION_TYPE);
    } else {
      builder.getActionBuilder().setActionId(type.name()).setActionType(type);
    }
    return builder.build();
  }

  // TODO - add tests for when values already exist.
  @Test
  public void setFlatRequest() {
    JoinedImpression.Builder event = JoinedImpression.newBuilder();
    Consumer<MismatchError> errorLogger = Mockito.mock(Consumer.class);
    JoinedImpression.Builder mergedBuilder =
        FlatUtil.setFlatRequest(
            event,
            Request.newBuilder()
                .setUserInfo(UserInfo.newBuilder().setAnonUserId("anon").setLogUserId("log"))
                .setViewId("view")
                .setSessionId("sess")
                .setViewId("view")
                .setRequestId("req")
                .setClientRequestId("creq")
                .build(),
            errorLogger);

    assertEquals(
        JoinedImpression.newBuilder()
            .setIds(
                JoinedIdentifiers.newBuilder()
                    .setAnonUserId("anon")
                    .setLogUserId("log")
                    .setSessionId("sess")
                    .setViewId("view")
                    .setRequestId("req"))
            .setRequest(Request.newBuilder().setClientRequestId("creq"))
            .build(),
        mergedBuilder.build());
  }

  // TODO - add tests for when values already exist.
  @Test
  public void setFlatImpression() {
    JoinedImpression.Builder event = JoinedImpression.newBuilder();
    Consumer<MismatchError> errorLogger = Mockito.mock(Consumer.class);
    JoinedImpression.Builder mergedBuilder =
        FlatUtil.setFlatImpression(
            event,
            Impression.newBuilder()
                .setUserInfo(UserInfo.newBuilder().setAnonUserId("anon").setLogUserId("log"))
                .setViewId("view")
                .setSessionId("sess")
                .setViewId("view")
                .setRequestId("req")
                .setInsertionId("ins")
                .setImpressionId("imp")
                .setContentId("cont")
                .build(),
            errorLogger);

    assertEquals(
        JoinedImpression.newBuilder()
            .setIds(
                JoinedIdentifiers.newBuilder()
                    .setAnonUserId("anon")
                    .setLogUserId("log")
                    .setSessionId("sess")
                    .setViewId("view")
                    .setRequestId("req")
                    .setInsertionId("ins")
                    .setImpressionId("imp"))
            .setImpression(Impression.newBuilder().setContentId("cont"))
            .build(),
        mergedBuilder.build());
  }

  @Test
  public void getContentIdPreferAction() {
    AttributedAction.Builder builder = AttributedAction.newBuilder();
    assertEquals("", FlatUtil.getContentIdPreferAction(builder.clone().build()));
    builder
        .getTouchpointBuilder()
        .getJoinedImpressionBuilder()
        .setResponseInsertion(Insertion.newBuilder().setContentId("ResIns.content"));
    assertEquals("ResIns.content", FlatUtil.getContentIdPreferAction(builder.clone().build()));
    builder.getActionBuilder().setContentId("Act.content");
    assertEquals("Act.content", FlatUtil.getContentIdPreferAction(builder.clone().build()));
    builder
        .getActionBuilder()
        .setSingleCartContent(CartContent.newBuilder().setContentId("Act.single_cart_content"));
    assertEquals(
        "Act.single_cart_content", FlatUtil.getContentIdPreferAction(builder.clone().build()));
  }

  // Instead of testing all mismatches, just test one.
  @Test
  public void mismatchErrorLogger() {
    Consumer<MismatchError> errorLogger = Mockito.mock(Consumer.class);
    FlatUtil.setFlatImpression(
        JoinedImpression.newBuilder()
            .setIds(
                JoinedIdentifiers.newBuilder()
                    .setPlatformId(1L)
                    .setViewId("view1")
                    .setRequestId("req1")
                    .setInsertionId("ins1")),
        Impression.newBuilder().setPlatformId(2L).setImpressionId("imp1").build(),
        errorLogger);
    verify(errorLogger)
        .accept(
            eq(
                MismatchError.newBuilder()
                    .setRecordType(RecordType.IMPRESSION)
                    .setField(Field.PLATFORM_ID)
                    .setLhsIds(
                        ai.promoted.metrics.common.JoinedIdentifiers.newBuilder()
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
    assertFalse(FlatUtil.hasIgnoreUsage(JoinedImpression.getDefaultInstance()));
    assertTrue(
        FlatUtil.hasIgnoreUsage(
            JoinedImpression.newBuilder()
                .setRequest(
                    Request.newBuilder().setUserInfo(UserInfo.newBuilder().setIgnoreUsage(true)))
                .build()));
  }

  @Test
  public void hasIgnoreUsage_flatResponseInsertion() {
    assertFalse(FlatUtil.hasIgnoreUsage(FlatResponseInsertion.getDefaultInstance()));
    assertTrue(
        FlatUtil.hasIgnoreUsage(
            FlatResponseInsertion.newBuilder()
                .setRequest(
                    Request.newBuilder().setUserInfo(UserInfo.newBuilder().setIgnoreUsage(true)))
                .build()));
  }

  @Test
  public void createFlatResponseInsertion_primaryKeys() {
    JoinedImpression.Builder imp = JoinedImpression.newBuilder();
    imp.getIdsBuilder().setInsertionId("ins_id").setImpressionId("imp_id");
    AttributedAction.Builder act = AttributedAction.newBuilder();
    act.getActionBuilder().setActionId("act_id");
    FlatResponseInsertion.Builder actual =
        FlatUtil.createFlatResponseInsertion(
            ImmutableList.of(imp.build()), ImmutableList.of(act.build()));
    assertEquals("ins_id", actual.getIds().getInsertionId());
    assertEquals("imp_id", actual.getImpression(0).getImpressionId());
    assertEquals("act_id", actual.getAttributedAction(0).getAction().getActionId());
  }

  // This is currently an unsupported corner case.
  // The unit test is more to make sure the utility function can handle it.
  @Test
  public void createFlatResponseInsertion_noJoinedImpressions() {
    AttributedAction.Builder act = AttributedAction.newBuilder();
    act.getActionBuilder().setActionId("act_id");
    act.getTouchpointBuilder()
        .getJoinedImpressionBuilder()
        .getIdsBuilder()
        .setInsertionId("ins_id")
        .setImpressionId("imp_id");
    FlatResponseInsertion.Builder actual =
        FlatUtil.createFlatResponseInsertion(ImmutableList.of(), ImmutableList.of(act.build()));
    assertEquals("ins_id", actual.getIds().getInsertionId());
    // FlatResponseInsertion.ids.impression_id should not be filled in since this record represents
    // an Insertion.
    assertEquals("", actual.getIds().getImpressionId());
    assertEquals("act_id", actual.getAttributedAction(0).getAction().getActionId());
  }

  @ParameterizedTest
  @EnumSource(ActionType.class)
  public void getCountAggValue_actionTypes(ActionType actionType) {
    AggMetric value = FlatUtil.getAggMetricValue(createAttributedAction(actionType));
    switch (actionType) {
      case UNRECOGNIZED:
      case UNKNOWN_ACTION_TYPE:
      case CUSTOM_ACTION_TYPE:
        assertEquals(AggMetric.UNKNOWN_AGGREGATE, value);
        break;
      default:
        assertThat(value.name()).contains(actionType.name());
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
    Request.Builder builder = Request.newBuilder();
    if (query != null) {
      builder.setSearchQuery(query);
    }
    Request request = builder.build();
    assertEquals(Long.parseUnsignedLong(expectedHex, 16), FlatUtil.getQueryHash(request));
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
    Request.Builder builder = Request.newBuilder();
    if (query != null) {
      builder.setSearchQuery(query);
    }
    Request request = builder.build();
    assertEquals(expectedHex, FlatUtil.getQueryHashHex(request));
  }

  @Test
  public void getQueryHash_EmptyString() {
    assertEquals(FlatUtil.EMPTY_STRING_HASH, FlatUtil.getQueryHash(Request.getDefaultInstance()));
  }
}
