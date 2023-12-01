package ai.promoted.metrics.logprocessor.common.functions.inferred;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.promoted.metrics.logprocessor.common.functions.KeyUtil;
import ai.promoted.metrics.logprocessor.common.functions.inferred.AbstractMergeDetails.MissingEvent;
import ai.promoted.metrics.logprocessor.common.util.DebugIds;
import ai.promoted.metrics.logprocessor.common.util.TrackingUtil;
import ai.promoted.proto.common.ClientInfo;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.delivery.Response;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.AttributedAction;
import ai.promoted.proto.event.Attribution;
import ai.promoted.proto.event.Cart;
import ai.promoted.proto.event.CartContent;
import ai.promoted.proto.event.HiddenApiRequest;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.JoinedIdentifiers;
import ai.promoted.proto.event.JoinedImpression;
import ai.promoted.proto.event.TinyAction;
import ai.promoted.proto.event.TinyAttributedAction;
import ai.promoted.proto.event.TinyDeliveryLog;
import ai.promoted.proto.event.TinyImpression;
import ai.promoted.proto.event.TinyInsertion;
import ai.promoted.proto.event.TinyInsertionCore;
import ai.promoted.proto.event.TinyJoinedImpression;
import ai.promoted.proto.event.TinyTouchpoint;
import ai.promoted.proto.event.Touchpoint;
import ai.promoted.proto.event.UnionEvent;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.twitter.chill.protobuf.ProtobufSerializer;
import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests mainly for the underlying BaseInferred join implementation. */
public class MergeActionDetailsTest {
  private static final JoinedImpression IMPRESSION1 =
      createJoinedImpression("impression1", "insertion1");
  private static final JoinedImpression IMPRESSION2 =
      createJoinedImpression("impression2", "insertion2");

  private static final Action ACTION1 = createAction("action1");
  private static final Action ACTION2 = createAction("action2");

  private KeyedTwoInputStreamOperatorTestHarness<
          Tuple2<Long, String>, UnionEvent, TinyAttributedAction, AttributedAction>
      harness;
  private MergeActionDetails function;

  private static <T> void assertEmpty(List<T> list) {
    assertTrue(list.isEmpty(), () -> "List should be empty but was not, list=" + list);
  }

  private static JoinedImpression createJoinedImpression() {
    return createJoinedImpression("impression1", "insertion1");
  }

  private static JoinedImpression createJoinedImpression(String impressionId, String insertionId) {
    return JoinedImpression.newBuilder()
        .setIds(
            JoinedIdentifiers.newBuilder()
                .setViewId("view1")
                .setRequestId("request1")
                .setInsertionId(insertionId)
                .setImpressionId(impressionId))
        .setTiming(Timing.newBuilder().setEventApiTimestamp(200))
        .setRequest(Request.newBuilder().setTiming(Timing.newBuilder().setEventApiTimestamp(150)))
        .setHiddenApiRequest(
            HiddenApiRequest.newBuilder()
                .setRequestId("request3")
                .setTiming(Timing.newBuilder().setEventApiTimestamp(150))
                .setClientInfo(ClientInfo.getDefaultInstance())
                .build())
        // TODO - add HiddenApiRequest.
        .setResponse(Response.getDefaultInstance())
        .setResponseInsertion(Insertion.newBuilder().setContentId("content1"))
        .setImpression(
            Impression.newBuilder().setTiming(Timing.newBuilder().setEventApiTimestamp(200)))
        .build();
  }

  private static AttributedAction createExpectedAttributedAction() {
    return createExpectedAttributedActionBuilder().build();
  }

  private static AttributedAction.Builder createExpectedAttributedActionBuilder() {
    return AttributedAction.newBuilder()
        .setTouchpoint(Touchpoint.newBuilder().setJoinedImpression(createJoinedImpression()))
        .setAction(
            Action.newBuilder()
                .setTiming(Timing.newBuilder().setEventApiTimestamp(250).setProcessingTimestamp(1L))
                .setActionId("action1"))
        .setAttribution(
            Attribution.newBuilder().setModelId(AttributionModel.LATEST.id).setCreditMillis(1000));
  }

  private static TinyAttributedAction createInputTinyAttributedAction1() {
    return TinyAttributedAction.newBuilder()
        .setAttribution(
            Attribution.newBuilder().setModelId(AttributionModel.LATEST.id).setCreditMillis(1000))
        .setAction(TinyAction.newBuilder().setActionId("action1"))
        .setTouchpoint(
            TinyTouchpoint.newBuilder()
                .setJoinedImpression(
                    TinyJoinedImpression.newBuilder()
                        .setImpression(TinyImpression.newBuilder().setImpressionId("impression1"))
                        .setInsertion(
                            TinyInsertion.newBuilder()
                                .setRequestId("request1")
                                .setCore(
                                    TinyInsertionCore.newBuilder()
                                        .setInsertionId("insertion1")
                                        .setContentId("content1")))))
        .build();
  }

  private static UnionEvent toUnionEvent(JoinedImpression impression, long logTimestamp) {
    return UnionEvent.newBuilder()
        .setJoinedImpression(withLogTimestamp(impression, logTimestamp))
        .build();
  }

  private static UnionEvent toUnionEvent(Action action, long logTimestamp) {
    return UnionEvent.newBuilder().setAction(withLogTimestamp(action, logTimestamp)).build();
  }

  private static JoinedImpression withLogTimestamp(JoinedImpression impression, long logTimestamp) {
    JoinedImpression.Builder builder = impression.toBuilder();
    builder.getTimingBuilder().setEventApiTimestamp(logTimestamp);
    return builder.build();
  }

  private static Action createAction(String actionId) {
    return Action.newBuilder().setActionId(actionId).build();
  }

  private static Action withLogTimestamp(Action action, long logTimestamp) {
    Action.Builder builder = action.toBuilder();
    builder.getTimingBuilder().setEventApiTimestamp(logTimestamp);
    return builder.build();
  }

  @BeforeAll
  public static void globalSetUp() {
    TrackingUtil.processingTimeSupplier = () -> 1L;
  }

  @BeforeEach
  public void setUp() throws Exception {
    if (harness != null) {
      harness.close();
    }
    function =
        new MergeActionDetails(
            Duration.ofMillis(900),
            Duration.ofMillis(100),
            Duration.ofMillis(500),
            Duration.ofMillis(50),
            0L,
            DebugIds.empty());
    harness =
        new KeyedTwoInputStreamOperatorTestHarness<>(
            new KeyedCoProcessOperator<>(function),
            KeyUtil.unionEntityKeySelector,
            KeyUtil.tinyAttributedActionAnonUserIdKey,
            Types.TUPLE(Types.LONG, Types.STRING));
    ExecutionConfig config = harness.getExecutionConfig();
    config.registerTypeWithKryoSerializer(UnionEvent.class, ProtobufSerializer.class);
    config.registerTypeWithKryoSerializer(AttributedAction.class, ProtobufSerializer.class);
    config.registerTypeWithKryoSerializer(TinyAttributedAction.class, ProtobufSerializer.class);
    config.registerTypeWithKryoSerializer(TinyDeliveryLog.class, ProtobufSerializer.class);
    harness.setup();
    harness.open();
  }

  @AfterEach
  public void tearDown() throws Exception {
    harness.close();
    harness = null;
  }

  @Test
  public void fullAction() throws Exception {
    harness.processElement1(toUnionEvent(IMPRESSION1, 200), 200);
    harness.processElement1(toUnionEvent(ACTION1, 250), 250);

    harness.processBothWatermarks(new Watermark(300));

    harness.processElement2(createInputTinyAttributedAction1(), 300);

    harness.processBothWatermarks(new Watermark(350));

    assertEmptyIncompleteEventStates();
    assertEquals(1, Iterables.size(function.joinedImpressionMerger.idToJoinedImpression.entries()));
    assertEquals(1, Iterables.size(function.actionMerger.idToAction.entries()));

    assertIterableEquals(
        ImmutableList.of(new StreamRecord(createExpectedAttributedAction(), 300)),
        harness.extractOutputStreamRecords());

    harness.processBothWatermarks(new Watermark(1500));
    assertEquals(1, harness.extractOutputStreamRecords().size(), "Still 1 output");
    assertEmptyStates();
  }

  @Test
  public void fullAction_extraInput() throws Exception {
    harness.processElement1(toUnionEvent(IMPRESSION1, 200), 200);
    harness.processElement1(toUnionEvent(IMPRESSION2, 201), 201);
    harness.processElement1(toUnionEvent(ACTION1, 250), 250);
    harness.processElement1(toUnionEvent(ACTION2, 251), 251);

    harness.processBothWatermarks(new Watermark(300));

    harness.processElement2(createInputTinyAttributedAction1(), 300);

    harness.processBothWatermarks(new Watermark(350));

    assertEmptyIncompleteEventStates();
    assertEquals(2, Iterables.size(function.joinedImpressionMerger.idToJoinedImpression.entries()));
    assertEquals(2, Iterables.size(function.actionMerger.idToAction.entries()));

    assertIterableEquals(
        ImmutableList.of(new StreamRecord(createExpectedAttributedAction(), 300)),
        harness.extractOutputStreamRecords());

    harness.processBothWatermarks(new Watermark(1500));
    assertEquals(1, harness.extractOutputStreamRecords().size(), "Still 1 output");
    assertEmptyStates();
  }

  @Test
  public void fullAction_outOfOrder() throws Exception {
    harness.processElement2(createInputTinyAttributedAction1(), 100);

    assertEquals(1, Iterables.size(function.timeToIncompleteEvents.entries()));
    assertEquals(
        1, Iterables.size(function.actionMerger.actionIdToIncompleteEventTimers.entries()));
    assertEquals(0, Iterables.size(function.joinedImpressionMerger.idToJoinedImpression.entries()));
    assertEquals(0, Iterables.size(function.actionMerger.idToAction.entries()));

    harness.processElement1(toUnionEvent(IMPRESSION1, 200), 200);
    harness.processElement1(toUnionEvent(ACTION1, 250), 250);

    harness.processBothWatermarks(new Watermark(250));

    assertEquals(0, Iterables.size(function.timeToIncompleteEvents.entries()));
    assertEquals(
        0, Iterables.size(function.actionMerger.actionIdToIncompleteEventTimers.entries()));
    assertEquals(1, Iterables.size(function.joinedImpressionMerger.idToJoinedImpression.entries()));
    assertEquals(1, Iterables.size(function.actionMerger.idToAction.entries()));

    assertIterableEquals(
        ImmutableList.of(new StreamRecord(createExpectedAttributedAction(), 250)),
        harness.extractOutputStreamRecords());

    harness.processBothWatermarks(new Watermark(1500));
    assertEquals(1, harness.extractOutputStreamRecords().size(), "Still 1 output");
    assertEmptyStates();
  }

  @Test
  public void shoppingCart_simple() throws Exception {
    CartContent cartContent =
        CartContent.newBuilder().setContentId("content1").setQuantity(1).build();
    CartContent ignoredCartContent =
        CartContent.newBuilder().setContentId("ignoredContent1").setQuantity(1).build();
    Cart cart = Cart.newBuilder().addContents(ignoredCartContent).addContents(cartContent).build();
    assertShoppingCart(cart, createInputTinyAttributedAction1(), cartContent);
  }

  @Test
  public void shoppingCart_zeroQuantity() throws Exception {
    CartContent cartContent =
        CartContent.newBuilder().setContentId("content1").setQuantity(0).build();
    CartContent ignoredCartContent =
        CartContent.newBuilder().setContentId("ignoredContent1").setQuantity(1).build();
    Cart cart = Cart.newBuilder().addContents(ignoredCartContent).addContents(cartContent).build();
    CartContent expectedSingleCartContent = cartContent.toBuilder().setQuantity(1).build();
    assertShoppingCart(cart, createInputTinyAttributedAction1(), expectedSingleCartContent);
  }

  @Test
  public void shoppingCart_purchaseContentIdIsChild() throws Exception {
    Cart cart =
        Cart.newBuilder()
            .addContents(CartContent.newBuilder().setContentId("promotion1").setQuantity(2))
            .addContents(CartContent.newBuilder().setContentId("ignoredPromotion2").setQuantity(4))
            .build();
    CartContent expectedSingleCartContent =
        CartContent.newBuilder().setContentId("promotion1").setQuantity(2).build();
    TinyAttributedAction.Builder tinyActionBuilder = createInputTinyAttributedAction1().toBuilder();
    tinyActionBuilder.getActionBuilder().setContentId("promotion1");
    assertShoppingCart(cart, tinyActionBuilder.build(), expectedSingleCartContent);
  }

  public void assertShoppingCart(
      Cart inputCart, TinyAttributedAction tinyAction, CartContent expectedSingleCartContent)
      throws Exception {
    harness.processElement1(toUnionEvent(IMPRESSION1, 200), 200);
    Action purchase = ACTION1.toBuilder().setCart(inputCart).build();
    harness.processElement1(toUnionEvent(purchase, 250), 250);

    harness.processBothWatermarks(new Watermark(300));

    harness.processElement2(tinyAction, 300);

    harness.processBothWatermarks(new Watermark(350));

    AttributedAction.Builder expectedFlatActionBuilder = createExpectedAttributedActionBuilder();
    expectedFlatActionBuilder
        .getActionBuilder()
        .setCart(inputCart)
        .setSingleCartContent(expectedSingleCartContent);
    assertIterableEquals(
        ImmutableList.of(new StreamRecord(expectedFlatActionBuilder.build(), 300)),
        harness.extractOutputStreamRecords());

    harness.processBothWatermarks(new Watermark(1500));
    assertEquals(1, harness.extractOutputStreamRecords().size(), "Still 1 output");
    assertEmptyStates();
  }

  @Test
  public void noTinyAction() throws Exception {
    harness.processElement1(toUnionEvent(IMPRESSION1, 200), 200);
    harness.processElement1(toUnionEvent(ACTION1, 250), 250);

    harness.processBothWatermarks(new Watermark(350));

    assertEmptyIncompleteEventStates();
    assertEquals(1, Iterables.size(function.joinedImpressionMerger.idToJoinedImpression.entries()));
    assertEquals(1, Iterables.size(function.actionMerger.idToAction.entries()));

    harness.processBothWatermarks(new Watermark(1500));
    assertEmpty(harness.extractOutputStreamRecords());
    assertEmptyStates();
  }

  @Test
  public void noFullImpression() throws Exception {
    harness.processElement1(toUnionEvent(ACTION1, 250), 250);

    harness.processBothWatermarks(new Watermark(300));

    harness.processElement2(createInputTinyAttributedAction1(), 300);

    harness.processBothWatermarks(new Watermark(330));

    assertEquals(1, Iterables.size(function.timeToIncompleteEvents.entries()));
    assertEquals(
        1,
        Iterables.size(
            function.joinedImpressionMerger.impressionIdToIncompleteEventTimers.entries()));
    assertEquals(
        0, Iterables.size(function.actionMerger.actionIdToIncompleteEventTimers.entries()));
    assertEquals(0, Iterables.size(function.joinedImpressionMerger.idToJoinedImpression.entries()));
    assertEquals(1, Iterables.size(function.actionMerger.idToAction.entries()));

    harness.processBothWatermarks(new Watermark(1500));
    assertEmpty(harness.extractOutputStreamRecords());
    assertEmptyStates();
  }

  @Test
  public void noFullAction() throws Exception {
    harness.processElement1(toUnionEvent(IMPRESSION1, 200), 200);

    harness.processBothWatermarks(new Watermark(300));

    harness.processElement2(createInputTinyAttributedAction1(), 300);

    harness.processBothWatermarks(new Watermark(330));

    assertEquals(1, Iterables.size(function.timeToIncompleteEvents.entries()));
    assertEquals(
        0,
        Iterables.size(
            function.joinedImpressionMerger.impressionIdToIncompleteEventTimers.entries()));
    assertEquals(
        1, Iterables.size(function.actionMerger.actionIdToIncompleteEventTimers.entries()));
    assertEquals(1, Iterables.size(function.joinedImpressionMerger.idToJoinedImpression.entries()));
    assertEquals(0, Iterables.size(function.actionMerger.idToAction.entries()));

    harness.processBothWatermarks(new Watermark(1500));
    assertEmpty(harness.extractOutputStreamRecords());
    assertEmptyStates();
  }

  @Test
  public void hasRequiredEvents() {
    assertTrue(function.hasRequiredEvents(EnumSet.noneOf(MissingEvent.class)));
    assertFalse(function.hasRequiredEvents(EnumSet.of(MissingEvent.JOINED_IMPRESSION)));
    assertFalse(function.hasRequiredEvents(EnumSet.of(MissingEvent.ACTION)));
  }

  private void assertEmptyStates() throws Exception {
    assertEmptyIncompleteEventStates();
    assertEmptyDetailsStates();
  }

  private void assertEmptyIncompleteEventStates() throws Exception {
    assertEquals(0, Iterables.size(function.timeToIncompleteEvents.entries()));
    assertEquals(
        0,
        Iterables.size(
            function.joinedImpressionMerger.impressionIdToIncompleteEventTimers.entries()));
    assertEquals(
        0, Iterables.size(function.actionMerger.actionIdToIncompleteEventTimers.entries()));
  }

  private void assertEmptyDetailsStates() throws Exception {
    assertEquals(0, Iterables.size(function.joinedImpressionMerger.idToJoinedImpression.entries()));
    assertEquals(0, Iterables.size(function.actionMerger.idToAction.entries()));
  }
}
