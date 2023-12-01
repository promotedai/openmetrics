package ai.promoted.metrics.logprocessor.common.table;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.promoted.metrics.logprocessor.common.functions.inferred.AttributionModel;
import ai.promoted.proto.common.Money;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.AttributedAction;
import ai.promoted.proto.event.Attribution;
import ai.promoted.proto.event.CartContent;
import ai.promoted.proto.event.JoinedIdentifiers;
import ai.promoted.proto.event.JoinedImpression;
import ai.promoted.proto.event.Touchpoint;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

public class AttributedActionTableTest {

  @Test
  public void toRow() {
    LocalDateTime eventApiTimestamp = LocalDateTime.of(2022, 11, 6, 13, 40, 10, 4000000);
    Action action =
        Action.newBuilder()
            .setPlatformId(1)
            .setTiming(
                Timing.newBuilder()
                    .setEventApiTimestamp(
                        eventApiTimestamp.toInstant(ZoneOffset.UTC).toEpochMilli()))
            .setActionId("act1")
            .setActionType(ActionType.PURCHASE)
            .setSingleCartContent(
                CartContent.newBuilder()
                    .setContentId("cont1")
                    .setQuantity(2)
                    .setPricePerUnit(Money.newBuilder().setAmountMicros(2000000)))
            .build();
    AttributedAction attributedAction =
        AttributedAction.newBuilder()
            .setAction(action)
            .setAttribution(
                Attribution.newBuilder().setModelId(AttributionModel.EVEN.id).setCreditMillis(1000))
            .setTouchpoint(
                Touchpoint.newBuilder()
                    .setJoinedImpression(
                        JoinedImpression.newBuilder()
                            .setIds(
                                JoinedIdentifiers.newBuilder()
                                    .setPlatformId(1L)
                                    .setImpressionId("imp1")
                                    .setInsertionId("ins1"))
                            .setRequest(Request.newBuilder().setSearchQuery("query1"))))
            .build();

    assertEquals(
        Row.of(
            1L, eventApiTimestamp, "act1", "imp1", "ins1", "cont1", 2L, "query1", 3, 2L, 2000000L),
        AttributedActionTable.toRow(attributedAction));
  }
}
