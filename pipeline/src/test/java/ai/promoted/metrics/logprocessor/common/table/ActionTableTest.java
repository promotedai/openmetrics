package ai.promoted.metrics.logprocessor.common.table;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.promoted.proto.common.Money;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.Cart;
import ai.promoted.proto.event.CartContent;
import com.google.common.collect.ImmutableList;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

public class ActionTableTest {

  @Test
  public void toActionRow() {
    LocalDateTime eventApiTimestamp = LocalDateTime.of(2022, 11, 6, 13, 40, 10, 4000000);
    Action action =
        Action.newBuilder()
            .setPlatformId(1)
            .setTiming(
                Timing.newBuilder()
                    .setEventApiTimestamp(
                        eventApiTimestamp.toInstant(ZoneOffset.UTC).toEpochMilli()))
            .setActionId("act1")
            .setImpressionId("imp1")
            .setInsertionId("ins1")
            .setContentId("cont1")
            .setActionType(ActionType.NAVIGATE)
            .build();
    assertEquals(
        Row.of(1L, eventApiTimestamp, "act1", "imp1", "ins1", "cont1", 2),
        ActionTable.toActionRow(action));
  }

  @Test
  public void toActionCartContentRow_empty() {
    LocalDateTime eventApiTimestamp = LocalDateTime.of(2022, 11, 6, 13, 40, 10, 4000000);
    Action action =
        Action.newBuilder()
            .setPlatformId(1)
            .setTiming(
                Timing.newBuilder()
                    .setEventApiTimestamp(
                        eventApiTimestamp.toInstant(ZoneOffset.UTC).toEpochMilli()))
            .setActionId("act1")
            .setImpressionId("imp1")
            .setInsertionId("ins1")
            .setContentId("cont1")
            .setActionType(ActionType.CHECKOUT)
            .build();
    assertEquals(ImmutableList.of(), ActionTable.toActionCartContentRow(action));
  }

  @Test
  public void toActionCartContentRow() {
    LocalDateTime eventApiTimestamp = LocalDateTime.of(2022, 11, 6, 13, 40, 10, 4000000);
    Action action =
        Action.newBuilder()
            .setPlatformId(1)
            .setTiming(
                Timing.newBuilder()
                    .setEventApiTimestamp(
                        eventApiTimestamp.toInstant(ZoneOffset.UTC).toEpochMilli()))
            .setActionId("act1")
            .setImpressionId("imp1")
            .setInsertionId("ins1")
            .setActionType(ActionType.PURCHASE)
            .setCart(
                Cart.newBuilder()
                    .addContents(
                        CartContent.newBuilder()
                            .setContentId("cont1")
                            .setQuantity(2)
                            .setPricePerUnit(Money.newBuilder().setAmountMicros(2000000)))
                    .addContents(
                        CartContent.newBuilder()
                            .setContentId("cont2")
                            .setQuantity(4)
                            .setPricePerUnit(Money.newBuilder().setAmountMicros(500000))))
            .build();
    assertEquals(
        ImmutableList.of(
            Row.of(1L, eventApiTimestamp, "act1", "imp1", "ins1", "cont1", 3, 2L, 2000000L),
            Row.of(1L, eventApiTimestamp, "act1", "imp1", "ins1", "cont2", 3, 4L, 500000L)),
        ActionTable.toActionCartContentRow(action));
  }
}
