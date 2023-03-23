package ai.promoted.metrics.logprocessor.common.table;

import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.CartContent;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.types.Row;

public interface ActionTable {
  TypeInformation<Row> ACTION_ROW =
      Types.ROW_NAMED(
          new String[] {
            "platform_id",
            "event_api_timestamp",
            "action_id",
            "impression_id",
            "insertion_id",
            "content_id",
            "action_type"
          },
          Types.LONG,
          Types.LOCAL_DATE_TIME,
          Types.STRING,
          Types.STRING,
          Types.STRING,
          Types.STRING,
          Types.INT);
  Schema ACTION_SCHEMA =
      Schema.newBuilder()
          .columnByMetadata("rowtime", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), "rowtime")
          .watermark("rowtime", "SOURCE_WATERMARK()")
          .build();
  TypeInformation<Row> ACTION_CART_CONTENT_ROW =
      Types.ROW_NAMED(
          new String[] {
            "platform_id",
            "event_api_timestamp",
            "action_id",
            "impression_id",
            "insertion_id",
            "content_id",
            "action_type",
            "quantity",
            "price_usd_micros_per_unit"
          },
          Types.LONG,
          Types.LOCAL_DATE_TIME,
          Types.STRING,
          Types.STRING,
          Types.STRING,
          Types.STRING,
          Types.INT,
          Types.LONG,
          Types.LONG);
  Schema ACTION_CART_CONTENT_SCHEMA =
      Schema.newBuilder()
          .columnByMetadata("rowtime", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), "rowtime")
          .watermark("rowtime", "SOURCE_WATERMARK()")
          .build();

  static Row toActionRow(Action action) {
    return Row.of(
        action.getPlatformId(),
        LocalDateTime.ofEpochSecond(
            action.getTiming().getEventApiTimestamp() / 1000,
            (int) ((action.getTiming().getEventApiTimestamp() % 1000) * 1000000),
            ZoneOffset.UTC),
        action.getActionId(),
        action.getImpressionId(),
        action.getInsertionId(),
        action.getContentId(),
        action.getActionType().getNumber());
  }

  static List<Row> toActionCartContentRow(Action action) {
    List<CartContent> cartContents = action.getCart().getContentsList();
    return cartContents.stream()
        .map(
            cartContent ->
                Row.of(
                    action.getPlatformId(),
                    LocalDateTime.ofEpochSecond(
                        action.getTiming().getEventApiTimestamp() / 1000,
                        (int) ((action.getTiming().getEventApiTimestamp() % 1000) * 1000000),
                        ZoneOffset.UTC),
                    action.getActionId(),
                    action.getImpressionId(),
                    action.getInsertionId(),
                    cartContent.getContentId(),
                    action.getActionType().getNumber(),
                    cartContent.getQuantity(),
                    cartContent.getPricePerUnit().getAmountMicros()))
        .collect(Collectors.toList());
  }
}
