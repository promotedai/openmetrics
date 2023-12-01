package ai.promoted.metrics.logprocessor.common.table;

import ai.promoted.metrics.logprocessor.common.util.FlatUtil;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.AttributedAction;
import ai.promoted.proto.event.JoinedImpression;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.types.Row;

public interface AttributedActionTable {
  TypeInformation<Row> ROW_TYPE_INFORMATION =
      Types.ROW_NAMED(
          new String[] {
            "platform_id",
            "event_api_timestamp",
            "action_id",
            "impression_id",
            "insertion_id",
            "content_id",
            "model_id",
            "search_query",
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
          Types.LONG,
          Types.STRING,
          Types.INT,
          Types.LONG,
          Types.LONG);
  Schema SCHEMA =
      Schema.newBuilder()
          .columnByMetadata("rowtime", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), "rowtime")
          .watermark("rowtime", "SOURCE_WATERMARK()")
          .build();

  // The join action table already splits the actions per cart item.
  static Row toRow(AttributedAction attributedAction) {
    Action action = attributedAction.getAction();
    JoinedImpression joinedImpression = attributedAction.getTouchpoint().getJoinedImpression();
    boolean hasCartContent = attributedAction.getAction().hasSingleCartContent();
    String contentId = FlatUtil.getContentIdPreferAction(attributedAction);

    long quantity = hasCartContent ? action.getSingleCartContent().getQuantity() : 1L;
    if (quantity == 0L) {
      quantity = 1L;
    }
    long priceUsdMicrosPerUnit =
        hasCartContent ? action.getSingleCartContent().getPricePerUnit().getAmountMicros() : 0L;

    return Row.of(
        action.getPlatformId(),
        LocalDateTime.ofEpochSecond(
            action.getTiming().getEventApiTimestamp() / 1000,
            (int) ((action.getTiming().getEventApiTimestamp() % 1000) * 1000000),
            ZoneOffset.UTC),
        action.getActionId(),
        joinedImpression.getIds().getImpressionId(),
        joinedImpression.getIds().getInsertionId(),
        contentId,
        attributedAction.getAttribution().getModelId(),
        joinedImpression.getRequest().getSearchQuery(),
        action.getActionType().getNumber(),
        quantity,
        priceUsdMicrosPerUnit);
  }
}
