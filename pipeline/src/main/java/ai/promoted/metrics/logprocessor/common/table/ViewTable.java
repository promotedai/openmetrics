package ai.promoted.metrics.logprocessor.common.table;

import ai.promoted.proto.event.View;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.types.Row;

public interface ViewTable {
  TypeInformation<Row> ROW_TYPE_INFORMATION =
      Types.ROW_NAMED(
          new String[] {"platform_id", "event_api_timestamp", "view_id", "name", "content_id"},
          Types.LONG,
          Types.LOCAL_DATE_TIME,
          Types.STRING,
          Types.STRING,
          Types.STRING);
  Schema SCHEMA =
      Schema.newBuilder()
          .columnByMetadata("rowtime", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), "rowtime")
          .watermark("rowtime", "SOURCE_WATERMARK()")
          .build();

  static Row toRow(View view) {
    return Row.of(
        view.getPlatformId(),
        LocalDateTime.ofEpochSecond(
            view.getTiming().getEventApiTimestamp() / 1000,
            (int) ((view.getTiming().getEventApiTimestamp() % 1000) * 1000000),
            ZoneOffset.UTC),
        view.getViewId(),
        view.getName(),
        view.getContentId());
  }
}
