package ai.promoted.metrics.logprocessor.job.contentmetrics;

import ai.promoted.proto.event.Impression;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.types.Row;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public interface ImpressionTable {
    TypeInformation<Row> IMPRESSION_ROW = Types.ROW_NAMED(
            new String[]{"platform_id", "event_api_timestamp", "impression_id", "insertion_id", "content_id"},
            Types.LONG, Types.LOCAL_DATE_TIME, Types.STRING, Types.STRING, Types.STRING);
    Schema IMPRESSION_SCHEMA = Schema.newBuilder()
            .columnByMetadata("rowtime", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), "rowtime")
            .watermark("rowtime", "SOURCE_WATERMARK()")
            .build();

    static Row toImpressionRow(Impression impression) {
        return Row.of(
                impression.getPlatformId(),
                LocalDateTime.ofEpochSecond(
                        impression.getTiming().getEventApiTimestamp() / 1000,
                        (int) ((impression.getTiming().getEventApiTimestamp() % 1000) * 1000000),
                        ZoneOffset.UTC),
                impression.getImpressionId(),
                impression.getInsertionId(),
                impression.getContentId());
    }
}
