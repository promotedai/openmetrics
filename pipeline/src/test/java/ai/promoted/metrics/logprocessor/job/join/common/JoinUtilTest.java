package ai.promoted.metrics.logprocessor.job.join.common;

import static com.google.common.truth.Truth.assertThat;

import ai.promoted.metrics.logprocessor.common.table.FlinkTableUtils;
import ai.promoted.proto.event.TinyAction;
import org.apache.avro.Schema;
import org.apache.flink.table.types.DataType;
import org.junit.jupiter.api.Test;

public class JoinUtilTest {
  @Test
  public void toSelectFieldSql() {
    Schema schema = FlinkTableUtils.protoToAvroSchema(TinyAction.class);
    DataType rowType = FlinkTableUtils.avroSchemaToDataType(schema);

    assertThat(JoinUtil.toSelectFieldSql(rowType, ""))
        .isEqualTo(
            "common, view_id, request_id, paging_id, insertion_id, impression_id, "
                + "action_id, action_type, custom_action_type, content_id, other_content_ids");

    assertThat(JoinUtil.toSelectFieldSql(rowType, "action."))
        .isEqualTo(
            "action.common, action.view_id, action.request_id, action.paging_id, "
                + "action.insertion_id, action.impression_id, action.action_id, "
                + "action.action_type, action.custom_action_type, action.content_id, "
                + "action.other_content_ids");
  }

  @Test
  public void getSchemaSql() {
    Schema schema = FlinkTableUtils.protoToAvroSchema(TinyAction.class);
    DataType rowType = FlinkTableUtils.avroSchemaToDataType(schema);

    assertThat(JoinUtil.getSchemaSql(rowType))
        .isEqualTo(
            "ROW<`common` ROW<`platform_id` BIGINT, `anon_user_id` STRING, "
                + "`event_api_timestamp` BIGINT>, `view_id` STRING, `request_id` STRING, "
                + "`paging_id` STRING, `insertion_id` STRING, `impression_id` STRING, "
                + "`action_id` STRING, `action_type` STRING, `custom_action_type` STRING, "
                + "`content_id` STRING, `other_content_ids` ARRAY<ROW<`key` INT, "
                + "`value` STRING> NOT NULL>>");
  }
}
