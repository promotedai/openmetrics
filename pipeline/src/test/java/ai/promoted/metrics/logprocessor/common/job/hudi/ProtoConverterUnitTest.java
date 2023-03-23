package ai.promoted.metrics.logprocessor.common.job.hudi;

import ai.promoted.metrics.logprocessor.common.avro.FixedProtobufData;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.AutoView;
import ai.promoted.proto.event.CohortMembership;
import ai.promoted.proto.event.Diagnostics;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.User;
import ai.promoted.proto.event.View;
import org.apache.avro.Schema;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.protobuf.PbFormatConfig;
import org.apache.flink.formats.protobuf.deserialize.PromotedProtoToRowConverter;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ProtoConverterUnitTest {
  //  @Disabled("Fix the code-gen issue")
  @ParameterizedTest
  @ValueSource(
      classes = {
        User.class,
        CohortMembership.class,
        View.class,
        AutoView.class,
        DeliveryLog.class,
        Impression.class,
        Action.class,
        Diagnostics.class,
        // LogRequest.class TODO Add this back when we solve the 64k issue
      })
  void testProtoTypeConverterCodeGen(Class<?> protoClass) throws Exception {
    Schema originalAvroSchema = FixedProtobufData.get().getSchema(protoClass);
    RowType rowType =
        (RowType)
            AvroSchemaConverter.convertToDataType(originalAvroSchema.toString()).getLogicalType();
    PromotedProtoToRowConverter<?> converter =
        new PromotedProtoToRowConverter<>(
            rowType, new PbFormatConfig(protoClass.getName(), false, false, ""));
    Assertions.assertNotNull(converter);
  }
}
