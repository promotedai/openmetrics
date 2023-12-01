package ai.promoted.metrics.logprocessor.common.job;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

import ai.promoted.metrics.logprocessor.common.avro.PromotedProtobufData;
import ai.promoted.proto.common.Struct1;
import ai.promoted.proto.common.Struct2;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.common.Value1;
import ai.promoted.proto.common.Value2;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.AttributedAction;
import ai.promoted.proto.event.AutoView;
import ai.promoted.proto.event.CohortMembership;
import ai.promoted.proto.event.Diagnostics;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.JoinedIdentifiers;
import ai.promoted.proto.event.JoinedImpression;
import ai.promoted.proto.event.Touchpoint;
import ai.promoted.proto.event.User;
import ai.promoted.proto.event.View;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.protobuf.PbFormatConfig;
import org.apache.flink.formats.protobuf.deserialize.PromotedProtoToRowConverter;
import org.apache.flink.formats.protobuf.serialize.PromotedRowToProtoConverter;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ProtoConverterUnitTest {
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
        AttributedAction.class,
        JoinedImpression.class,
        // LogRequest.class TODO Add this back when we solve the 64k issue
      })
  void testProtoTypeConverterCodeGen(Class<?> protoClass) throws Exception {
    Schema originalAvroSchema = PromotedProtobufData.get().getSchema(protoClass);
    RowType rowType =
        (RowType)
            AvroSchemaConverter.convertToDataType(originalAvroSchema.toString()).getLogicalType();
    PromotedProtoToRowConverter<?> converter =
        new PromotedProtoToRowConverter<>(
            rowType, new PbFormatConfig(protoClass.getName(), false, false, ""));
    Assertions.assertNotNull(converter);
  }

  @Test
  void testConvertingJoinedImpression() throws Exception {
    Schema schema = PromotedProtobufData.get().getSchema(JoinedImpression.class);
    RowType rowType =
        (RowType) AvroSchemaConverter.convertToDataType(schema.toString()).getLogicalType();
    PbFormatConfig config = new PbFormatConfig(JoinedImpression.class.getName(), false, false, "");

    // From Proto to RowData
    PromotedProtoToRowConverter<JoinedImpression> converter =
        new PromotedProtoToRowConverter<>(rowType, config);
    JoinedImpression joinedImpression = createTestJoinedImpression();
    RowData rowResult = converter.convertProtoToRow(joinedImpression);
    GenericRowData expectedRowResult = createExpectedJoinedImpressionGenericRowData();
    assertGenericRowDataEqual(expectedRowResult, (GenericRowData) rowResult);

    // From RowData to Proto
    PromotedRowToProtoConverter<JoinedImpression> rowToProtoConverter =
        new PromotedRowToProtoConverter<>(rowType, config, true);
    JoinedImpression joinedImpressionResult = rowToProtoConverter.parseFromRowData(rowResult);
    assertThat(joinedImpressionResult).isEqualTo(joinedImpression);
  }

  @Test
  void testConvertingStruct1() throws Exception {
    Schema schema = PromotedProtobufData.get().getSchema(Struct1.class);
    RowType rowType =
        (RowType) AvroSchemaConverter.convertToDataType(schema.toString()).getLogicalType();
    PbFormatConfig config = new PbFormatConfig(Struct1.class.getName(), false, false, "");

    // From Proto to RowData
    PromotedProtoToRowConverter<Struct1> converter =
        new PromotedProtoToRowConverter<>(rowType, config);
    Struct1 struct1 =
        Struct1.newBuilder()
            .putAllFields(
                Map.of(
                    "key1",
                    Value1.newBuilder()
                        .setStructValue(
                            Struct2.newBuilder()
                                .putFields(
                                    "key2", Value2.newBuilder().setStringValue("value2").build())
                                .build())
                        .build()))
            .build();
    RowData rowResult = converter.convertProtoToRow(struct1);
    GenericRowData expectedRowData = new GenericRowData(1);
    expectedRowData.setField(
        0,
        new GenericArrayData(
            new GenericRowData[] {
              GenericRowData.of(
                  BinaryStringData.fromString("key1"),
                  GenericRowData.of(
                      null,
                      null,
                      null,
                      null,
                      GenericRowData.of(
                          new GenericArrayData(
                              new GenericRowData[] {
                                GenericRowData.of(
                                    BinaryStringData.fromString("key2"),
                                    GenericRowData.of(
                                        null,
                                        null,
                                        BinaryStringData.fromString("value2"),
                                        null,
                                        null,
                                        null))
                              })),
                      null)),
            }));
    assertGenericRowDataEqual(expectedRowData, (GenericRowData) rowResult);

    // From RowData to Proto
    PromotedRowToProtoConverter<Struct1> rowToProtoConverter =
        new PromotedRowToProtoConverter<>(rowType, config, false);
    Struct1 struct1Result = rowToProtoConverter.parseFromRowData(rowResult);
    assertThat(struct1Result).isEqualTo(struct1);
  }

  private GenericRowData createExpectedJoinedImpressionGenericRowData() {
    GenericRowData expected = new GenericRowData(11);
    // `JoinedImpression.ids`.
    expected.setField(
        0,
        GenericRowData.of(
            1L,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            new BinaryStringData("impression-0")));
    // `JoinedImpression.timing`.
    expected.setField(1, GenericRowData.of(null, 1L, null, null));
    // `JoinedImpression.response_insertion`.
    expected.setField(
        6,
        GenericRowData.of(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            new BinaryStringData("content-1"),
            5L,
            null,
            null,
            null,
            null,
            null,
            null,
            null));
    return expected;
  }

  private JoinedImpression createTestJoinedImpression() {
    return JoinedImpression.newBuilder()
        .setIds(
            JoinedIdentifiers.newBuilder()
                .setPlatformId(1L)
                .setImpressionId("impression-0")
                .build())
        .setTiming(Timing.newBuilder().setEventApiTimestamp(1))
        .setResponseInsertion(Insertion.newBuilder().setContentId("content-1").setPosition(5))
        .build();
  }

  @Test
  void testConvertingAttributedAction() throws Exception {
    Schema schema = PromotedProtobufData.get().getSchema(AttributedAction.class);
    RowType rowType =
        (RowType) AvroSchemaConverter.convertToDataType(schema.toString()).getLogicalType();
    PromotedProtoToRowConverter<AttributedAction> converter =
        new PromotedProtoToRowConverter<>(
            rowType, new PbFormatConfig(AttributedAction.class.getName(), false, false, ""));
    AttributedAction attributedEvent =
        AttributedAction.newBuilder()
            .setAction(Action.newBuilder().setActionId("action-0"))
            .setTouchpoint(
                Touchpoint.newBuilder().setJoinedImpression(createTestJoinedImpression()))
            .build();
    RowData result = converter.convertProtoToRow(attributedEvent);
    GenericRowData expected = new GenericRowData(3);
    expected.setField(
        0,
        GenericRowData.of(
            null,
            null,
            null,
            null,
            new BinaryStringData("action-0"),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null));
    expected.setField(2, GenericRowData.of(createExpectedJoinedImpressionGenericRowData()));
    assertGenericRowDataEqual(expected, (GenericRowData) result);
  }

  private void assertGenericRowDataEqual(GenericRowData expected, GenericRowData actual) {
    assertGenericRowDataEqual(expected, actual, "");
  }

  private void assertGenericRowDataEqual(
      GenericRowData expected, GenericRowData actual, String path) {
    assertWithMessage("Unexpected size of row, path=%s", path)
        .that(actual.getArity())
        .isEqualTo(expected.getArity());

    for (int i = 0; i < actual.getArity(); i++) {
      String newPath = path + "." + i;

      Object actualValue = actual.getField(i);
      Object expectedValue = expected.getField(i);
      assertWithMessage("Unexpected type of row data, path=%s", newPath)
          .that(nullableGetClass(actualValue))
          .isEqualTo(nullableGetClass(expectedValue));

      if (actualValue instanceof GenericRowData) {
        assertGenericRowDataEqual(
            (GenericRowData) expectedValue, (GenericRowData) actualValue, newPath);
      } else {
        assertWithMessage("Unexpected row data at path=%s", newPath)
            .that(actualValue)
            .isEqualTo(expectedValue);
      }
    }
  }

  private Class nullableGetClass(Object obj) {
    return obj == null ? null : obj.getClass();
  }
}
