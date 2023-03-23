package ai.promoted.metrics.logprocessor.common.table;

import ai.promoted.metrics.logprocessor.common.avro.FixedProtobufData;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.AvroToRowDataConverters;
import org.apache.flink.formats.avro.AvroToRowDataConverters.AvroToRowDataConverter;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.protobuf.PbFormatConfig;
import org.apache.flink.formats.protobuf.deserialize.PromotedProtoToRowConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

public class FlinkTableUtils {

  /** Register the given protobuf DataStream as a view in the table env. */
  public static <T extends GeneratedMessageV3> void registerProtoView(
      StreamTableEnvironment tableEnv, SingleOutputStreamOperator<T> input, String viewName) {
    // TODO Support rowtime
    Class<T> clazz = input.getType().getTypeClass();
    Schema avroSchema = FlinkTableUtils.protoToAvroSchema(clazz);
    DataType rowType = FlinkTableUtils.avroSchemaToDataType(avroSchema);
    org.apache.flink.table.api.Schema flinkSchema =
        org.apache.flink.table.api.Schema.newBuilder()
            .fromRowDataType(AvroSchemaConverter.convertToDataType(avroSchema.toString()))
            .build();
    SingleOutputStreamOperator<RowData> rowDataStream =
        FlinkTableUtils.protoToRowDataStream(input, (RowType) rowType.getLogicalType());
    registerView(tableEnv, rowDataStream, avroSchema, viewName);
  }

  /** Register the give RowData DataStream as a view in the table env. */
  public static void registerView(
      StreamTableEnvironment tableEnv,
      SingleOutputStreamOperator<RowData> input,
      Schema avroSchema,
      String viewName) {
    input.returns(InternalTypeInfo.of(avroSchemaToDataType(avroSchema).getLogicalType()));
    tableEnv.createTemporaryView(viewName, input);
  }

  public static <T extends GeneratedMessageV3>
      SingleOutputStreamOperator<RowData> protoToRowDataStream(
          DataStream<T> input, RowType rowType) {
    Class<T> clazz = input.getType().getTypeClass();
    SingleOutputStreamOperator<RowData> result =
        input.map(
            new RichMapFunction<>() {
              private PromotedProtoToRowConverter<T> converter;

              @Override
              public void open(Configuration parameters) throws Exception {
                converter =
                    new PromotedProtoToRowConverter<>(
                        rowType, new PbFormatConfig(clazz.getName(), false, false, ""));
              }

              @Override
              public RowData map(T t) throws Exception {
                return converter.convertProtoToRow(t);
              }
            });
    result.name(clazz.getSimpleName() + "-to-RowData");
    return result;
  }

  public static <T extends IndexedRecord> SingleOutputStreamOperator<RowData> avroToRowDataStream(
      DataStream<T> input, RowType rowType) {
    Class<T> clazz = input.getType().getTypeClass();
    SingleOutputStreamOperator<RowData> result =
        input.map(
            new RichMapFunction<>() {
              private AvroToRowDataConverter converter;

              @Override
              public void open(Configuration parameters) {
                converter = AvroToRowDataConverters.createRowConverter(rowType);
              }

              @Override
              public RowData map(T t) {
                return (RowData) converter.convert(t);
              }
            });
    result.name(clazz.getSimpleName() + "-to-RowData");
    return result;
  }

  public static <T extends IndexedRecord> Schema toAvroSchema(Class<T> clazz) {
    return SpecificData.get().getSchema(clazz);
  }

  public static <T extends GeneratedMessageV3> Schema protoToAvroSchema(Class<T> clazz) {
    Schema originalAvroSchema = FixedProtobufData.get().getSchema(clazz);
    // We convert to rowType first to flatten the schema
    RowType rowType =
        (RowType)
            AvroSchemaConverter.convertToDataType(originalAvroSchema.toString()).getLogicalType();
    return AvroSchemaConverter.convertToSchema(rowType);
  }

  public static DataType avroSchemaToDataType(Schema avroSchema) {
    return AvroSchemaConverter.convertToDataType(avroSchema.toString());
  }

  public static Schema logicalTypeToAvroSchema(LogicalType logicalType) {
    return AvroSchemaConverter.convertToSchema(logicalType);
  }
}
