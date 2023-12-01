package ai.promoted.metrics.logprocessor.common.table;

import static org.apache.flink.table.api.DataTypes.TIMESTAMP_LTZ;

import ai.promoted.metrics.logprocessor.common.avro.PromotedProtobufData;
import com.google.protobuf.GeneratedMessageV3;
import java.util.List;
import javax.annotation.Nullable;
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
import org.apache.flink.formats.protobuf.serialize.PromotedRowToProtoConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FlinkTableUtils {

  /** Register the given protobuf DataStream as a view in the table env. */
  public static <T extends GeneratedMessageV3> void registerProtoView(
      StreamTableEnvironment tableEnv, DataStream<T> input, String viewName) {
    registerProtoView(tableEnv, input, viewName, true);
  }

  public static <T extends GeneratedMessageV3> void registerProtoView(
      StreamTableEnvironment tableEnv,
      DataStream<T> input,
      String viewName,
      boolean withSourceWatermark) {
    Class<T> clazz = input.getType().getTypeClass();
    Schema avroSchema = FlinkTableUtils.protoToAvroSchema(clazz);
    DataType rowType = FlinkTableUtils.avroSchemaToDataType(avroSchema);
    SingleOutputStreamOperator<RowData> rowDataStream =
        FlinkTableUtils.protoToRowDataStream(input, (RowType) rowType.getLogicalType());
    registerView(tableEnv, rowDataStream, avroSchema, viewName, withSourceWatermark);
  }

  public static <T extends IndexedRecord> void registerAvroView(
      StreamTableEnvironment tableEnv, SingleOutputStreamOperator<T> input, String viewName) {
    registerAvroView(tableEnv, input, viewName, true);
  }

  public static <T extends IndexedRecord> void registerAvroView(
      StreamTableEnvironment tableEnv,
      SingleOutputStreamOperator<T> input,
      String viewName,
      boolean withSourceWatermark) {
    Class<T> clazz = input.getType().getTypeClass();
    Schema avroSchema = FlinkTableUtils.toAvroSchema(clazz);
    DataType rowType = FlinkTableUtils.avroSchemaToDataType(avroSchema);
    SingleOutputStreamOperator<RowData> rowDataStream =
        FlinkTableUtils.avroToRowDataStream(input, (RowType) rowType.getLogicalType());
    registerView(tableEnv, rowDataStream, avroSchema, viewName, withSourceWatermark);
  }

  public static void registerView(
      StreamTableEnvironment tableEnv,
      SingleOutputStreamOperator<RowData> input,
      Schema avroSchema,
      String viewName) {
    registerView(tableEnv, input, avroSchema, viewName, true);
  }

  /** Register the give RowData DataStream as a view in the table env. */
  public static void registerView(
      StreamTableEnvironment tableEnv,
      SingleOutputStreamOperator<RowData> input,
      Schema avroSchema,
      String viewName,
      boolean withSourceWatermark) {
    input.returns(InternalTypeInfo.of(avroSchemaToDataType(avroSchema).getLogicalType()));
    org.apache.flink.table.api.Schema.Builder builder =
        org.apache.flink.table.api.Schema.newBuilder()
            .fromRowDataType(AvroSchemaConverter.convertToDataType(avroSchema.toString()));
    if (withSourceWatermark) {
      builder
          .columnByMetadata("rowtime", TIMESTAMP_LTZ(3))
          .watermark("rowtime", "SOURCE_WATERMARK()");
    }
    org.apache.flink.table.api.Schema flinkSchema = builder.build();
    tableEnv.createTemporaryView(viewName, input, flinkSchema);
  }

  public static <T extends GeneratedMessageV3>
      SingleOutputStreamOperator<RowData> protoToRowDataStream(
          DataStream<T> input, RowType rowType) {
    Class<T> clazz = input.getType().getTypeClass();
    SingleOutputStreamOperator<RowData> result =
        input.map(
            new RichMapFunction<>() {
              private final Logger LOGGER = LogManager.getLogger(this.getClass());
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
    result.name(clazz.getSimpleName() + "-to-RowData").setParallelism(input.getParallelism());
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
    result.name(clazz.getSimpleName() + "-to-RowData").setParallelism(input.getParallelism());
    return result;
  }

  public static <T extends GeneratedMessageV3>
      SingleOutputStreamOperator<T> toProtoStreamWithRowtime(
          StreamTableEnvironment tEnv, Table table, Class<T> protoClass, @Nullable String opUid) {
    Schema avroSchema = FlinkTableUtils.protoToAvroSchema(protoClass);
    DataType dataType = FlinkTableUtils.avroSchemaToDataType(avroSchema);
    // We append the rowtime column to the end of the schema fields just for preserving the rowtime
    // and watermark.
    // The field will be ignored when converting the RowData to the Proto event.
    DataType dataTypeWithRowtime =
        DataTypeUtils.appendRowFields(
            dataType, List.of(DataTypes.FIELD("rowtime", DataTypes.TIMESTAMP_LTZ(3))));
    DataStream<RowData> rowDataStream =
        tEnv.toDataStream(
            table,
            DataTypes.of(InternalTypeInfo.of((RowType) dataTypeWithRowtime.getLogicalType())));
    SingleOutputStreamOperator<T> result =
        rowDataStream
            .<T>map(
                new RichMapFunction<>() {
                  PromotedRowToProtoConverter<T> converter;

                  @Override
                  public void open(Configuration parameters) throws Exception {
                    converter =
                        new PromotedRowToProtoConverter<>(
                            (RowType) dataTypeWithRowtime.getLogicalType(),
                            new PbFormatConfig(protoClass.getName(), false, false, ""));
                    super.open(parameters);
                  }

                  @Override
                  public T map(RowData rowData) throws Exception {
                    return converter.parseFromRowData(rowData);
                  }
                })
            .returns(protoClass);
    if (null != opUid) {
      result.uid(opUid).name(opUid);
    }
    return result;
  }

  public static <T extends IndexedRecord> Schema toAvroSchema(Class<T> clazz) {
    return SpecificData.get().getSchema(clazz);
  }

  public static <T extends GeneratedMessageV3> Schema protoToAvroSchema(Class<T> clazz) {
    Schema originalAvroSchema = PromotedProtobufData.get().getSchema(clazz);
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
