/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro;

import static org.apache.flink.formats.avro.AvroFormatOptions.AVRO_OUTPUT_CODEC;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.table.factories.BulkReaderFormatFactory;
import org.apache.flink.connector.file.table.factories.BulkWriterFormatFactory;
import org.apache.flink.connector.file.table.format.BulkDecodingFormat;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.format.ProjectableDecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

/**
 * Xingcan: Ported from
 * https://github.com/apache/flink/blob/1.16/flink-formats/flink-avro/src/main/java/org/apache/flink/formats/avro/AvroFileFormatFactory.java
 *
 * <p>Allow the user to provide an extra Avro schema in CREATE TABLE DDL.
 */
public class PromotedAvroFileFormatFactory
    implements BulkReaderFormatFactory, BulkWriterFormatFactory {
  // Xingcan: use this table option to set an Avro schema in CREATE TABLE DDL.
  public static final ConfigOption<String> AVRO_SCHEMA =
      ConfigOptions.key("promoted-avro.schema")
          .stringType()
          .noDefaultValue()
          .withDescription("The schema for Avro");
  public static final String IDENTIFIER = "promoted-avro";
  // End

  @Override
  public BulkDecodingFormat<RowData> createDecodingFormat(
      DynamicTableFactory.Context context, ReadableConfig formatOptions) {
    return new AvroBulkDecodingFormat();
  }

  @Override
  public EncodingFormat<BulkWriter.Factory<RowData>> createEncodingFormat(
      DynamicTableFactory.Context context, ReadableConfig formatOptions) {
    return new EncodingFormat<BulkWriter.Factory<RowData>>() {

      @Override
      public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
      }

      @Override
      public BulkWriter.Factory<RowData> createRuntimeEncoder(
          DynamicTableSink.Context context, DataType consumedDataType) {
        return new RowDataAvroWriterFactory(
            (RowType) consumedDataType.getLogicalType(), formatOptions.get(AVRO_OUTPUT_CODEC));
      }
    };
  }

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return new HashSet<>();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    Set<ConfigOption<?>> options = new HashSet<>();
    options.add(AVRO_OUTPUT_CODEC);
    options.add(AVRO_SCHEMA);
    return options;
  }

  @Override
  public Set<ConfigOption<?>> forwardOptions() {
    return optionalOptions();
  }

  /** Xingcan: Allow a format to accept an Avro schema. */
  public interface AvroSchemaAware {
    void setAvroSchema(String avroSchema);
  }
  // End

  private static class AvroBulkDecodingFormat
      implements BulkDecodingFormat<RowData>,
          ProjectableDecodingFormat<BulkFormat<RowData, FileSourceSplit>>,
          AvroSchemaAware {

    // Xingcan: use this variable to store an avro schema.
    private String avroSchema;

    @Override
    public void setAvroSchema(String avroSchema) {
      this.avroSchema = avroSchema;
    }
    // End

    @Override
    public BulkFormat<RowData, FileSourceSplit> createRuntimeDecoder(
        DynamicTableSource.Context context, DataType physicalDataType, int[][] projections) {
      // avro is a file format that keeps schemas in file headers,
      // if the schema given to the reader is not equal to the schema in header,
      // reader will automatically map the fields and give back records with our desired
      // schema
      //
      // for detailed discussion see comments in https://github.com/apache/flink/pull/18657
      // Xingcan: If an Avro schema is provided, use it. Otherwise, generate the schema from
      // DataType.
      if (null != avroSchema) {
        DataType avroDataType = AvroSchemaConverter.convertToDataType(avroSchema);
        Schema schema = new Schema.Parser().parse(avroSchema);
        DataType producedDataType = Projection.of(projections).project(avroDataType);
        // Xingcan: Apply projections to the given Avro schema.
        // TODO Need to modify this to support nested projection pushdown in the future.
        List<Schema.Field> projectedFields = new ArrayList<>(projections.length);
        for (int[] projection : projections) {
          Schema.Field oldField = schema.getFields().get(projection[0]);
          projectedFields.add(
              new Schema.Field(
                  oldField.name(),
                  oldField.schema(),
                  oldField.doc(),
                  oldField.defaultVal(),
                  oldField.order()));
        }
        Schema projectedSchema =
            Schema.createRecord(
                schema.getName(),
                schema.getDoc(),
                schema.getNamespace(),
                schema.isError(),
                projectedFields);

        return new AvroGenericRecordBulkFormat(
            context, (RowType) producedDataType.getLogicalType().copy(false), projectedSchema);
      } else {
        DataType producedDataType = Projection.of(projections).project(physicalDataType);
        return new AvroGenericRecordBulkFormat(
            context, (RowType) producedDataType.getLogicalType().copy(false));
      }
      // End
    }

    @Override
    public ChangelogMode getChangelogMode() {
      return ChangelogMode.insertOnly();
    }
  }

  private static class AvroGenericRecordBulkFormat
      extends AbstractAvroBulkFormat<GenericRecord, RowData, FileSourceSplit> {

    private static final long serialVersionUID = 1L;

    private final RowType producedRowType;
    private final TypeInformation<RowData> producedTypeInfo;

    public AvroGenericRecordBulkFormat(
        DynamicTableSource.Context context, RowType producedRowType) {
      super(AvroSchemaConverter.convertToSchema(producedRowType));
      this.producedRowType = producedRowType;
      this.producedTypeInfo = context.createTypeInformation(producedRowType);
    }

    /** Xingcan: use the provided avro schema */
    public AvroGenericRecordBulkFormat(
        DynamicTableSource.Context context, RowType producedRowType, Schema avroSchema) {
      super(avroSchema);
      this.producedRowType = producedRowType;
      this.producedTypeInfo = context.createTypeInformation(producedRowType);
    }
    // End
    @Override
    protected GenericRecord createReusedAvroRecord() {
      return new GenericData.Record(readerSchema);
    }

    @Override
    protected Function<GenericRecord, RowData> createConverter() {
      AvroToRowDataConverters.AvroToRowDataConverter converter =
          AvroToRowDataConverters.createRowConverter(producedRowType);
      return record -> {
        return record == null ? null : (GenericRowData) converter.convert(record);
      };
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
      return producedTypeInfo;
    }
  }

  /**
   * A {@link BulkWriter.Factory} to convert {@link RowData} to {@link GenericRecord} and wrap
   * {@link AvroWriterFactory}.
   */
  private static class RowDataAvroWriterFactory implements BulkWriter.Factory<RowData> {

    private static final long serialVersionUID = 1L;

    private final AvroWriterFactory<GenericRecord> factory;
    private final RowType rowType;

    private RowDataAvroWriterFactory(RowType rowType, String codec) {
      this.rowType = rowType;
      this.factory =
          new AvroWriterFactory<>(
              new AvroBuilder<GenericRecord>() {
                @Override
                public DataFileWriter<GenericRecord> createWriter(OutputStream out)
                    throws IOException {
                  Schema schema = AvroSchemaConverter.convertToSchema(rowType);
                  DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
                  DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);

                  if (codec != null) {
                    dataFileWriter.setCodec(CodecFactory.fromString(codec));
                  }
                  dataFileWriter.create(schema, out);
                  return dataFileWriter;
                }
              });
    }

    @Override
    public BulkWriter<RowData> create(FSDataOutputStream out) throws IOException {
      BulkWriter<GenericRecord> writer = factory.create(out);
      RowDataToAvroConverters.RowDataToAvroConverter converter =
          RowDataToAvroConverters.createConverter(rowType);
      Schema schema = AvroSchemaConverter.convertToSchema(rowType);
      return new BulkWriter<RowData>() {

        @Override
        public void addElement(RowData element) throws IOException {
          GenericRecord record = (GenericRecord) converter.convert(schema, element);
          writer.addElement(record);
        }

        @Override
        public void flush() throws IOException {
          writer.flush();
        }

        @Override
        public void finish() throws IOException {
          writer.finish();
        }
      };
    }
  }
}
