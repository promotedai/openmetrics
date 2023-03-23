/*
 * This license block is from the original code at org.apache.flink.formats.csv.
 *
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

package org.apache.flink.formats.csv;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.formats.csv.PromotedRowDataToCsvConverters.PromotedRowDataToCsvConverter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

/**
 * This file is heavily modified from the original. We switched from Encoders to BulkWriter so this
 * writer can control when files are created. The scope of the {@code CsvRowDataBulkWriter} is one
 * writer per file.
 */
public final class CsvRowDataBulkWriter implements BulkWriter<RowData> {

  private static final long serialVersionUID = 1L;

  private final FSDataOutputStream stream;
  /** Logical row type describing the input CSV data. */
  private final RowType rowType;
  /** Runtime instance that performs the actual work. */
  private final PromotedRowDataToCsvConverter runtimeConverter;
  /** CsvMapper used to write {@link JsonNode} into bytes. */
  private final CsvMapper csvMapper;
  /** Schema describing the input CSV data. */
  private final CsvSchema csvSchema;
  /** Object writer used to write rows. It is configured by {@link CsvSchema}. */
  private final ObjectWriter objectWriter;
  /** This writer is used for the first row to include a header. */
  private final ObjectWriter firstObjectWriter;
  /** Reusable object node. */
  private final transient ObjectNode root;

  private final PromotedRowDataToCsvConverter.RowDataToCsvFormatConverterContext context;
  private boolean hasProcessedAnyElements;

  private CsvRowDataBulkWriter(FSDataOutputStream stream, RowType rowType, CsvSchema csvSchema) {
    this.stream = stream;
    this.hasProcessedAnyElements = false;
    this.rowType = rowType;
    this.runtimeConverter = PromotedRowDataToCsvConverters.createRowConverter(rowType);
    this.csvMapper = new CsvMapper();
    // Does not override the line separator.
    this.firstObjectWriter = csvMapper.writer(csvSchema.withHeader().withLineSeparator("\n"));
    this.csvSchema = csvSchema.withLineSeparator("\n");
    this.objectWriter = csvMapper.writer(this.csvSchema);
    this.root = csvMapper.createObjectNode();
    this.context =
        new PromotedRowDataToCsvConverter.RowDataToCsvFormatConverterContext(csvMapper, root);
  }

  // This function is heavily modified.
  @Override
  public void addElement(RowData row) {
    try {
      runtimeConverter.convert(row, context);
      if (hasProcessedAnyElements) {
        stream.write(objectWriter.writeValueAsBytes(root));
      } else {
        hasProcessedAnyElements = true;
        stream.write(firstObjectWriter.writeValueAsBytes(root));
      }
    } catch (Throwable t) {
      throw new RuntimeException(String.format("Could not serialize row '%s'.", row), t);
    }
  }

  @Override
  public void flush() throws IOException {
    stream.flush();
  }

  @Override
  public void finish() throws IOException {
    // Don't close the stream here.  The caller will close when ready.
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || o.getClass() != this.getClass()) {
      return false;
    }
    if (this == o) {
      return true;
    }
    final CsvRowDataBulkWriter that = (CsvRowDataBulkWriter) o;
    final CsvSchema otherSchema = that.csvSchema;

    return rowType.equals(that.rowType)
        && csvSchema.getColumnSeparator() == otherSchema.getColumnSeparator()
        && Arrays.equals(csvSchema.getLineSeparator(), otherSchema.getLineSeparator())
        && csvSchema.getArrayElementSeparator().equals(otherSchema.getArrayElementSeparator())
        && csvSchema.getQuoteChar() == otherSchema.getQuoteChar()
        && csvSchema.getEscapeChar() == otherSchema.getEscapeChar()
        && Arrays.equals(csvSchema.getNullValue(), otherSchema.getNullValue());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        rowType,
        csvSchema.getColumnSeparator(),
        Arrays.hashCode(csvSchema.getLineSeparator()),
        csvSchema.getArrayElementSeparator(),
        csvSchema.getQuoteChar(),
        csvSchema.getEscapeChar(),
        Arrays.hashCode(csvSchema.getNullValue()));
  }

  /** A builder for creating a {@link CsvRowDataBulkWriter}. */
  public static class Builder implements BulkWriter.Factory<RowData> {

    private final RowType rowType;
    private CsvSchema csvSchema;

    /**
     * Creates a {@link CsvRowDataBulkWriter} expecting the given {@link RowType}.
     *
     * @param rowType logical row type used to create schema.
     */
    public Builder(RowType rowType) {
      Preconditions.checkNotNull(rowType, "Row type must not be null.");

      this.rowType = rowType;
      this.csvSchema = CsvRowSchemaConverter.convert(rowType);
    }

    public Builder setFieldDelimiter(char c) {
      this.csvSchema = this.csvSchema.rebuild().setColumnSeparator(c).build();
      return this;
    }

    public Builder setArrayElementDelimiter(String delimiter) {
      Preconditions.checkNotNull(delimiter, "Delimiter must not be null.");
      this.csvSchema = this.csvSchema.rebuild().setArrayElementSeparator(delimiter).build();
      return this;
    }

    public Builder disableQuoteCharacter() {
      this.csvSchema = this.csvSchema.rebuild().disableQuoteChar().build();
      return this;
    }

    public Builder setQuoteCharacter(char c) {
      this.csvSchema = this.csvSchema.rebuild().setQuoteChar(c).build();
      return this;
    }

    public Builder setEscapeCharacter(char c) {
      this.csvSchema = this.csvSchema.rebuild().setEscapeChar(c).build();
      return this;
    }

    public Builder setNullLiteral(String s) {
      this.csvSchema = this.csvSchema.rebuild().setNullValue(s).build();
      return this;
    }

    @Override
    public BulkWriter<RowData> create(FSDataOutputStream stream) throws IOException {
      return new CsvRowDataBulkWriter(stream, rowType, csvSchema);
    }
  }
}
