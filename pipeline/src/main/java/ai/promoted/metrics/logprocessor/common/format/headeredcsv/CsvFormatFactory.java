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

package ai.promoted.metrics.logprocessor.common.format.headeredcsv;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.BulkWriterFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.lang3.StringEscapeUtils;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static ai.promoted.metrics.logprocessor.common.format.headeredcsv.CsvFormatOptions.ALLOW_COMMENTS;
import static ai.promoted.metrics.logprocessor.common.format.headeredcsv.CsvFormatOptions.ARRAY_ELEMENT_DELIMITER;
import static ai.promoted.metrics.logprocessor.common.format.headeredcsv.CsvFormatOptions.DISABLE_QUOTE_CHARACTER;
import static ai.promoted.metrics.logprocessor.common.format.headeredcsv.CsvFormatOptions.ESCAPE_CHARACTER;
import static ai.promoted.metrics.logprocessor.common.format.headeredcsv.CsvFormatOptions.FIELD_DELIMITER;
import static ai.promoted.metrics.logprocessor.common.format.headeredcsv.CsvFormatOptions.IGNORE_PARSE_ERRORS;
import static ai.promoted.metrics.logprocessor.common.format.headeredcsv.CsvFormatOptions.NULL_LITERAL;
import static ai.promoted.metrics.logprocessor.common.format.headeredcsv.CsvFormatOptions.QUOTE_CHARACTER;

/**
 * Format factory for providing configured instances of CSV to RowData {@link SerializationSchema}
 * and {@link DeserializationSchema}.
 */
public final class CsvFormatFactory implements BulkWriterFormatFactory {

    // TODO - rename or rename package.
    public static final String IDENTIFIER = "headered-csv";

    @Override
    public EncodingFormat<BulkWriter.Factory<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        validateFormatOptions(formatOptions);

        return new EncodingFormat<BulkWriter.Factory<RowData>>() {
            @Override
            public BulkWriter.Factory<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context context, DataType consumedDataType) {
                final RowType rowType = (RowType) consumedDataType.getLogicalType();
                final CsvRowDataBulkWriter.Builder schemaBuilder =
                        new CsvRowDataBulkWriter.Builder(rowType);
                configureSerializationSchema(formatOptions, schemaBuilder);
                return schemaBuilder;
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FIELD_DELIMITER);
        options.add(DISABLE_QUOTE_CHARACTER);
        options.add(QUOTE_CHARACTER);
        options.add(ALLOW_COMMENTS);
        options.add(IGNORE_PARSE_ERRORS);
        options.add(ARRAY_ELEMENT_DELIMITER);
        options.add(ESCAPE_CHARACTER);
        options.add(NULL_LITERAL);
        return options;
    }

    // ------------------------------------------------------------------------
    //  Validation
    // ------------------------------------------------------------------------

    static void validateFormatOptions(ReadableConfig tableOptions) {
        final boolean hasQuoteCharacter = tableOptions.getOptional(QUOTE_CHARACTER).isPresent();
        final boolean isDisabledQuoteCharacter = tableOptions.get(DISABLE_QUOTE_CHARACTER);
        if (isDisabledQuoteCharacter && hasQuoteCharacter) {
            throw new ValidationException(
                    "Format cannot define a quote character and disabled quote character at the same time.");
        }
        // Validate the option value must be a single char.
        validateCharacterVal(tableOptions, FIELD_DELIMITER, true);
        validateCharacterVal(tableOptions, ARRAY_ELEMENT_DELIMITER);
        validateCharacterVal(tableOptions, QUOTE_CHARACTER);
        validateCharacterVal(tableOptions, ESCAPE_CHARACTER);
    }

    /** Validates the option {@code option} value must be a Character. */
    private static void validateCharacterVal(
            ReadableConfig tableOptions, ConfigOption<String> option) {
        validateCharacterVal(tableOptions, option, false);
    }

    /**
     * Validates the option {@code option} value must be a Character.
     *
     * @param tableOptions the table options
     * @param option the config option
     * @param unescape whether to unescape the option value
     */
    private static void validateCharacterVal(
            ReadableConfig tableOptions, ConfigOption<String> option, boolean unescape) {
        if (tableOptions.getOptional(option).isPresent()) {
            final String value =
                    unescape
                            ? StringEscapeUtils.unescapeJava(tableOptions.get(option))
                            : tableOptions.get(option);
            if (value.length() != 1) {
                throw new ValidationException(
                        String.format(
                                "Option '%s.%s' must be a string with single character, but was: %s",
                                IDENTIFIER, option.key(), tableOptions.get(option)));
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private static void configureSerializationSchema(
            ReadableConfig formatOptions, CsvRowDataBulkWriter.Builder schemaBuilder) {
        formatOptions
                .getOptional(FIELD_DELIMITER)
                .map(delimiter -> StringEscapeUtils.unescapeJava(delimiter).charAt(0))
                .ifPresent(schemaBuilder::setFieldDelimiter);

        if (formatOptions.get(DISABLE_QUOTE_CHARACTER)) {
            schemaBuilder.disableQuoteCharacter();
        } else {
            formatOptions
                    .getOptional(QUOTE_CHARACTER)
                    .map(quote -> quote.charAt(0))
                    .ifPresent(schemaBuilder::setQuoteCharacter);
        }

        formatOptions
                .getOptional(ARRAY_ELEMENT_DELIMITER)
                .ifPresent(schemaBuilder::setArrayElementDelimiter);

        formatOptions
                .getOptional(ESCAPE_CHARACTER)
                .map(escape -> escape.charAt(0))
                .ifPresent(schemaBuilder::setEscapeCharacter);

        formatOptions.getOptional(NULL_LITERAL).ifPresent(schemaBuilder::setNullLiteral);
    }
}
