package ai.promoted.metrics.logprocessor.common.testing;

import ai.promoted.metrics.logprocessor.common.avro.FixedProtobufData;
import ai.promoted.metrics.logprocessor.common.avro.FixedProtobufDatumWriter;
import ai.promoted.metrics.logprocessor.common.avro.PromotedProtobufData;
import ai.promoted.metrics.logprocessor.common.avro.PromotedProtobufDatumWriter;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utilities for converting from Protos to Avro.
 * This is in the testing directory because it's not very efficient.
 */
public class AvroProtoUtils {

    /**
     * Writes proto messages to Avro file.
     */
    public static <T extends GeneratedMessageV3> void writeAvroFile(File file, Iterable<T> messages) throws IOException {
        if (!messages.iterator().hasNext()) {
            throw new IllegalArgumentException("Expected at least one message");
        }
        Class<T> clazz = (Class<T>) messages.iterator().next().getClass();
        Schema schema = PromotedProtobufData.get().getSchema(clazz);
        PromotedProtobufDatumWriter<T> pbWriter = new PromotedProtobufDatumWriter<T>(schema);
        try (DataFileWriter<T> dataFileWriter = new DataFileWriter<T>(pbWriter)) {
            dataFileWriter.create(schema, file);
            for (T message : messages) {
                dataFileWriter.append(message);
            }
        }
    }

    /**
     * Converts a proto message to Avro GenericRecord.
     */
    public static <T extends GeneratedMessageV3> GenericRecord toAvroGenericRecord(T message) throws IOException {
        return toAvroGenericRecords(ImmutableList.of(message)).iterator().next();
    }

    /**
     * Converts multiple proto messages to Avro GenericRecords
     */
    public static <T extends GeneratedMessageV3> Iterable<GenericRecord> toAvroGenericRecords(Iterable<T> messages) throws IOException {
        if (!messages.iterator().hasNext()) {
            return ImmutableList.of();
        }
        Class<?> clazz = messages.iterator().next().getClass();
        Schema schema = PromotedProtobufData.get().getSchema(clazz);
        PromotedProtobufDatumWriter<T> pbWriter = new PromotedProtobufDatumWriter<T>(schema);
        DataFileWriter<T> dataFileWriter = new DataFileWriter<T>(pbWriter);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        dataFileWriter.create(schema, out);
        for (T message : messages) {
            dataFileWriter.append(message);
        }
        dataFileWriter.close();
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>();
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new SeekableByteArrayInput(out.toByteArray()), reader)) {
            return (Iterable<GenericRecord>) dataFileReader;
        }
    }

    /**
     * Converts a proto message to Avro GenericRecord with Avro schema fixes for recursive fields.
     */
    public static <T extends GeneratedMessageV3> GenericRecord toFixedAvroGenericRecord(T message) throws IOException {
        return toFixedAvroGenericRecords(ImmutableList.of(message)).iterator().next();
    }

    /**
     * Converts multiple proto messages to Avro GenericRecords with Avro schema fixes for recursive fields.
     */
    public static <T extends GeneratedMessageV3> Iterable<GenericRecord> toFixedAvroGenericRecords(Stream<T> messages) throws IOException {
        return toFixedAvroGenericRecords(messages.collect(Collectors.toList()));
    }

    /**
     * Converts multiple proto messages to Avro GenericRecords with Avro schema fixes for recursive fields.
     */
    public static <T extends GeneratedMessageV3> Iterable<GenericRecord> toFixedAvroGenericRecords(Iterable<T> messages) throws IOException {
        if (!messages.iterator().hasNext()) {
            return ImmutableList.of();
        }
        Class<?> avroProtoClass = messages.iterator().next().getClass();
        Schema schema = FixedProtobufData.get().getSchema(avroProtoClass);
        FixedProtobufDatumWriter<T> pbWriter = new FixedProtobufDatumWriter<>(schema);
        DataFileWriter<T> dataFileWriter = new DataFileWriter<>(pbWriter);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        dataFileWriter.create(schema, out);
        for (T message : messages) {
            dataFileWriter.append(message);
        }
        dataFileWriter.close();
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>();
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new SeekableByteArrayInput(out.toByteArray()), reader)) {
            return (Iterable<GenericRecord>) dataFileReader;
        }
    }
}
