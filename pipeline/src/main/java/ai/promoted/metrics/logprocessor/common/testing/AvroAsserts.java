package ai.promoted.metrics.logprocessor.common.testing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.*;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.util.Preconditions;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Generic Avro asserts.
 * Proto or Pojo specific asserts should go into other assert files.
 */
public class AvroAsserts {

    public static Object getNestedField(GenericRecord record, String... fieldNames) {
        return getNestedField(record, ImmutableList.copyOf(fieldNames));
    }

    public static Object getNestedField(GenericRecord record, List<String> fieldNames) {
        Preconditions.checkArgument(!fieldNames.isEmpty(), "fieldNames need to be specified");
        Object child = record.get(fieldNames.get(0));
        if (fieldNames.size() == 1) {
            return child;
        } else {
            Preconditions.checkArgument(child instanceof GenericRecord,
                    "field %s needs to be a GenericRecord, it's actually=%s",
                    fieldNames.get(0), child.getClass().getSimpleName());
            return getNestedField((GenericRecord) child, fieldNames.subList(1, fieldNames.size()));
        }
    }

    public static ImmutableSet<GenericRecord> loadAvroRecords(File[] files) throws IOException {
        ImmutableSet.Builder<GenericRecord> resultBuilder = ImmutableSet.builder();
        for (File file : files) {
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>();
            try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader(file, reader)) {
                resultBuilder.addAll((Iterable<GenericRecord>) dataFileReader);
            }
        }
        return resultBuilder.build();
    }

    public static ImmutableSet<GenericRecord> loadAvroParquetRecords(File[] files) throws IOException {
        ImmutableSet.Builder<GenericRecord> resultBuilder = ImmutableSet.builder();
        for (File file : files) {
            try (ParquetReader<GenericRecord> reader = AvroParquetReader
                    .<GenericRecord>builder(new org.apache.hadoop.fs.Path(file.getPath()))
                    .build()) {
                resultBuilder.add(reader.read());
            }
        }
        return resultBuilder.build();
    }

    public static void assertPropertiesFixed(GenericRecord rec, Boolean isFixed) {
        String schemaJSON = rec.getSchema().toString();
        // if the word "Properties" exists in the schema, confirm that it has/hasn't been substituted.
        assertTrue(schemaJSON.contains("properties"),
                "Record does not contain 'properties'.  Do not call assertPropertiesFixed for records that do not properties");
        assertEquals(isFixed, schemaJSON.contains("FlatProperties"));
        assertEquals(isFixed, schemaJSON.contains("Struct1"));
    }

    public static void assertAvroFiles(GenericRecord expectedRecord, File[] files, String message) throws IOException {
        assertAvroFilesSet(ImmutableSet.of(expectedRecord), files, message);
    }

    public static void assertAvroFiles(Iterable<GenericRecord> expectedRecords, File[] files, String message) throws IOException {
        assertAvroFilesSet(ImmutableSet.copyOf(expectedRecords), files, message);
    }

    private static void assertAvroFilesSet(Set<GenericRecord> expectedRecords, File[] files, String message) throws IOException {
        Set<GenericRecord> actualRecords = loadAvroRecords(files);
        assertGenericRecordSet(expectedRecords, actualRecords, message);
    }

    public static void assertAvroParquetFiles(GenericRecord expectedRecord, File[] files, String message) throws IOException {
        assertAvroParquetFilesSet(ImmutableSet.of(expectedRecord), files, message);
    }

    public static void assertAvroParquetFiles(Iterable<GenericRecord> expectedRecords, File[] files, String message) throws IOException {
        assertAvroParquetFilesSet(ImmutableSet.copyOf(expectedRecords), files, message);
    }

    private static void assertAvroParquetFilesSet(Set<GenericRecord> expectedRecords, File[] files, String message) throws IOException {
        Set<GenericRecord> actualRecords = loadAvroParquetRecords(files);
        assertGenericRecordSet(expectedRecords, actualRecords, message);
    }

    private static void assertGenericRecordSet(Set<GenericRecord> expectedRecords, Set<GenericRecord> actualRecords, String message) throws IOException {
        if (expectedRecords.size() == 1 && actualRecords.size() == 1) {
            assertGenericRecordEquals(expectedRecords.iterator().next(), actualRecords.iterator().next(), message);
            return;
        }

        Sets.SetView<GenericRecord> expectedDiffs = Sets.difference(expectedRecords, actualRecords);
        Sets.SetView<GenericRecord> actualDiffs = Sets.difference(actualRecords, expectedRecords);

        assertAll(message,
                Streams.concat(
                        expectedDiffs.stream().map((record) -> () -> {
                            fail("Did not find expected record=" + record);
                        }),
                        actualDiffs.stream().map((record) -> () -> {
                            fail("Found unexpected record=" + record);
                        })));
    }

    public static void assertGenericRecordEquals(GenericRecord expectedRecord, GenericRecord actualRecord, String message) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> expected = mapper.readValue(expectedRecord.toString(), Map.class);
        Map<String, Object> expectedFlat = FlatMapUtil.flatten(expected);
        Map<String, Object> actual = mapper.readValue(actualRecord.toString(), Map.class);
        Map<String, Object> actualFlat = FlatMapUtil.flatten(actual);

        MapDifference diff = Maps.difference(expectedFlat, actualFlat);

        assertTrue(diff.areEqual(), () -> "Records are different, message=" + message + ", " + diff);
    }
}