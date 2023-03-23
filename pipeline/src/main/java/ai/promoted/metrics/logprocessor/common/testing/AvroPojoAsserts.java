package ai.promoted.metrics.logprocessor.common.testing;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.flink.util.Preconditions;

/**
 * Asserts for Avro files and plain old java objects (POJOs). Prefer using AvroProtoUtils. It is
 * easier to reuse common test objects that are protos across systems. POJO comparisons will be used
 * more for internal testing of our frameworks.
 */
public interface AvroPojoAsserts {

  /**
   * Converts a GenericRecord to a Object Map.
   *
   * @param record Avro object
   * @return POJO map version.
   */
  static Map<String, Object> fromAvroRecordToMap(GenericRecord record) {
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    record.getSchema().getFields().stream()
        .forEach(
            field -> {
              Object value = record.get(field.name());
              if (value == null) {
                return;
              }
              value = fromAvroToPojo(value);
              if (value != null) {
                builder.put(field.name(), value);
              }
            });
    return builder.build();
  }

  /**
   * Converts from Avro object to POJO.
   *
   * @param value Avro object
   * @return POJO
   */
  static Object fromAvroToPojo(Object value) {
    if (value == null) {
      return null;
    }
    Preconditions.checkArgument(
        value instanceof Utf8
            || value instanceof Boolean
            || value instanceof Long
            || value instanceof Integer
            || value instanceof Double
            || value instanceof String
            || value instanceof ByteBuffer
            || value instanceof GenericRecord
            || value instanceof GenericData.Array
            || value instanceof GenericData.EnumSymbol,
        "Avro GenericRecord value class is unsupported: %s",
        value.getClass());
    if (value instanceof ByteBuffer) {
      // There are other ByteBuffer implementations.  Just wrap for now to simplify comparisons.
      return ByteBuffer.wrap(((ByteBuffer) value).array());
    } else if (value instanceof GenericData.Array) {
      return ((GenericData.Array) value)
          .stream().map(AvroPojoAsserts::fromAvroToPojo).collect(Collectors.toList());
    } else if (value instanceof GenericRecord) {
      return fromAvroRecordToMap((GenericRecord) value);
    } else if (value instanceof GenericData.EnumSymbol) {
      return value.toString();
    } else {
      return value;
    }
  }

  /**
   * Reads the Avro {@code files} and convert to POJO representation.
   *
   * @param files Avro files
   * @return Set of POJOs
   * @throws IOException failure reading the files
   */
  static Set<Map<String, Object>> getRecordMaps(File[] files) throws IOException {
    return AvroAsserts.loadAvroRecords(files).stream()
        .map(AvroPojoAsserts::fromAvroRecordToMap)
        .collect(Collectors.toUnmodifiableSet());
  }

  static Set<Map<String, Object>> getAvroParquetRecordMaps(File[] files) throws IOException {
    return AvroAsserts.loadAvroParquetRecords(files).stream()
        .map(AvroPojoAsserts::fromAvroRecordToMap)
        .collect(Collectors.toUnmodifiableSet());
  }

  /** A wrapper that outputs typo info to make test failures easier to see. */
  final class ObjectWrapper {
    private Object obj;

    ObjectWrapper(Object obj) {
      this.obj = obj;
    }

    @Override
    public int hashCode() {
      return obj.hashCode();
    }

    @Override
    public boolean equals(Object right) {
      if (!(right instanceof ObjectWrapper)) {
        return false;
      }
      return Objects.equal(obj, ((ObjectWrapper) right).obj);
    }

    @Override
    public String toString() {
      return "[" + obj.getClass().getSimpleName() + "] " + obj.toString();
    }
  }

  // Internal.
  static Map<String, Object> addTypeLayer(Map<String, Object> map) {
    ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
    map.entrySet()
        .forEach(
            entry -> {
              // TODO - support list.
              Object value;
              if (entry.getValue() instanceof Map) {
                value =
                    ((Map<String, Object>) entry.getValue())
                        .entrySet().stream()
                            .collect(
                                Collectors.toMap(
                                    Map.Entry::getKey, e -> new ObjectWrapper(e.getValue())));
              } else {
                value = entry.getValue();
              }
              mapBuilder.put(entry.getKey(), new ObjectWrapper(value));
            });
    return mapBuilder.build();
  }

  /**
   * Assert {@code files} contains the {@code expectedRecords}.
   *
   * @param expectedRecords expected POJOs
   * @param files the files to check
   */
  static void assertAvroFiles(Set<Map<String, Object>> expectedRecords, File[] files)
      throws IOException {
    Set<Map<String, Object>> actualRecords = getRecordMaps(files);
    assertAvroFiles(expectedRecords, actualRecords);
  }

  static void assertAvroParquetFiles(Set<Map<String, Object>> expectedRecords, File[] files)
      throws IOException {
    Set<Map<String, Object>> actualRecords = getAvroParquetRecordMaps(files);
    assertAvroFiles(expectedRecords, actualRecords);
  }

  static void assertAvroFiles(
      Set<Map<String, Object>> expectedRecords, Set<Map<String, Object>> actualRecords)
      throws IOException {
    // Easier asserts when there is only one item in each Set.
    if (expectedRecords.size() == 1 && actualRecords.size() == 1) {
      assertMapEquals(expectedRecords.iterator().next(), actualRecords.iterator().next());
      return;
    }

    Sets.SetView<Map<String, Object>> expectedDiffs =
        Sets.difference(expectedRecords, actualRecords);
    Sets.SetView<Map<String, Object>> actualDiffs = Sets.difference(actualRecords, expectedRecords);

    assertAll(
        "record",
        Streams.concat(
            expectedDiffs.stream()
                .map(
                    (record) ->
                        () -> {
                          fail("Did not find expected record=" + addTypeLayer(record));
                        }),
            actualDiffs.stream()
                .map(
                    (record) ->
                        () -> {
                          fail("Found unexpected record=" + addTypeLayer(record));
                        })));
  }

  /** Better assert for comparing map objects. */
  static void assertMapEquals(Map<String, Object> expectedRecord, Map<String, Object> actualRecord)
      throws IOException {
    MapDifference<String, Object> expectedDiffs = Maps.difference(expectedRecord, actualRecord);

    if (!expectedDiffs.areEqual()) {
      if (!expectedDiffs.entriesDiffering().isEmpty()) {
        fail("Entries differ, " + expectedDiffs.entriesDiffering());
      }
      if (!expectedDiffs.entriesOnlyOnLeft().isEmpty()) {
        fail("Entries only in expected, " + expectedDiffs.entriesOnlyOnLeft());
      }
      if (!expectedDiffs.entriesOnlyOnRight().isEmpty()) {
        fail("Entries only in actual, " + expectedDiffs.entriesOnlyOnRight());
      }
    }
  }
}
