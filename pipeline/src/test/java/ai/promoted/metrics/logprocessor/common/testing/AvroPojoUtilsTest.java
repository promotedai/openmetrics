package ai.promoted.metrics.logprocessor.common.testing;

import ai.promoted.metrics.logprocessor.common.fakedatagenerator.LogRequestFactory;
import ai.promoted.proto.event.Impression;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opentest4j.AssertionFailedError;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static ai.promoted.metrics.logprocessor.common.fakedatagenerator.LogRequestFactory.pushDownToImpressions;
import static ai.promoted.metrics.logprocessor.common.testing.AvroAsserts.loadAvroRecords;
import static ai.promoted.metrics.logprocessor.common.testing.AvroPojoAsserts.assertAvroFiles;
import static ai.promoted.metrics.logprocessor.common.testing.AvroPojoAsserts.assertMapEquals;
import static ai.promoted.metrics.logprocessor.common.testing.AvroPojoAsserts.fromAvroRecordToMap;
import static ai.promoted.metrics.logprocessor.common.testing.AvroPojoAsserts.getRecordMaps;
import static ai.promoted.metrics.logprocessor.common.testing.AvroProtoUtils.writeAvroFile;
import static ai.promoted.metrics.logprocessor.common.testing.Iterables.exactlyOne;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Warning: this test runs the stream jobs as batch.
 * Flink stream minicluster tests don't automatically checkpoint at the end.
 * TODO - add link to thread about this.
 */
public class AvroPojoUtilsTest {

  @TempDir
  File tempDir;

  final long timeMillis1 = 1601596151000L;
  final long timeMillis2 =  1601596351000L;

  List<Impression> getTestProtos(long timeMillis) {
    return pushDownToImpressions(LogRequestFactory.createFullLogRequests(timeMillis, 1));
  }

  File writeTestData(String filename, long timeMillis) throws IOException {
    File file = new File(tempDir, filename);
    writeAvroFile(file, getTestProtos(timeMillis));
    return file;
  }

  Map<String, Object> getExpectedRecordMap(long timeMillis) {
    return ImmutableMap.<String, Object>builder()
            .put("platform_id", 1L)
            .put("user_info", ImmutableMap.<String, Object>builder()
                    .put("user_id", "")
                    .put("log_user_id", "00000000-0000-0000-0000-000000000001")
                    .put("is_internal_user", false)
                    .put("ignore_usage", false)
                    .build())
            .put("timing", ImmutableMap.<String, Object>builder()
                    .put("client_log_timestamp", timeMillis)
                    .put("event_api_timestamp", timeMillis)
                    .put("log_timestamp", 0L)
                    .build())
            .put("impression_id", "55555555-5555-5555-0000-000000000001")
            .put("insertion_id", "44444444-4444-4444-0000-000000000001")
            .put("request_id", "")
            .put("view_id", "")
            .put("auto_view_id", "")
            .put("session_id", "")
            .put("content_id", "i-1-1")
            .put("source_type", "UNKNOWN_IMPRESSION_SOURCE_TYPE")
            .put("has_superimposed_views", false)
            .build();
  }

  @Test
  public void testFromAvroRecordToMap() throws IOException {
    File file = writeTestData("testdata", timeMillis1);
    File files[] = {file};
    ImmutableSet<GenericRecord> actual = loadAvroRecords(files);
    assertEquals(1, actual.size());

    assertMapEquals(
            getExpectedRecordMap(timeMillis1),
            fromAvroRecordToMap(exactlyOne(actual)));
  }

  @Test
  public void testGetRecordMaps() throws IOException {
    File file1 = writeTestData("testdata1", timeMillis1);
    File file2 = writeTestData("testdata2", timeMillis2);
    File files[] = {file1, file2};

    assertEquals(
            ImmutableSet.of(
                    getExpectedRecordMap(timeMillis1),
                    getExpectedRecordMap(timeMillis2)),
            getRecordMaps(files));
  }

  @Test
  public void testAssertAvroFiles_oneRecords() throws IOException {
    File file = writeTestData("testdata1", timeMillis1);
    File files[] = {file};

    assertAvroFiles(
            ImmutableSet.of(getExpectedRecordMap(timeMillis1)),
            files);
  }

  @Test
  public void testAssertAvroFiles_fail() throws IOException {
    File file = writeTestData("testdata1", timeMillis1);
    File files[] = {file};
    assertThrows(AssertionFailedError.class, () -> {
      assertAvroFiles(
              ImmutableSet.of(getExpectedRecordMap(timeMillis2)),
              files);
    });
  }

  @Test
  public void testAssertAvroFiles_multipleRecords() throws IOException {
    File file1 = writeTestData("testdata1", timeMillis1);
    File file2 = writeTestData("testdata2", timeMillis2);
    File files[] = {file1, file2};

    assertAvroFiles(
            ImmutableSet.of(
                    getExpectedRecordMap(timeMillis1),
                    getExpectedRecordMap(timeMillis2)),
            files);
  }
}
