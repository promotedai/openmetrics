package ai.promoted.metrics.logprocessor.common.testing;

import static ai.promoted.metrics.logprocessor.common.testing.AvroAsserts.assertAvroFiles;
import static ai.promoted.metrics.logprocessor.common.testing.AvroAsserts.assertPropertiesFixed;
import static ai.promoted.metrics.logprocessor.common.testing.AvroAsserts.loadAvroRecords;
import static ai.promoted.metrics.logprocessor.common.testing.AvroPojoAsserts.assertMapEquals;
import static ai.promoted.metrics.logprocessor.common.testing.AvroPojoAsserts.fromAvroRecordToMap;
import static ai.promoted.metrics.logprocessor.common.testing.AvroProtoUtils.toFixedAvroGenericRecord;
import static ai.promoted.metrics.logprocessor.common.testing.AvroProtoUtils.writeAvroFile;
import static ai.promoted.metrics.logprocessor.common.testing.Iterables.exactlyOne;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.promoted.metrics.logprocessor.common.fakedatagenerator.LogRequestFactory;
import ai.promoted.proto.common.Properties;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.event.Impression;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Warning: this test runs the stream jobs as batch. Flink stream minicluster tests don't
 * automatically checkpoint at the end.
 *
 * <p>http://apache-flink-user-mailing-list-archive.2336050.n4.nabble.com/Minicluster-Flink-tests-checkpoints-and-inprogress-part-files-td41282.html
 */
public class AvroProtoUtilsTest {

  @TempDir File tempDir;

  List<Impression> getTestProtos() {
    return LogRequestFactory.pushDownToImpressions(
        LogRequestFactory.createFullLogRequests(1601596151000L, 1));
  }

  File writeTestData() throws IOException {
    File file = new File(tempDir, "testdata");
    writeAvroFile(file, getTestProtos());
    return file;
  }

  @Test
  public void testGetGenericRecordsSet() throws IOException {
    File file = writeTestData();
    File[] files = {file};
    ImmutableSet<GenericRecord> actual = loadAvroRecords(files);
    assertEquals(1, actual.size());

    Map<String, Object> actualMap = fromAvroRecordToMap(actual.iterator().next());
    assertMapEquals(
        ImmutableMap.<String, Object>builder()
            .put("platform_id", 1L)
            .put(
                "user_info",
                ImmutableMap.<String, Object>builder()
                    .put("user_id", "")
                    .put("anon_user_id", "")
                    .put("log_user_id", "00000000-0000-0000-0000-000000000001")
                    .put("is_internal_user", false)
                    .put("ignore_usage", false)
                    .put("retained_user_id", "")
                    .build())
            .put(
                "timing",
                ImmutableMap.<String, Object>builder()
                    .put("client_log_timestamp", 1601596151000L)
                    .put("event_api_timestamp", 1601596151000L)
                    .put("log_timestamp", 0L)
                    .put("processing_timestamp", 0L)
                    .build())
            .put("impression_id", "55555555-5555-5555-0000-000000000001")
            .put("insertion_id", "44444444-4444-4444-0000-000000000001")
            .put("request_id", "")
            .put("auto_view_id", "")
            .put("view_id", "")
            .put("session_id", "")
            .put("content_id", "i-1-1")
            .put("source_type", "UNKNOWN_IMPRESSION_SOURCE_TYPE")
            .put("has_superimposed_views", false)
            .build(),
        actualMap);
  }

  @Test
  public void testAssertAvroFilesSingle() throws IOException {
    File file = writeTestData();
    File[] files = {file};
    ImmutableSet<GenericRecord> actual = loadAvroRecords(files);
    assertAvroFiles(exactlyOne(actual), files, "message");
  }

  @Test
  public void testAssertAvroFilesIterable() throws IOException {
    File file = writeTestData();
    File[] files = {file};
    ImmutableSet<GenericRecord> actual = loadAvroRecords(files);
    assertAvroFiles(actual, files, "message");
  }

  @Test
  public void assertPatchedFlatProperties() throws IOException {
    DeliveryLog deliveryLog =
        DeliveryLog.newBuilder()
            .setRequest(
                Request.newBuilder()
                    .setProperties(
                        Properties.newBuilder()
                            .setStruct(
                                Struct.newBuilder()
                                    .putFields(
                                        "stringKey", Value.newBuilder().setStringValue("a").build())
                                    .putFields(
                                        "structKey",
                                        Value.newBuilder()
                                            .setStructValue(
                                                Struct.newBuilder()
                                                    .putFields(
                                                        "stringKey2",
                                                        Value.newBuilder()
                                                            .setStringValue("b")
                                                            .build())
                                                    .putFields(
                                                        "structKey2",
                                                        Value.newBuilder()
                                                            .setStructValue(
                                                                Struct.newBuilder()
                                                                    .putFields(
                                                                        "stringKey3",
                                                                        Value.newBuilder()
                                                                            .setStringValue("b")
                                                                            .build()))
                                                            .build()))
                                            .build()))))
            .build();
    assertPropertiesFixed(toFixedAvroGenericRecord(deliveryLog), true);
  }
}
