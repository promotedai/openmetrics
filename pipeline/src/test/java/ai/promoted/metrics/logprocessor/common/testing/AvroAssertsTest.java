package ai.promoted.metrics.logprocessor.common.testing;

import static ai.promoted.metrics.logprocessor.common.fakedatagenerator.LogRequestFactory.pushDownToImpressions;
import static ai.promoted.metrics.logprocessor.common.testing.AvroAsserts.loadAvroRecords;
import static ai.promoted.metrics.logprocessor.common.testing.AvroProtoUtils.toAvroGenericRecord;
import static ai.promoted.metrics.logprocessor.common.testing.AvroProtoUtils.toAvroGenericRecords;
import static ai.promoted.metrics.logprocessor.common.testing.AvroProtoUtils.writeAvroFile;
import static ai.promoted.metrics.logprocessor.common.testing.Iterables.exactlyOne;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.promoted.metrics.logprocessor.common.fakedatagenerator.LogRequestFactory;
import ai.promoted.proto.event.Impression;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Warning: this test runs the stream jobs as batch. Flink stream minicluster tests don't
 * automatically checkpoint at the end. TODO - add link to thread about this.
 */
public class AvroAssertsTest {

  @TempDir File tempDir;

  List<Impression> getTestProtos() {
    return pushDownToImpressions(LogRequestFactory.createFullLogRequests(1601596151000L, 1));
  }

  File writeTestData() throws IOException {
    File file = new File(tempDir, "testdata");
    writeAvroFile(file, getTestProtos());
    return file;
  }

  // Use other utilities to have a simple test.
  @Test
  public void testToAvroGenericRecords() throws IOException {
    File file = writeTestData();
    File files[] = {file};
    ImmutableSet<GenericRecord> expected = loadAvroRecords(files);
    assertEquals(expected, ImmutableSet.copyOf(toAvroGenericRecords(getTestProtos())));
  }

  // Use other utilities to have a simple test.
  @Test
  public void testToAvroGenericRecord() throws IOException {
    File file = writeTestData();
    File files[] = {file};
    GenericRecord expected = exactlyOne(loadAvroRecords(files));
    assertEquals(expected, toAvroGenericRecord(exactlyOne(getTestProtos())));
  }
}
