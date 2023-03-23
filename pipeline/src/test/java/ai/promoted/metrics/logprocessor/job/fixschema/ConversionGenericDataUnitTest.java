package ai.promoted.metrics.logprocessor.job.fixschema;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import ai.promoted.metrics.datafix.RecursiveValue;
import ai.promoted.metrics.datafix.TestRecord1;
import ai.promoted.metrics.datafix.TestRecord2;
import ai.promoted.metrics.logprocessor.common.avro.FixedProtobufData;
import ai.promoted.metrics.logprocessor.common.avro.FixedProtobufDatumWriter;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.LogRequestFactory;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.event.LogRequest;
import com.google.common.collect.ImmutableList;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.Test;

public class ConversionGenericDataUnitTest {
  @Test
  void copyTestRecord() {
    TestRecord1 record =
        TestRecord1.newBuilder()
            .setPlatformId(1L)
            // GMT: Thursday, October 1, 2020 11:49:11 PM
            .setEventApiTimestamp(1601596151000L)
            .setValue(
                RecursiveValue.newBuilder()
                    .setValue(RecursiveValue.newBuilder().setStringValue("b").build())
                    .setStringValue("b")
                    .build())
            .build();

    GenericRecord copy =
        new ConversionGenericData(
                ImmutableList.of(
                    new Tuple2<>(
                        "ai.promoted.metrics.datafix.RecursiveValue",
                        "ai.promoted.metrics.datafix.Value")))
            .deepCopy(TestRecord2.SCHEMA$, record);
    assertNotNull(copy);
  }

  // This doesn't change any schema for DeliveryLogs since the new and old schemas are the same.
  @Test
  void copyDeliveryLog() throws IOException {
    DeliveryLog deliveryLogProto = createDeliveryLogProto();

    ConversionGenericData data =
        new ConversionGenericData(
            ImmutableList.of(
                new Tuple2<>(
                    "ai.promoted.metrics.datafix.RecursiveValue",
                    "ai.promoted.metrics.datafix.Value")));
    Schema schema = FixedProtobufData.get().getSchema(DeliveryLog.getDescriptor());
    GenericRecord record = toGenericRecord(deliveryLogProto, data);

    long start = System.currentTimeMillis();
    for (int i = 0; i < 100; i++) {
      GenericRecord copy = data.deepCopy(schema, record);
      assertNotNull(copy);
    }
    long end = System.currentTimeMillis();
    // Was 16ms to 17ms for the previous implementation.
    // assertEquals(0, end - start);
  }

  private DeliveryLog createDeliveryLogProto() {
    List<LogRequest> logRequests =
        LogRequestFactory.createLogRequests(
            1601596151000L, LogRequestFactory.DetailLevel.PARTIAL, 1);
    return LogRequestFactory.pushDownToDeliveryLogs(logRequests).get(0);
  }

  private GenericRecord toGenericRecord(DeliveryLog deliveryLogProto, ConversionGenericData data)
      throws IOException {
    Schema schema = FixedProtobufData.get().getSchema(DeliveryLog.getDescriptor());
    FixedProtobufDatumWriter<DeliveryLog> datumWriter = new FixedProtobufDatumWriter<>(schema);
    DataFileWriter<DeliveryLog> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.setCodec(CodecFactory.snappyCodec());
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    dataFileWriter.create(schema, stream);
    dataFileWriter.append(deliveryLogProto);
    dataFileWriter.close();

    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
    SeekableByteArrayInput input = new SeekableByteArrayInput(stream.toByteArray());
    DataFileReader<GenericRecord> dataFileReader =
        new DataFileReader<GenericRecord>(input, datumReader);
    GenericRecord record = dataFileReader.next();
    dataFileReader.close();
    return record;
  }
}
