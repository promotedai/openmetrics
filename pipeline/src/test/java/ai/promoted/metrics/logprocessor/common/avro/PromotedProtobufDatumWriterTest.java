package ai.promoted.metrics.logprocessor.common.avro;

import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.Action;
import java.io.ByteArrayOutputStream;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.protobuf.ProtobufDatumWriter;
import org.junit.jupiter.api.Test;

public class PromotedProtobufDatumWriterTest {

  @Test
  public void recognizedValue() throws Exception {
    DataFileWriter<Action> dataFileWriter = createTestDataFileWriter();
    dataFileWriter.append(createAction(1));
    // Just make sure it finishes.
  }

  @Test
  public void unrecognizedValue() throws Exception {
    DataFileWriter<Action> dataFileWriter = createTestDataFileWriter();
    // Use a very large number to avoid a chance of collision.
    dataFileWriter.append(createAction(20000));
    // Just make sure it finishes.
  }

  private DataFileWriter<Action> createTestDataFileWriter() throws Exception {
    PromotedProtobufData protobufData = PromotedProtobufData.get();
    Schema schema = protobufData.getSchema(Action.class);
    ProtobufDatumWriter<Action> pbWriter = new PromotedProtobufDatumWriter(schema, protobufData);
    DataFileWriter<Action> dataFileWriter = new DataFileWriter<>(pbWriter);
    dataFileWriter.setCodec(CodecFactory.bzip2Codec());
    ByteArrayOutputStream out = new ByteArrayOutputStream(1_000_000);
    dataFileWriter.create(schema, out);
    return dataFileWriter;
  }

  private Action createAction(long actionType) {
    return Action.newBuilder()
        .setUserInfo(UserInfo.newBuilder().setLogUserId("logUser1"))
        .setActionTypeValue(1)
        .setImpressionId("imp1")
        .setContentId("content1")
        .build();
  }
}
