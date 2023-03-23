package ai.promoted.metrics.logprocessor.common.records;

import ai.promoted.metrics.logprocessor.common.functions.SerializableParseFrom;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Base64;

/** A serializable function to deserialize the proto. */
public class ParseFromProtoDeserializer<T extends GeneratedMessageV3>
    implements ProtoDeserializer<T> {
  private SerializableParseFrom<T> parseFrom;

  public ParseFromProtoDeserializer(SerializableParseFrom<T> parseFrom) {
    this.parseFrom = parseFrom;
  }

  @Override
  public T deserialize(byte[] bytes) {
    try {
      return parseFrom.apply(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(
          "Invalid proto, base64=" + Base64.getEncoder().encodeToString(bytes), e);
    }
  }
}
