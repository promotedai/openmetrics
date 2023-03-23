package ai.promoted.metrics.logprocessor.common.records;

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.Serializable;

public interface ProtoDeserializer<T> extends Serializable {
  T deserialize(byte[] bytes) throws InvalidProtocolBufferException;
}
