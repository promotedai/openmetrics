package ai.promoted.metrics.logprocessor.common.functions;

import com.google.protobuf.InvalidProtocolBufferException;

import java.io.Serializable;

@FunctionalInterface
public interface SerializableParseFrom<T> extends Serializable {
    T apply(byte[] bytes) throws InvalidProtocolBufferException;
}
