package ai.promoted.metrics.logprocessor.common.records;

import com.google.protobuf.GeneratedMessageV3;

/** Used to serialize Protobufs as Kafka values. No Flink dependency. */
public class ProtoKafkaValueSerializationSchema<T extends GeneratedMessageV3> {
  public byte[] serialize(T record) {
    return record.toByteArray();
  }
}
