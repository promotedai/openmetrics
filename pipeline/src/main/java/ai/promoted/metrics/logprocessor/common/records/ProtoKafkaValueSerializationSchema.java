package ai.promoted.metrics.logprocessor.common.records;

import com.google.protobuf.GeneratedMessageV3;
import org.apache.flink.api.common.serialization.SerializationSchema;

/** Used to serialize Protobufs as Kafka values. */
public class ProtoKafkaValueSerializationSchema<T extends GeneratedMessageV3>
    implements SerializationSchema<T> {

  @Override
  public byte[] serialize(T record) {
    return record.toByteArray();
  }
}
