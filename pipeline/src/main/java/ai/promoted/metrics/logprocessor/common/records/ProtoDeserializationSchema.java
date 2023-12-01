/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.promoted.metrics.logprocessor.common.records;

import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.Base64;

// TODO - see if we can do this serialization automatically.

// TODO - rename to SerDeSchema

/** A Kafka deserialization schema to deserialize Protocol Buffers. No Flink deps. */
public class ProtoDeserializationSchema<T extends GeneratedMessageV3> {
  private static final long serialVersionUID = 2L;

  private final ProtoDeserializer<T> deserializer;

  // Flink complains now that this class is not serializable.
  public ProtoDeserializationSchema() {
    deserializer = null;
  }

  public ProtoDeserializationSchema(ProtoDeserializer<T> deserializer) {
    this.deserializer = deserializer;
  }

  public T deserialize(byte[] message) throws IOException {
    try {
      return deserializer.deserialize(message);
    } catch (InvalidProtocolBufferException e) {
      throw new IOException(
          "Invalid proto, base64=" + Base64.getEncoder().encodeToString(message), e);
    }
  }

  public byte[] serialize(T message) {
    return message.toByteArray();
  }
}
