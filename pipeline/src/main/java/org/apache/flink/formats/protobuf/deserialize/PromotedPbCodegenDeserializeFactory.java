/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.protobuf.deserialize;

import com.google.protobuf.Descriptors;
import org.apache.flink.formats.protobuf.PbCodegenException;
import org.apache.flink.formats.protobuf.PbFormatContext;
import org.apache.flink.formats.protobuf.util.PbFormatUtils;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

/**
 * Codegen factory class which return {@link PbCodegenDeserializer} of different data type. *
 *
 * <p>Copied from
 * https://github.com/apache/flink/blob/cc94a1d1855af3523ed117e6cbab8999b231c822/flink-formats/flink-protobuf/src/main/java/org/apache/flink/formats/protobuf/deserialize/PbCodegenDeserializeFactory.java
 */
public class PromotedPbCodegenDeserializeFactory {
  public static PbCodegenDeserializer getPbCodegenDes(
      Descriptors.FieldDescriptor fd, LogicalType type, PbFormatContext formatContext)
      throws PbCodegenException {
    // We do not use FieldDescriptor to check because there's no way to get
    // element field descriptor of array type.

    // Replaces the deserializers with Promoted ones
    if (type instanceof RowType) {
      return new PromotedPbCodegenRowDeserializer(
          fd.getMessageType(), (RowType) type, formatContext);
    } else if (PbFormatUtils.isSimpleType(type)) {
      return new PbCodegenSimpleDeserializer(fd, type);
    } else if (type instanceof ArrayType) {
      return new PromotedPbCodegenArrayDeserializer(
          fd, ((ArrayType) type).getElementType(), formatContext);
    } else if (type instanceof MapType) {
      return new PromotedPbCodegenMapDeserializer(fd, (MapType) type, formatContext);
    } else {
      throw new PbCodegenException("Do not support flink type: " + type);
    }
  }

  public static PbCodegenDeserializer getPbCodegenTopRowDes(
      Descriptors.Descriptor descriptor, RowType rowType, PbFormatContext formatContext) {
    return new PromotedPbCodegenRowDeserializer(descriptor, rowType, formatContext);
  }
}
