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
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import org.apache.flink.formats.protobuf.PbCodegenException;
import org.apache.flink.formats.protobuf.PbFormatContext;
import org.apache.flink.formats.protobuf.util.PbCodegenAppender;
import org.apache.flink.formats.protobuf.util.PbCodegenVarId;
import org.apache.flink.formats.protobuf.util.PbFormatUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/**
 * Deserializer to convert proto message type object to flink row type data.
 *
 * <p>Copied from
 * https://github.com/apache/flink/blob/cc94a1d1855af3523ed117e6cbab8999b231c822/flink-formats/flink-protobuf/src/main/java/org/apache/flink/formats/protobuf/deserialize/PbCodegenRowDeserializer.java
 */
public class PromotedPbCodegenRowDeserializer implements PbCodegenDeserializer {
  private final Descriptor descriptor;
  private final RowType rowType;
  private final PbFormatContext formatContext;

  private final Descriptors.FileDescriptor.Syntax syntax;

  public PromotedPbCodegenRowDeserializer(
      Descriptor descriptor, RowType rowType, PbFormatContext formatContext) {
    this.rowType = rowType;
    this.descriptor = descriptor;
    this.formatContext = formatContext;
    this.syntax = descriptor.getFile().getSyntax();
  }

  @Override
  public String codegen(String resultVar, String pbObjectCode, int indent)
      throws PbCodegenException {
    // The type of pbObjectCode is a general pb object,
    // it should be converted to RowData of flink internal type as resultVariable
    PbCodegenAppender appender = new PbCodegenAppender(indent);
    PbCodegenVarId varUid = PbCodegenVarId.getInstance();
    int uid = varUid.getAndIncrement();
    String flinkRowDataVar = "rowData" + uid;

    int fieldSize = rowType.getFieldNames().size();
    String pbMessageTypeStr = null;
    String pbMessageVar;
    // ============== Xingcan: Use entry to iterate map ====================
    if (descriptor.getOptions().hasMapEntry()) {
      pbMessageVar = pbObjectCode; // entry+uid
    } else {
      pbMessageVar = "message" + uid;
      pbMessageTypeStr = PbFormatUtils.getFullJavaName(descriptor);
      appender.appendLine(
          String.format(
              "%s %s = (%s)%s", pbMessageTypeStr, pbMessageVar, pbMessageTypeStr, pbObjectCode));
    }
    // ============== Xingcan: Use entry to iterate map ====================
    appender.appendLine(
        "GenericRowData " + flinkRowDataVar + " = new GenericRowData(" + fieldSize + ")");
    int index = 0;
    for (String fieldName : rowType.getFieldNames()) {
      int subUid = varUid.getAndIncrement();
      String flinkRowEleVar = "elementDataVar" + subUid;

      LogicalType subType = rowType.getTypeAt(rowType.getFieldIndex(fieldName));
      FieldDescriptor elementFd = descriptor.findFieldByName(fieldName);
      String strongCamelFieldName = PbFormatUtils.getStrongCamelCaseJsonName(fieldName);
      PbCodegenDeserializer codegen =
          PromotedPbCodegenDeserializeFactory.getPbCodegenDes(elementFd, subType, formatContext);
      appender.appendLine("Object " + flinkRowEleVar + " = null");
      if (!formatContext.getPbFormatConfig().isReadDefaultValues() && null != pbMessageTypeStr) {
        String isMessageElementNonEmptyCode =
            isMessageElementNonEmptyCode(
                pbMessageTypeStr,
                pbMessageVar,
                strongCamelFieldName,
                PbFormatUtils.isRepeatedType(subType),
                fieldName);
        appender.begin("if(" + isMessageElementNonEmptyCode + "){");
      }
      String pbGetMessageElementCode =
          pbGetMessageElementCode(
              pbMessageVar, strongCamelFieldName, elementFd, PbFormatUtils.isArrayType(subType));
      String code =
          codegen.codegen(flinkRowEleVar, pbGetMessageElementCode, appender.currentIndent());
      appender.appendSegment(code);
      if (!formatContext.getPbFormatConfig().isReadDefaultValues() && null != pbMessageTypeStr) {
        appender.end("}");
      }
      appender.appendLine(flinkRowDataVar + ".setField(" + index + ", " + flinkRowEleVar + ")");
      index += 1;
    }
    appender.appendLine(resultVar + " = " + flinkRowDataVar);
    return appender.code();
  }

  private String pbGetMessageElementCode(
      String message, String fieldName, FieldDescriptor fd, boolean isList) {
    if (fd.isMapField()) {
      // map
      return message + ".get" + fieldName + "Map()";
    } else if (isList) {
      // list
      return message + ".get" + fieldName + "List()";
    } else {
      return message + ".get" + fieldName + "()";
    }
  }

  private String isMessageElementNonEmptyCode(
      String pbMessageTypeStr,
      String message,
      String fieldName,
      boolean isListOrMap,
      String protoFieldName) {
    if (isListOrMap) {
      return message + ".get" + fieldName + "Count() > 0";
    } else {
      // proto syntax class do not have hasName() interface
      switch (syntax) {
        case PROTO3:
          return String.format(
              "%s.hasField(%s.getDescriptor().findFieldByName(\"%s\"))",
              message, pbMessageTypeStr, protoFieldName);
        case PROTO2:
          // proto syntax class do not have hasName() interface
          return message + ".has" + fieldName + "()";
        default:
          throw new IllegalArgumentException("Unknown proto syntax " + syntax);
      }
    }
  }
}
