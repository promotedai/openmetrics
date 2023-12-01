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

package org.apache.flink.formats.protobuf.serialize;

import com.google.protobuf.Descriptors;
import org.apache.flink.formats.protobuf.PbCodegenException;
import org.apache.flink.formats.protobuf.PbFormatContext;
import org.apache.flink.formats.protobuf.util.PbCodegenAppender;
import org.apache.flink.formats.protobuf.util.PbCodegenVarId;
import org.apache.flink.table.types.logical.LogicalType;

/** Serializer to convert flink array type data to proto array type object. */
public class PromotedPbCodegenArraySerializer implements PbCodegenSerializer {
  private final Descriptors.FieldDescriptor fd;
  private final LogicalType elementType;
  private final PbFormatContext formatContext;

  public PromotedPbCodegenArraySerializer(
      Descriptors.FieldDescriptor fd, LogicalType elementType, PbFormatContext formatContext) {
    this.fd = fd;
    this.elementType = elementType;
    this.formatContext = formatContext;
  }

  @Override
  public String codegen(String resultVar, String flinkObjectCode, int indent)
      throws PbCodegenException {
    // The type of flinkObjectCode is a ArrayData of flink,
    // it should be converted to array of protobuf as resultVariable.
    PbCodegenVarId varUid = PbCodegenVarId.getInstance();
    int uid = varUid.getAndIncrement();
    PbCodegenAppender appender = new PbCodegenAppender(indent);
    String protoTypeStr = PromotedPbCodegenUtils.getTypeStrFromProto(fd, false);
    String pbCollectionVar = fd.isMapField() ? "pbMap" + uid : "pbList" + uid;
    String flinkArrayDataVar = "arrData" + uid;
    String iVar = "i" + uid;

    appender.appendLine("ArrayData " + flinkArrayDataVar + " = " + flinkObjectCode);
    if (fd.isMapField()) {
      String pbElementEntryVar = "elementPbEntryVar" + uid;
      appender.appendLine(protoTypeStr + " " + pbCollectionVar + "= new HashMap()");
      appender.begin(
          "for(int "
              + iVar
              + "=0;"
              + iVar
              + " < "
              + flinkArrayDataVar
              + ".size(); "
              + iVar
              + "++){");
      String convertFlinkArrayElementToPbCode =
          PromotedPbCodegenUtils.convertFlinkArrayElementToPbMapEntry(
              flinkArrayDataVar,
              iVar,
              pbCollectionVar,
              pbElementEntryVar,
              fd,
              elementType,
              formatContext,
              appender.currentIndent());

      appender.appendSegment(convertFlinkArrayElementToPbCode);
    } else {
      appender.appendLine("List<" + protoTypeStr + "> " + pbCollectionVar + "= new ArrayList()");
      String pbElementVar = "elementPbVar" + uid;
      appender.begin(
          "for(int "
              + iVar
              + "=0;"
              + iVar
              + " < "
              + flinkArrayDataVar
              + ".size(); "
              + iVar
              + "++){");

      String convertFlinkArrayElementToPbCode =
          PromotedPbCodegenUtils.convertFlinkArrayElementToPbWithDefaultValueCode(
              flinkArrayDataVar,
              iVar,
              pbElementVar,
              fd,
              elementType,
              formatContext,
              appender.currentIndent());

      appender.appendSegment(convertFlinkArrayElementToPbCode);
      // add pb element to result list
      appender.appendLine(pbCollectionVar + ".add(" + pbElementVar + ")");
    }
    // end for
    appender.end("}");
    appender.appendLine(resultVar + " = " + pbCollectionVar);
    return appender.code();
  }
}
