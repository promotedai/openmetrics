package org.apache.flink.formats.protobuf.deserialize;
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

// Port from Flink repository

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FileDescriptor.Syntax;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.formats.protobuf.PbCodegenException;
import org.apache.flink.formats.protobuf.PbConstant;
import org.apache.flink.formats.protobuf.PbFormatConfig;
import org.apache.flink.formats.protobuf.PbFormatContext;
import org.apache.flink.formats.protobuf.util.PbCodegenAppender;
import org.apache.flink.formats.protobuf.util.PbCodegenUtils;
import org.apache.flink.formats.protobuf.util.PbFormatUtils;
import org.apache.flink.table.codesplit.JavaCodeSplitter;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * {@link org.apache.flink.formats.protobuf.deserialize.ProtoToRowConverter} can convert binary
 * protobuf message data to flink row data by codegen process.
 *
 * <p>Copied from
 * https://github.com/apache/flink/blob/cc94a1d1855af3523ed117e6cbab8999b231c822/flink-formats/flink-protobuf/src/main/java/org/apache/flink/formats/protobuf/deserialize/ProtoToRowConverter.java
 */
public class PromotedProtoToRowConverter<T> {
  private static final Logger LOG = LogManager.getLogger(PromotedProtoToRowConverter.class);
  private final Method parseFromMethod;
  private final Method decodeMethod;
  private final Object decodeObject;

  public PromotedProtoToRowConverter(RowType rowType, PbFormatConfig formatConfig)
      throws PbCodegenException {
    try {
      Descriptors.Descriptor descriptor =
          PbFormatUtils.getDescriptor(formatConfig.getMessageClassName());
      Class<?> messageClass =
          Class.forName(
              formatConfig.getMessageClassName(),
              true,
              Thread.currentThread().getContextClassLoader());
      String fullMessageClassName = PbFormatUtils.getFullJavaName(descriptor);
      if (descriptor.getFile().getSyntax() == Syntax.PROTO3) {
        // pb3 always read default values
        formatConfig =
            new PbFormatConfig(
                formatConfig.getMessageClassName(),
                formatConfig.isIgnoreParseErrors(),
                true,
                formatConfig.getWriteNullStringLiterals());
      }
      PbCodegenAppender codegenAppender = new PbCodegenAppender();
      PbFormatContext pbFormatContext = new PbFormatContext(formatConfig);
      String uuid = UUID.randomUUID().toString().replaceAll("\\-", "");
      String generatedClassName = "GeneratedProtoToRow_" + uuid;
      String generatedPackageName =
          org.apache.flink.formats.protobuf.deserialize.ProtoToRowConverter.class
              .getPackage()
              .getName();
      codegenAppender.appendLine("package " + generatedPackageName);
      codegenAppender.appendLine("import " + RowData.class.getName());
      codegenAppender.appendLine("import " + ArrayData.class.getName());
      codegenAppender.appendLine("import " + BinaryStringData.class.getName());
      codegenAppender.appendLine("import " + GenericRowData.class.getName());
      codegenAppender.appendLine("import " + GenericMapData.class.getName());
      codegenAppender.appendLine("import " + GenericArrayData.class.getName());
      codegenAppender.appendLine("import " + ArrayList.class.getName());
      codegenAppender.appendLine("import " + List.class.getName());
      codegenAppender.appendLine("import " + Map.class.getName());
      codegenAppender.appendLine("import " + HashMap.class.getName());
      codegenAppender.appendLine("import " + ByteString.class.getName());

      codegenAppender.appendSegment("public class " + generatedClassName + "{");
      // Don't use static method since JavaSplitter doesn't support it.
      codegenAppender.appendSegment(
          "public RowData "
              + PbConstant.GENERATED_DECODE_METHOD
              + "("
              + fullMessageClassName
              + " message){");
      codegenAppender.appendLine("RowData rowData=null");
      PbCodegenDeserializer codegenDes =
          PromotedPbCodegenDeserializeFactory.getPbCodegenTopRowDes(
              descriptor, rowType, pbFormatContext);
      String genCode = codegenDes.codegen("rowData", "message", 0);
      codegenAppender.appendSegment(genCode);
      codegenAppender.appendLine("return rowData");
      codegenAppender.appendSegment("}");
      codegenAppender.appendSegment("}");

      if (LOG.isDebugEnabled()) {
        String printCode = codegenAppender.printWithLineNumber();
        LOG.debug("Protobuf decode codegen: \n" + printCode);
      }

      // The generated source code needs to be split to avoid the 64KB limit problem.
      String splitCode = JavaCodeSplitter.split(codegenAppender.code(), 4000, 10000);
      Class<?> generatedClass =
          PbCodegenUtils.compileClass(
              Thread.currentThread().getContextClassLoader(),
              generatedPackageName + "." + generatedClassName,
              splitCode);
      decodeObject = generatedClass.getConstructor().newInstance();
      decodeMethod = generatedClass.getMethod(PbConstant.GENERATED_DECODE_METHOD, messageClass);
      parseFromMethod = messageClass.getMethod(PbConstant.PB_METHOD_PARSE_FROM, byte[].class);
    } catch (Exception ex) {
      throw new PbCodegenException(ex);
    }
  }

  public RowData convertProtoBinaryToRow(byte[] data) throws Exception {
    Object messageObj = parseFromMethod.invoke(null, data);
    return (RowData) decodeMethod.invoke(decodeObject, messageObj);
  }

  public RowData convertProtoToRow(T messageObject) throws Exception {
    return (RowData) decodeMethod.invoke(decodeObject, messageObject);
  }
}
