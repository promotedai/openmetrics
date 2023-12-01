package ai.promoted.metrics.logprocessor.common.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.promoted.proto.common.FlatProperties;
import ai.promoted.proto.common.ListValue1;
import ai.promoted.proto.common.ListValue2;
import ai.promoted.proto.common.Properties;
import ai.promoted.proto.common.Value1;
import ai.promoted.proto.common.Value2;
import ai.promoted.proto.delivery.Request;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import org.apache.avro.Schema;
import org.apache.avro.protobuf.ProtobufData;
import org.junit.jupiter.api.Test;

public class PromotedProtobufDataTest {
  @Test
  public void testGetSchema_Properties() {
    Schema schema = PromotedProtobufData.get().getSchema(Properties.getDescriptor());
    assertTrue(schema.toString().contains("FlatProperties"));
    assertTrue(schema.toString().contains("Struct1"));
  }

  @Test
  public void testGetSchema_Request() {
    Schema schema = PromotedProtobufData.get().getSchema(Request.getDescriptor());
    assertTrue(schema.toString().contains("FlatRequest"));
    assertTrue(schema.toString().contains("ListValue1"));
  }

  @Test
  public void testResolveUnionNormal_Properties() {
    Schema schemaProperties = ProtobufData.get().getSchema(Properties.class);
    Schema schemaStruct = schemaProperties.getField("struct").schema();
    Properties p = Properties.newBuilder().build();
    int i = PromotedProtobufData.get().resolveUnion(schemaStruct, p.getStruct());
    assertEquals(1, i);
  }

  @Test
  public void testResolveUnionModified_FlatProperties() {
    Schema schemaProperties = ProtobufData.get().getSchema(FlatProperties.class);
    Schema schemaStruct = schemaProperties.getField("struct").schema();
    Properties p = Properties.newBuilder().build();
    int i = PromotedProtobufData.get().resolveUnion(schemaStruct, p.getStruct());
    assertEquals(1, i);
  }

  // `validate` is a public method.
  @Test
  public void testValidate_ListValue() {
    Schema schema = ProtobufData.get().getSchema(ListValue.class);
    ListValue lv =
        ListValue.newBuilder()
            .addValues(Value.newBuilder().setStringValue("a"))
            .addValues(Value.newBuilder().setNumberValue(1))
            .build();
    assertTrue(PromotedProtobufData.get().validate(schema, lv));
  }

  @Test
  public void testValidate_ListValue1() {
    Schema schema = PromotedProtobufData.get().getSchema(ListValue1.class);
    ListValue1 lv =
        ListValue1.newBuilder()
            .addValues(Value1.newBuilder().setStringValue("a"))
            .addValues(Value1.newBuilder().setNumberValue(1))
            .build();
    assertTrue(PromotedProtobufData.get().validate(schema, lv));
  }

  // `validate` is a public method.
  @Test
  public void testValidate_ValueContainingListValue() {
    Schema schema = ProtobufData.get().getSchema(Value.class);
    Value v =
        Value.newBuilder()
            .setListValue(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("a"))
                    .addValues(Value.newBuilder().setNumberValue(1)))
            .build();
    assertTrue(PromotedProtobufData.get().validate(schema, v));
  }

  @Test
  public void testValidate_ValueContainingListValue1() {
    Schema schema = PromotedProtobufData.get().getSchema(Value1.class);
    Value1 v =
        Value1.newBuilder()
            .setListValue(
                ListValue2.newBuilder()
                    .addValues(Value2.newBuilder().setStringValue("a"))
                    .addValues(Value2.newBuilder().setNumberValue(1)))
            .build();
    assertTrue(PromotedProtobufData.get().validate(schema, v));
  }
}
