package ai.promoted.metrics.logprocessor.common.avro;

import ai.promoted.proto.common.FlatProperties;
import ai.promoted.proto.delivery.FlatRequest;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.protobuf.ProtobufData;

/**
 * ProtobufData that contains Promoted additions:
 *
 * <ol>
 *   <li>Supports UNRECOGNIZED enum values.
 *   <li>Helps us change our Avro schemas to work with AWS Glue because Glue does not support
 *       recursive schemas.
 * </ol>
 */
public class PromotedProtobufData extends ProtobufData {
  private static final PromotedProtobufData INSTANCE = new PromotedProtobufData();
  private static final Schema NULL = Schema.create(Schema.Type.NULL);
  private final List<String> structUnionedFullNames =
      getUnionedFullNames("ai.promoted.proto.common.Struct%d", 5);
  private final List<String> listValueUnionedFullNames =
      getUnionedFullNames("ai.promoted.proto.common.ListValue%d", 5);
  private final List<String> valueUnionedFullNames =
      getUnionedFullNames("ai.promoted.proto.common.Value%d", 5);

  protected PromotedProtobufData() {}

  public static PromotedProtobufData get() {
    return INSTANCE;
  }

  /** Makes almost all types nullable. */
  @Override
  public Schema getSchema(Descriptors.FieldDescriptor f) {
    Schema s = getNonRepeatedSchema(f);
    /*
     * (Note that when a default value is specified for a record field whose type is a union, the
     * type of the default value must match the first element of the union. Thus, for unions
     * containing "null", the "null" is usually listed first, since the default value of such unions
     * is typically null.)
     */
    if (f.getType() != Descriptors.FieldDescriptor.Type.MESSAGE) {
      s = Schema.createUnion(Arrays.asList(s, NULL));
    } else if (f.isOptional()) {
      s = Schema.createUnion(Arrays.asList(NULL, s));
    }
    if (f.isRepeated()) {
      s = Schema.createUnion(Schema.createArray(s), NULL);
    }
    return s;
  }

  private Schema getNonRepeatedSchema(Descriptors.FieldDescriptor f) {
    // Copied from the parent class since it has private access.
    Schema result;
    switch (f.getType()) {
      case BOOL:
        return Schema.create(Schema.Type.BOOLEAN);
      case FLOAT:
        return Schema.create(Schema.Type.FLOAT);
      case DOUBLE:
        return Schema.create(Schema.Type.DOUBLE);
      case STRING:
        Schema s = Schema.create(Schema.Type.STRING);
        GenericData.setStringType(s, GenericData.StringType.String);
        return s;
      case BYTES:
        return Schema.create(Schema.Type.BYTES);
      case INT32:
      case UINT32:
      case SINT32:
      case FIXED32:
      case SFIXED32:
        return Schema.create(Schema.Type.INT);
      case INT64:
      case UINT64:
      case SINT64:
      case FIXED64:
      case SFIXED64:
        return Schema.create(Schema.Type.LONG);
      case ENUM:
        return getSchema(f.getEnumType());
      case MESSAGE:
        result = getSchema(f.getMessageType());
        // The logic of wrapping to union is moved to getSchema().
        // if (f.isOptional())
        //   // wrap optional record fields in a union with null
        //   result = Schema.createUnion(Arrays.asList(NULL, result));
        return result;
      case GROUP: // groups are deprecated
      default:
        throw new UnsupportedOperationException("Unexpected type: " + f.getType());
    }
  }
  /** Modified version of ProtobufData that also includes UNRECOGNIZED. */
  @Override
  public Schema getSchema(Descriptors.EnumDescriptor d) {
    List<String> symbols = new ArrayList<>(d.getValues().size() + 1);

    for (EnumValueDescriptor e : d.getValues()) {
      symbols.add(e.getName());
    }
    // Map all unrecognized values to UNRECOGNIZED.
    // PR - The Avro numeric value for the unrecognized value will change as we add more values.
    // This is fine for Avro.
    // If we want to keep the values the same, it might make sense to put UNRECOGNIZED first.
    symbols.add("UNRECOGNIZED");
    return Schema.createEnum(
        d.getName(), null, this.getNamespace(d.getFile(), d.getContainingType()), symbols);
  }

  @Override
  protected Object getField(Object record, String name, int pos, Object state) {
    Message m = (Message) record;
    Descriptors.FieldDescriptor f = ((Descriptors.FieldDescriptor[]) state)[pos];
    switch (f.getType()) {
      case MESSAGE:
        if (!f.isRepeated() && !m.hasField(f)) return null;
      default:
        Object result = m.getField(f);
        if (result instanceof Descriptors.EnumValueDescriptor) {
          return createEnum(result.toString(), getSchema(((EnumValueDescriptor) result).getType()));
        }
        return result;
    }
  }

  @Override
  public int resolveUnion(Schema union, Object datum) {
    if (datum != null && isRecord(datum)) {
      Integer i = null;
      String name = getRecordSchema(datum).getFullName();
      switch (name) {
        case "com.google.protobuf.Struct":
          i = matchLevel(union, structUnionedFullNames);
          break;
        case "com.google.protobuf.ListValue":
          i = matchLevel(union, listValueUnionedFullNames);
          break;
        case "com.google.protobuf.Value":
          i = matchLevel(union, valueUnionedFullNames);
          break;
      }
      if (i != null) return i;
    }
    return super.resolveUnion(union, datum);
  }

  @Override
  public Schema getSchema(Descriptors.Descriptor descriptor) {
    // The structure of these Data classes makes it difficult to swap out a recursive type directly.
    // The code needs to swap out the non-recursive wrapper here.
    if (descriptor.getFullName().equals("common.Properties")) {
      return getSchema(FlatProperties.class);
    }
    if (descriptor.getFullName().equals("delivery.Request")) {
      return getSchema(FlatRequest.class);
    }
    return super.getSchema(descriptor);
  }

  // Match fixed-level Avro schema name with Struct Protobuf class
  // e.g., Struct->Struct1, ListValue->ListValue3, Value->Value4, etc.
  // returns null if not found (too many levels of nesting).
  // Converts a 0-based List index to a 1-based Avro schema index.
  private Integer matchLevel(Schema union, List<String> fullNames) {
    Integer level = null;
    for (String fullName : fullNames) {
      level = union.getIndexNamed(fullName);
      if (level != null) break;
    }
    return level;
  }

  private List<String> getUnionedFullNames(String fmt, int levels) {
    return IntStream.range(1, levels + 1)
        // Intern it to speed up string comparisons.
        .mapToObj(level -> String.format(fmt, level).intern())
        .collect(Collectors.toList());
  }
}
