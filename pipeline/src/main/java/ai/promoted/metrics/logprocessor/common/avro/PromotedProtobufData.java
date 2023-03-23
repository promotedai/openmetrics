package ai.promoted.metrics.logprocessor.common.avro;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.protobuf.ProtobufData;

/** ProtobufData that contains Promoted additions: - Supports UNRECOGNIZED enum values. */
public class PromotedProtobufData extends ProtobufData {
  private static final PromotedProtobufData INSTANCE = new PromotedProtobufData();

  protected PromotedProtobufData() {}

  public static PromotedProtobufData get() {
    return INSTANCE;
  }

  /** Modified version of ProtobufData that also includes UNRECOGNIZED. */
  @Override
  public Schema getSchema(Descriptors.EnumDescriptor d) {
    List<String> symbols = new ArrayList(d.getValues().size() + 1);
    Iterator var3 = d.getValues().iterator();

    while (var3.hasNext()) {
      Descriptors.EnumValueDescriptor e = (Descriptors.EnumValueDescriptor) var3.next();
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
  protected boolean isEnum(Object datum) {
    return super.isEnum(datum) || datum instanceof EnumValueDescriptor;
  }
}
