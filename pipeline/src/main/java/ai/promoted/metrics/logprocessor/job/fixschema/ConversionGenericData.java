package ai.promoted.metrics.logprocessor.job.fixschema;

import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.UnresolvedUnionException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * This is a modified version of GenericData that supports the required type of schema fixes. It
 * supports changing the recursive protos to non-recursive protos. It also matches fields based on
 * names (slower but supports switching positions).
 */
class ConversionGenericData extends GenericData {

  private final List<Tuple2<String, String>> oldToNewRecordPrefixes;

  public ConversionGenericData(List<Tuple2<String, String>> oldToNewRecordPrefixes) {
    this.oldToNewRecordPrefixes = oldToNewRecordPrefixes;
  }

  // Overrides a static on GenericData.
  public static ConversionGenericData get() {
    throw new UnsupportedOperationException("call the constructor instead");
  }

  @Override
  protected Object getRecordState(Object record, Schema schema) {
    // Only way to pass the new schema into getField.
    return schema;
  }

  protected Object getField(Object record, String name, int pos, Object state) {
    IndexedRecord indexedRecord = ((IndexedRecord) record);
    Schema.Field field = indexedRecord.getSchema().getField(name);
    if (field == null) {
      Schema newSchema = (Schema) state;
      return newSchema.getField(name).defaultVal();
    }
    return ((IndexedRecord) record).get(field.pos());
  }

  @Override
  public Object getField(Object record, String name, int position) {
    throw new UnsupportedOperationException("used the other getField function");
  }

  @Override
  public int resolveUnion(Schema union, Object datum) {
    try {
      return super.resolveUnion(union, datum);
    } catch (UnresolvedUnionException e) {
      String oldSchemaName = getSchemaName(datum);
      List<Schema> newSchemas = union.getTypes();
      for (int i = 0; i < newSchemas.size(); i++) {
        Schema newSchema = newSchemas.get(i);
        if (newSchema.getType() == Type.RECORD) {
          String newSchemaName = newSchema.getFullName();
          for (Tuple2<String, String> oldToNewRecordPrefix : oldToNewRecordPrefixes) {
            if (oldSchemaName.equals(oldToNewRecordPrefix.f0)
                && newSchemaName.startsWith(oldToNewRecordPrefix.f1)) {
              return i;
            }
          }
        }
      }
      throw new UnresolvedUnionException(union, datum);
    }
  }
}
