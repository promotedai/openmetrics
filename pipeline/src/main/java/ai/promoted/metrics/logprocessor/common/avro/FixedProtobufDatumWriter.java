package ai.promoted.metrics.logprocessor.common.avro;

import org.apache.avro.Schema;

/** Replace ProtobufData singleton with ProtobufDataFixedProperties singleton */
// TODO: remove this class and merge back into PromotedProtobufDatumWriter
public class FixedProtobufDatumWriter<T> extends PromotedProtobufDatumWriter<T> {

    public FixedProtobufDatumWriter(Class<T> c) {
        super(FixedProtobufData.get().getSchema(c), PromotedProtobufData.get());
    }

    public FixedProtobufDatumWriter(Schema schema) {
        super(schema, FixedProtobufData.get());
    }
}
