package ai.promoted.metrics.logprocessor.common.avro;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.Descriptors;
import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;
import org.apache.avro.protobuf.ProtobufDatumWriter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** A version of ProtobufDatumWriter that supports unrecognized enum values. */
public class PromotedProtobufDatumWriter<T> extends ProtobufDatumWriter<T> {
    /**
     * Unrecognized enum values get their own EnumValueDescriptors.
     * This complicates the Proto Avro code.
     * To support this case, we cache the mapping from EnumValueDescriptors to symbols.
     * There doesn't appear to be clean API to differentiate between recognized and unrecognized.
     * We'll use a cache.  EnumValueDescriptor equals/hashcode is the default Object implementations.
     * This appears to be safe based on the current Protobuf GeneratedMessageV3 code.  If this structure
     * changes, it'll most likely just be slower.
     */
    private static final int MAX_ENUM_DESCRIPTOR_CACHE_SIZE = 1000;

    private transient LoadingCache<Descriptors.EnumValueDescriptor, String> descriptorToSymbol;

    public PromotedProtobufDatumWriter(Schema root) {
        this(root, PromotedProtobufData.get());
    }

    public PromotedProtobufDatumWriter(Schema root, PromotedProtobufData protobufData) {
        super(root, protobufData);
        // TODO - LRU?
        descriptorToSymbol = CacheBuilder.newBuilder()
                .maximumSize(MAX_ENUM_DESCRIPTOR_CACHE_SIZE)
                .build(new CacheLoader<Descriptors.EnumValueDescriptor, String>() {
                    @Override
                    public String load(final Descriptors.EnumValueDescriptor descriptor) throws Exception {
                        // Assumes core descriptors are reused.
                        Optional<Descriptors.EnumValueDescriptor> matchingDescriptor = descriptor.getType().getValues().stream()
                                .filter(valueDescriptor -> valueDescriptor == descriptor)
                                .findFirst();
                        if (matchingDescriptor.isPresent()) {
                            return descriptor.getName();
                        } else {
                            return "UNRECOGNIZED";
                        }
                    }
                });
    }

    protected void writeEnum(Schema schema, Object datum, Encoder out) throws IOException {
        if (!(datum instanceof Descriptors.EnumValueDescriptor)) {
            super.writeEnum(schema, datum, out);
        } else {
            Descriptors.EnumValueDescriptor descriptor = (Descriptors.EnumValueDescriptor) datum;
            out.writeEnum(schema.getEnumOrdinal(descriptorToSymbol.getUnchecked(descriptor)));
        }
    }
}
