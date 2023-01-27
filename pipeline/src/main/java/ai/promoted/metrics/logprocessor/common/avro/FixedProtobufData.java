package ai.promoted.metrics.logprocessor.common.avro;

import ai.promoted.proto.delivery.FlatQualityScoreConfig;
import org.apache.avro.Schema;
import com.google.protobuf.Descriptors.Descriptor;
import ai.promoted.proto.common.FlatProperties;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;


/**
 * FixedProtobufData helps us change our Avro schemas to work with AWS Glue
 * because Glue does not support recursive schemas.
 */
// TODO: remove this class and merge back into PromotedProtobufData
public class FixedProtobufData extends PromotedProtobufData {
    private static final FixedProtobufData INSTANCE = new FixedProtobufData();

    public static FixedProtobufData get() {
        return INSTANCE;
    }

    private final List<String> structUnionedFullNames = getUnionedFullNames("ai.promoted.proto.common.Struct%d", 5);
    private final List<String> listValueUnionedFullNames = getUnionedFullNames("ai.promoted.proto.common.ListValue%d", 5);
    private final List<String> valueUnionedFullNames = getUnionedFullNames("ai.promoted.proto.common.Value%d", 5);
    private final List<String> qualityScoreTermsUnionedFullNames = getUnionedFullNames("ai.promoted.proto.delivery.QualityScoreTerms%d", 5);
    private final List<String> qualityScoreTermUnionedFullNames = getUnionedFullNames("ai.promoted.proto.delivery.QualityScoreTerm%d", 5);

    @Override
    public Schema getSchema(Descriptor descriptor) {
        if (descriptor.getFullName().equals("common.Properties")) {
            return getSchema(FlatProperties.class);
        }
        if (descriptor.getFullName().equals("delivery.QualityScoreConfig")) {
            return getSchema(FlatQualityScoreConfig.class);
        }

        return super.getSchema(descriptor);
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
                case "ai.promoted.proto.delivery.QualityScoreTerms":
                    i = matchLevel(union, qualityScoreTermsUnionedFullNames);
                    break;
                case "ai.promoted.proto.delivery.QualityScoreTerm":
                    i = matchLevel(union, qualityScoreTermUnionedFullNames);
                    break;
            }
            if (i != null) return i;
        }
        return super.resolveUnion(union, datum);
    }

    // Match fixed-level Avro schema name with Struct Protobuf class
    // e.g., Struct->Struct1, ListValue->ListValue3, Value->Value4, etc.
    // returns null if not found (too many levels of nesting).
    // Converts a 0-based List index to a 1-based Avro schema index.
    private Integer matchLevel(Schema union, List<String> fullNames) {
        Integer level = null;
        for (String fullName: fullNames) {
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
