package ai.promoted.metrics.logprocessor.common.functions;

import ai.promoted.proto.delivery.Insertion;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.apache.flink.api.common.functions.MapFunction;
import ai.promoted.proto.delivery.DeliveryLog;

/**
 * Moves {@code Request.insertion_matrix_headers} and {@code Request.insertion_matrix} onto each {@code Request.insertion.properties.struct}.
 */
public class RestructureDeliveryLog implements MapFunction<DeliveryLog, DeliveryLog> {
    @Override
    public DeliveryLog map(DeliveryLog deliveryLog) {
        var deliveryLogBuilder = deliveryLog.toBuilder();
        var request = deliveryLogBuilder.getRequestBuilder();
        if (request.getInsertionMatrixHeadersCount() > 0 && request.getInsertionMatrix().getValuesCount() > 0) {
            request.clearInsertion();
            for (var insertion : request.getInsertionMatrix().getValuesList()) {
                var valueList = insertion.getListValue();
                var insertionBuilder = Insertion.newBuilder();
                // TODO(Dan) - refactor and move validation logic to the raw job.
                int minSize = Math.min(request.getInsertionMatrixHeadersCount(), valueList.getValuesCount());
                for (int i = 0; i < minSize; ++i) {
                    // We validate an equal number of headers and values before this point.
                    String header = request.getInsertionMatrixHeaders(i);
                    Value value = valueList.getValues(i);
                    // Fast-paths for legacy headers.
                    if (header.equals("contentId")) {
                        insertionBuilder.setContentId(value.getStringValue());
                        continue;
                    }
                    if (header.equals("retrievalRank")) {
                        insertionBuilder.setRetrievalRank((long) value.getNumberValue());
                        continue;
                    }
                    if (header.equals("retrievalScore")) {
                        insertionBuilder.setRetrievalScore((float) value.getNumberValue());
                        continue;
                    }
                    // Each "." in the header scopes into a sub-struct.
                    String[] parts = header.split("\\.");
                    traverseStructs(insertionBuilder.getPropertiesBuilder().getStructBuilder(), 0, parts, value);
                }
                request.addInsertion(insertionBuilder);
            }
            // Cleared to simplify testing.
            request.clearInsertionMatrixHeaders();
            request.clearInsertionMatrix();
        }
        return deliveryLogBuilder.build();
    }

    public Struct.Builder traverseStructs(Struct.Builder structBuilder, int offset, String[] parts, Value leafValue) {
        String part = parts[offset];
        // Leaf case.
        if (offset == parts.length - 1) {
            structBuilder.putFields(part, leafValue);
            return structBuilder;
        }
        Value value = structBuilder.getFieldsMap().get(part);
        Struct.Builder subStructBuilder;
        // If the sub-struct exists (and actually is a struct), don't overwrite it.
        if (value != null && value.hasStructValue()) {
            subStructBuilder = value.getStructValue().toBuilder();
        } else {
            subStructBuilder = Struct.newBuilder();
        }
        // Protobuf `map` is implemented as `Map` in Java. Protobuf doesn't offer any way of accessing builders through
        // `map` (maybe short of some sketchy descriptor casting) because the behavior of `Map` is undefined in such
        // cases. Instead, at each such point, we copy the map value to a new builder, modify it, and then put it back
        // in for the original key.
        subStructBuilder = traverseStructs(subStructBuilder, offset + 1, parts, leafValue);
        structBuilder.putFields(part, Value.newBuilder().setStructValue(subStructBuilder).build());
        return structBuilder;
    }

}
