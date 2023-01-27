package ai.promoted.metrics.logprocessor.job.join;

import ai.promoted.metrics.logprocessor.common.functions.inferred.MergeImpressionDetails;
import ai.promoted.metrics.logprocessor.common.util.StringUtil;
import ai.promoted.proto.common.Properties;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * Pulls other content IDs from Properties.
 * Other content IDs are foreign keys that are used when handling hierarchical content joins.
 */
final class OtherContentIdsConverter implements Serializable {
    // TODO - add serializable version.
    private static final Logger LOGGER = LogManager.getLogger(MergeImpressionDetails.class);

    /** The list of Property keys (e.g. "storeId") that identify other content IDs. */
    private final List<String> propertyKeys;
    private final List<Integer> propertyKeyHashes;

    OtherContentIdsConverter(List<String> propertyKeys) {
        this.propertyKeys = ImmutableList.copyOf(propertyKeys);
        propertyKeyHashes = propertyKeys.stream()
                .map(key -> StringUtil.hash(key))
                .collect(ImmutableList.toImmutableList());
    }

    boolean hasKeys() {
        return !propertyKeys.isEmpty();
    }

    // For now, we only support otherContentIds for one level in the Struct.
    void putFromProperties(
            BiConsumer<Integer, String> putOtherContentIds,
            Properties properties) {
        Struct struct = properties.getStruct();
        for (int i = 0; i < propertyKeys.size(); i++) {
            String key = propertyKeys.get(i);
            Value value = struct.getFieldsMap().get(key);
            if (value != null) {
                int hash = propertyKeyHashes.get(i);
                if (value.hasStringValue()) {
                    putOtherContentIds.accept(hash, value.getStringValue());
                } else if (value.hasNumberValue()) {
                    putOtherContentIds.accept(hash, Double.toString(value.getNumberValue()));
                } else if (value.hasBoolValue()) {
                    putOtherContentIds.accept(hash, Boolean.toString(value.getBoolValue()));
                } else {
                    putOtherContentIds.accept(hash, "");
                }
            }
        }
    }
}
