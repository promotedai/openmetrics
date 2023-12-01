package ai.promoted.metrics.logprocessor.common.functions.content.common;

import ai.promoted.metrics.logprocessor.common.functions.base.SerializableFunction;
import java.util.Map;

/**
 * Interface for calling Content API's /v2/content/query
 *
 * <p>Value = Map of contentId to "source" to projected props.
 */
public interface ContentQueryClient
    extends SerializableFunction<
        ContentQueryParams, Map<String, Map<String, Map<String, Object>>>> {}
