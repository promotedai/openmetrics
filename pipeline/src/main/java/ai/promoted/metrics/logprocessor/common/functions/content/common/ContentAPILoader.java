package ai.promoted.metrics.logprocessor.common.functions.content.common;

import ai.promoted.metrics.logprocessor.common.functions.SerializableFunction;
import java.util.Collection;
import java.util.Map;

/** Interface for loading content details from the Content API. */
public interface ContentAPILoader
    extends SerializableFunction<Collection<String>, Map<String, Map<String, Object>>> {}
