package ai.promoted.metrics.logprocessor.common.fakedatagenerator.content;

import com.google.auto.value.AutoValue;

import java.util.Map;
import java.util.Optional;

/**
 * Filter for ContentDB data.
 */
@AutoValue
public abstract class ContentDBFilter {
    abstract Optional<String> contentId();
    abstract Optional<String> fieldKey();
    abstract Optional<String> fieldValue();

    public static ContentDBFilter whereContentIdIs(String contentId) {
        return new AutoValue_ContentDBFilter(Optional.of(contentId), Optional.empty(), Optional.empty());
    }

    public static ContentDBFilter whereFieldIs(String key, String value) {
        return new AutoValue_ContentDBFilter(Optional.empty(), Optional.of(key), Optional.of(value));
    }
}