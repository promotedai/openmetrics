package ai.promoted.metrics.logprocessor.common.fakedatagenerator.content;

import com.google.auto.value.AutoValue;

/**
 * {@code Content} that has an Insertion.  Separate type to have type safety help
 * detect potential bugs.
 */
@AutoValue
public abstract class InsertedContent {
    public abstract Content content();
    public abstract String responseInsertionId();

    public String getContentId() {
        return content().contentId();
    }

    public static Builder builder() {
        return new AutoValue_InsertedContent.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract Builder setContent(Content content);
        public abstract Builder setResponseInsertionId(String responseInsertionId);
        public abstract InsertedContent build();
    }
}
