package ai.promoted.metrics.logprocessor.common.fakedatagenerator.content;

import com.google.auto.value.AutoValue;

/**
 * {@code Content} that has an Impression. Separate type to have type safety help detect potential
 * bugs.
 */
@AutoValue
public abstract class ImpressedContent {
  public abstract InsertedContent insertedContent();

  public abstract String impressionId();

  // TODO - does this make sense anymore?
  public String getContentId() {
    return insertedContent().getContentId();
  }

  public String id() {
    return insertedContent().content().contentId();
  }

  public String getResponseInsertionId() {
    return insertedContent().responseInsertionId();
  }

  public static Builder builder() {
    return new AutoValue_ImpressedContent.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setInsertedContent(InsertedContent insertedContent);

    public abstract Builder setImpressionId(String impressionId);

    public abstract ImpressedContent build();
  }
}
