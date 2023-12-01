package ai.promoted.metrics.logprocessor.common.fakedatagenerator.content;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.Map;

/**
 * Represents any test content: Item, Store and Promotion.
 *
 * <p>This {@code Content} does not map 1:1 to Content in the Content Store. This {@code Content}
 * represents the whole entity for fake data generation. It represents both the Content fields
 * passed on the Request and Content fields retrieved through DocumentDB Content Store.
 */
@AutoValue
public abstract class Content implements Serializable {
  public static final String TYPE = "type";
  private static final long serialVersionUID = 1L;

  public static Builder builder() {
    return new AutoValue_Content.Builder();
  }

  public abstract String contentId();

  /* Fields that are passed in on the Request. */
  public abstract ImmutableMap<String, String> requestFields();

  /* Fields that are passed in through DocumentDB. */
  public abstract ImmutableMap<String, String> documentFields();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setContentId(String contentId);

    public abstract Builder setRequestFields(Map<String, String> fields);

    abstract ImmutableMap.Builder<String, String> requestFieldsBuilder();

    public final Builder addRequestField(String key, String value) {
      requestFieldsBuilder().put(key, value);
      return this;
    }

    public abstract Builder setDocumentFields(Map<String, String> fields);

    abstract ImmutableMap.Builder<String, String> documentFieldsBuilder();

    public final Builder addDocumentField(String key, String value) {
      documentFieldsBuilder().put(key, value);
      return this;
    }

    public abstract Content build();
  }
}
