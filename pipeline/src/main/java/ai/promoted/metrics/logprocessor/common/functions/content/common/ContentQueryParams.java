package ai.promoted.metrics.logprocessor.common.functions.content.common;

import com.google.auto.value.AutoValue;
import java.util.List;

/** Params for the /v2/content/query call. */
@AutoValue
public abstract class ContentQueryParams {
  public static Builder builder() {
    return new AutoValue_ContentQueryParams.Builder();
  }

  public abstract List<String> contentIds();

  // Projections is an optional list of paths to restrict to in the query.
  public abstract List<String> projections();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setContentIds(List<String> contentIds);

    // Projections is an optional list of paths to restrict to in the query.
    public abstract Builder setProjections(List<String> projections);

    public abstract ContentQueryParams build();
  }
}
