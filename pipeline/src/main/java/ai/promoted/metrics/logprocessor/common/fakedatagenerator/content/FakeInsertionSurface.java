package ai.promoted.metrics.logprocessor.common.fakedatagenerator.content;

import com.google.auto.value.AutoValue;

/** Fake data generator configuration for an InsertionSurface (e.g. UseCase and content type). */
@AutoValue
public abstract class FakeInsertionSurface {
  public abstract ContentType contentType();

  public abstract boolean logImpressions();

  public abstract boolean logNavigates();

  public static FakeInsertionSurface create(
      ContentType contentType, boolean logImpressions, boolean logNavigates) {
    return new AutoValue_FakeInsertionSurface(contentType, logImpressions, logNavigates);
  }
}
