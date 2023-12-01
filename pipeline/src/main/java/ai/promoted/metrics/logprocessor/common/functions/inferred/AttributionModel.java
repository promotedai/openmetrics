package ai.promoted.metrics.logprocessor.common.functions.inferred;

/**
 * Represents an Attribution Model.
 *
 * <p>TODO - Add time windows.
 *
 * <p>TODO - Support static and dynamic attribution models.
 */
public enum AttributionModel {
  LATEST(1L),
  EVEN(2L);

  public final long id;

  AttributionModel(long id) {
    this.id = id;
  }
}
