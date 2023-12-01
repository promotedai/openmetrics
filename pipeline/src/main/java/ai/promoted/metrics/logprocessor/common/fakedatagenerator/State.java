package ai.promoted.metrics.logprocessor.common.fakedatagenerator;

import ai.promoted.proto.event.LogRequest;
import com.google.auto.value.AutoValue;
import java.time.Instant;
import java.util.Optional;

/**
 * An internal value for the LogRequestIterator's heap so we can track internal state along and with
 * background runnables to create more state. These are kept in the same Heap to simplify
 * synchronization of the two heaps.
 */
@AutoValue
abstract class State {
  /**
   * Dan's code comments.
   *
   * <p>Dan wants flags to specify these cases: 1) Hipcamp - Park -> Campground. 2) Snackpass ->
   * Promotion Feed -> Item in store ->
   *
   * <p>TODO - keep more state around for IDs and stuff in IteratorValue.
   *
   * <p>Design: - The iterator functions should be simple. - The Supplier gets registered with the
   * last state. It doesn't get executed until the timer is hit.
   */
  static State create(Instant time, int index, LogRequest external) {
    return new AutoValue_State(time, index, Optional.of(external), () -> {});
  }

  static State create(Instant time, int index, Runnable runnable) {
    return new AutoValue_State(time, index, Optional.empty(), runnable);
  }

  abstract Instant time();

  // A global order of state.  Used a secondary sort in case times are the same.
  abstract int index();

  abstract Optional<LogRequest> logRequest();

  /** Should be called when processing state. Might be a no-op Runnable. */
  abstract Runnable runnable();
}
