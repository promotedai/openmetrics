package ai.promoted.metrics.logprocessor.common.fakedatagenerator;

import ai.promoted.proto.event.LogRequest;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.Pair;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * An internal value for the LogRequestIterator's heap so we can track internal state along and with
 * background runnables to create more state.  These are kept in the same Heap to simplify synchronization
 * of the two heaps.
 */
 @AutoValue
abstract class State {
    abstract Instant time();
    // A global order of state.  Used a secondary sort in case times are the same.
    abstract int index();
    abstract Optional<LogRequest> logRequest();

    /**
     * Should be called when processing state.  Might be a no-op Runnable.
     */
    abstract Runnable runnable();

    /**
     Dan's code comments.

     Dan wants flags to specify these cases:
     1) ccc - Park -> Campground.
     2) bbb -> Promotion Feed -> Item in store ->

     TODO - keep more state around for IDs and stuff in IteratorValue.

     Design:
     - The iterator functions should be simple.
     - The Supplier gets registered with the last state.  It doesn't get executed until the timer is hit.
     */

    static State create(Instant time, int index, LogRequest external) {
        return new AutoValue_State(time, index, Optional.of(external), () -> {});
    }

    static State create(Instant time, int index, Runnable runnable) {
        return new AutoValue_State(time, index, Optional.empty(), runnable);
    }
}
