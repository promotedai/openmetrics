package ai.promoted.metrics.logprocessor.common.functions.inferred;

import ai.promoted.metrics.logprocessor.common.util.DebugIds;
import com.google.auto.value.AutoValue;
import com.google.auto.value.extension.memoized.Memoized;
import java.io.Serializable;
import java.time.Duration;

/** Standard runtime options for inference functions. */
@AutoValue
public abstract class Options implements Serializable {
  abstract boolean skipJoin();

  abstract boolean rightOuterJoin();

  abstract Duration maxTime();

  abstract Duration maxOutOfOrder();

  abstract boolean checkLateness();

  abstract int idJoinDurationMultiplier();

  abstract DebugIds debugIds();

  public static Builder builder() {
    return new AutoValue_Options.Builder()
        .setSkipJoin(false)
        .setRightOuterJoin(false)
        .setCheckLateness(true)
        .setIdJoinDurationMultiplier(1)
        .setDebugIds(DebugIds.empty());
  }

  @AutoValue.Builder
  public abstract static class Builder {
    /**
     * If true, skips the join and passes the RHS through. Instead of removing the inferred ref
     * operators from the job graph, we can just skip the join in the operator. This has two
     * benefits: 1. It'll be easier to support multi-tenancy if we don't change the Flink graph per
     * platform. 2. We still need to flatten TinyDeliveryLog into TinyEvents. The code for this is
     * already in BaseInferred. Even if we skip inferred references, we'd still need an operator to
     * flatten the records.
     */
    public abstract Builder setSkipJoin(boolean skipJoin);
    /** If true, performs a right outer join. */
    public abstract Builder setRightOuterJoin(boolean rightOuterJoin);
    /** Maximum time to wait to do an in-order join (time to see the RHS). */
    public abstract Builder setMaxTime(Duration value);
    /** Maximum time to wait to do an out of order join (time to see the delayed LHS). */
    public abstract Builder setMaxOutOfOrder(Duration value);
    /** If true, performs lateness/ordering checks; defaults to true. */
    public abstract Builder setCheckLateness(boolean value);
    /** Multiplicative factor for max time durations to clean id join state; defaults to 1. */
    public abstract Builder setIdJoinDurationMultiplier(int value);
    /** DebugIds to ouptut debug information on */
    public abstract Builder setDebugIds(DebugIds value);

    // Getters on builder to normalize input.
    abstract Duration maxTime();

    abstract Duration maxOutOfOrder();

    abstract Options autoBuild();

    public Options build() {
      setMaxTime(maxTime().abs());
      setMaxOutOfOrder(maxOutOfOrder().abs());
      return autoBuild();
    }
  }

  @Memoized
  long maxTimeMillis() {
    return maxTime().toMillis();
  }

  @Memoized
  long maxOutOfOrderMillis() {
    return maxOutOfOrder().toMillis();
  }
}
