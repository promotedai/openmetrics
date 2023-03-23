package ai.promoted.metrics.logprocessor.common.functions.filter;

import ai.promoted.metrics.logprocessor.common.functions.SerializableFunction;
import ai.promoted.metrics.logprocessor.common.functions.SerializablePredicate;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.delivery.internal.features.Features;
import java.util.List;

/**
 * Predicate that is used to keep (true) or filter out (false) flat events that have a sparseInt
 * matching one of the following.
 *
 * <p>This is inlined for now in the code to avoid having a different DB.
 *
 * <p>To calculate the hash, use go-hashlib and modifying a unit test to find the sparseInt.
 *
 * <p>Known issues
 *
 * <ul>
 *   <li>This relies on having an API Execution Insertion. This does not catch the case where we are
 *       missing API Execution Insertions (e.g. not calling shadow traffic or we have a bug in the
 *       join). For now, Dan thinks this is okay. When we want to support other cases, we either
 *       need to join User DB in Flink our get them to specify UserInfo.ignore_usage.
 * </ul>
 */
public final class BuyerPredicate<T> implements SerializablePredicate<T> {

  private static final long serialVersionUID = 1;

  private final long[] nonBuyerUserSparseHashes;
  private final SerializableFunction<T, Insertion> toExecutionInsertion;

  public BuyerPredicate(
      List<Long> nonBuyerUserSparseHashes,
      SerializableFunction<T, Insertion> toExecutionInsertion) {
    this.nonBuyerUserSparseHashes = nonBuyerUserSparseHashes.stream().mapToLong(l -> l).toArray();
    this.toExecutionInsertion = toExecutionInsertion;
  }

  @Override
  public boolean test(T t) {
    Insertion insertion = toExecutionInsertion.apply(t);
    Features features = insertion.getFeatureStage().getFeatures();
    for (long hash : nonBuyerUserSparseHashes) {
      if (features.getSparseIdOrDefault(hash, 0L) == 1L) {
        return false;
      }
    }
    return true;
  }
}
