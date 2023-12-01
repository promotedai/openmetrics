package ai.promoted.metrics.logprocessor.common.functions.filter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.promoted.proto.delivery.FeatureStage;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.delivery.internal.features.Features;
import ai.promoted.proto.event.JoinedImpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class BuyerPredicateTest {

  BuyerPredicate<JoinedImpression> filter =
      new BuyerPredicate<JoinedImpression>(
          ImmutableList.of(
              // User property "is_host" - Hipcamp
              6102540093495235004L,
              // User property "is_staff" - Hipcamp
              4078401918190321518L),
          JoinedImpression::getApiExecutionInsertion);

  @Test
  public void test() {
    assertTrue(filter.test(createJoinedEvent(createApiExecutionInsertion(ImmutableMap.of()))));
    assertTrue(
        filter.test(createJoinedEvent(createApiExecutionInsertion(ImmutableMap.of(1L, 1L)))));
    assertTrue(
        filter.test(
            createJoinedEvent(
                createApiExecutionInsertion(ImmutableMap.of(6102540093495235004L, 0L)))));
    assertFalse(
        filter.test(
            createJoinedEvent(
                createApiExecutionInsertion(ImmutableMap.of(6102540093495235004L, 1L)))));
  }

  private JoinedImpression createJoinedEvent(Insertion apiExecutionInsertion) {
    return JoinedImpression.newBuilder().setApiExecutionInsertion(apiExecutionInsertion).build();
  }

  private Insertion createApiExecutionInsertion(Map<Long, Long> sparseIds) {
    return Insertion.newBuilder()
        .setFeatureStage(
            FeatureStage.newBuilder().setFeatures(Features.newBuilder().putAllSparseId(sparseIds)))
        .build();
  }
}
