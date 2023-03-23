package ai.promoted.metrics.logprocessor.common.functions.content.common;

import ai.promoted.metrics.logprocessor.common.functions.SerializableBiFunction;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** Loads content details from the Content API. */
public final class EmptyContentAPILoader
    implements SerializableBiFunction<String, Executor, CompletableFuture<Map<String, String>>> {

  @Override
  public CompletableFuture<Map<String, String>> apply(String contentId, Executor executor) {
    return CompletableFuture.completedFuture(ImmutableMap.of());
  }
}
