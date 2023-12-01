package ai.promoted.metrics.logprocessor.common.functions.content.datastream;

import ai.promoted.metrics.logprocessor.common.functions.content.common.ContentQueryClient;
import ai.promoted.metrics.logprocessor.common.functions.content.common.ContentQueryParams;
import ai.promoted.metrics.logprocessor.common.util.StringUtil;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.flink.util.function.SerializableSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** A function wrapper that caches the Content API lookups for joining in other Content IDs. */
public final class ContentCacheSupplier
    implements SerializableSupplier<AsyncLoadingCache<String, Map<Integer, String>>>, Closeable {
  private static final Logger LOGGER = LogManager.getLogger(ContentCacheSupplier.class);
  private final int maxCacheSize;
  private final int maxBatchSize;
  private final Duration maxBatchDelay;
  private final Duration expiration;
  private final ContentQueryClient contentQueryClient;
  private final List<String> matchingKeys;
  private final Set<Integer> intMatchingKeys;
  // Null means to use the `CompletableFuture.supplyAsync`.  `supplyAsync` does not serialize.
  private final Object supplierLock = new Serializable() {};

  private transient BulkLoader<String, Map<Integer, String>> bulkLoader;

  @Nullable
  private final Function<
          Supplier<Map<String, Map<Integer, String>>>,
          CompletableFuture<Map<String, Map<Integer, String>>>>
      supplierToFuture;

  // TODO - there might be a better way to configure this to reduce the chance of redundant caches.
  private transient volatile AsyncLoadingCache<String, Map<Integer, String>> cache;

  /**
   * @param maxCacheSize maximium number of entries in cache
   * @param maxBatchSize maximium number of entries to fetch in a batch
   * @param maxBatchDelay maximium delay for a batch
   * @param expiration expiration time for entries in the cache
   * @param contentQueryClient client for calling Content API's query method
   * @param matchingKeys the prop keys to keep
   */
  public ContentCacheSupplier(
      int maxCacheSize,
      int maxBatchSize,
      Duration maxBatchDelay,
      Duration expiration,
      ContentQueryClient contentQueryClient,
      Collection<String> matchingKeys) {
    this(
        maxCacheSize,
        maxBatchSize,
        maxBatchDelay,
        expiration,
        contentQueryClient,
        matchingKeys,
        null);
  }

  /**
   * @param supplierToFuture support swapping out future execution to simplify tests
   */
  @VisibleForTesting
  ContentCacheSupplier(
      int maxCacheSize,
      int maxBatchSize,
      Duration maxBatchDelay,
      Duration expiration,
      ContentQueryClient contentQueryClient,
      Collection<String> matchingKeys,
      Function<
              Supplier<Map<String, Map<Integer, String>>>,
              CompletableFuture<Map<String, Map<Integer, String>>>>
          supplierToFuture) {
    this.maxCacheSize = maxCacheSize;
    this.maxBatchSize = maxBatchSize;
    this.maxBatchDelay = maxBatchDelay;
    this.expiration = expiration;
    this.contentQueryClient = contentQueryClient;
    this.matchingKeys = ImmutableList.copyOf(matchingKeys);
    this.intMatchingKeys = matchingKeys.stream().map(StringUtil::hash).collect(Collectors.toSet());
    this.supplierToFuture = supplierToFuture;
  }

  public AsyncLoadingCache<String, Map<Integer, String>> get() {
    if (cache == null) {
      synchronized (supplierLock) {
        if (null == cache) {
          LOGGER.info("Created Caffeine cache for CachingContentLookup");
          bulkLoader =
              new RotationBulkLoader<>(
                  maxBatchSize,
                  maxBatchDelay.toMillis(),
                  toLoad -> {
                    List<String> keys =
                        toLoad.stream().map(wk -> wk.key).collect(Collectors.toList());
                    asyncBulkLookup(keys)
                        .whenComplete(
                            (values, throwable) -> {
                              if (null != throwable) {
                                toLoad.forEach(
                                    waitingKey ->
                                        waitingKey.future.completeExceptionally(throwable));
                              } else {
                                for (BulkLoader.WaitingKey<String, Map<Integer, String>>
                                    waitingKey : toLoad) {
                                  if (values.containsKey(waitingKey.key))
                                    waitingKey.future.complete(values.get(waitingKey.key));
                                  else
                                    waitingKey.future.completeExceptionally(
                                        new NoSuchElementException(
                                            "No value for key " + waitingKey.key));
                                }
                              }
                            });
                  });
          cache =
              Caffeine.newBuilder()
                  .maximumSize(maxCacheSize)
                  .expireAfterAccess(expiration)
                  // Dan: executor is ignored.  Online advice I found says to do blocking calls.
                  // This is confusing and probably wrong.
                  // Xingcan: We manage the executor used for bulk loading by ourselves.
                  .executor(Executors.newSingleThreadExecutor())
                  .buildAsync(CoalescingBulkCacheLoader.create(bulkLoader));
        }
      }
    }
    return cache;
  }

  // Keeping as list to keep order.  Simplifies tests.
  @VisibleForTesting
  CompletableFuture<Map<String, Map<Integer, String>>> asyncBulkLookup(List<String> contentIds) {
    ImmutableMap.Builder<String, Map<Integer, String>> builder =
        ImmutableMap.builderWithExpectedSize(contentIds.size());
    try {
      Map<String, Map<String, Map<String, Object>>> contentIdToSourceToProps =
          contentQueryClient.apply(
              ContentQueryParams.builder()
                  .setContentIds(contentIds)
                  .setProjections(matchingKeys)
                  .build());
      addHashedContentMap(builder, contentIdToSourceToProps);
      // Content API doesn't return a key for missing content IDs.
      Set<String> missingContentIds = new HashSet<>(contentIds);
      missingContentIds.removeAll(contentIdToSourceToProps.keySet());
      for (String missingContentId : missingContentIds) {
        builder.put(missingContentId, ImmutableMap.of());
      }
      if (supplierToFuture != null) {
        return supplierToFuture.apply(builder::build);
      }
      return CompletableFuture.supplyAsync(builder::build);
    } catch (Throwable ex) {
      return CompletableFuture.failedFuture(ex);
    }
  }

  // Filter to a subset of fields.
  private void addHashedContentMap(
      ImmutableMap.Builder<String, Map<Integer, String>> builder,
      Map<String, Map<String, Map<String, Object>>> contentIdToSourceToProps) {
    for (Map.Entry<String, Map<String, Map<String, Object>>> contentIdToSourceToPropsEntry :
        contentIdToSourceToProps.entrySet()) {
      String contentId = contentIdToSourceToPropsEntry.getKey();
      builder.put(
          contentId, toHashedSourceProps(contentId, contentIdToSourceToPropsEntry.getValue()));
    }
  }

  private Map<Integer, String> toHashedSourceProps(
      String contentId, Map<String, Map<String, Object>> sourceToProps) {
    ImmutableMap.Builder<Integer, String> builder =
        ImmutableMap.builderWithExpectedSize(intMatchingKeys.size());
    // Failed to fetch the contents
    if (null == sourceToProps) {
      return builder.build();
    }
    // Ignore `source`.  Pick fields across all sources.
    for (Map<String, Object> props : sourceToProps.values()) {
      for (Map.Entry<String, Object> entry : props.entrySet()) {
        Integer key = StringUtil.hash(entry.getKey());
        if (intMatchingKeys.contains(key)) {
          if (entry.getValue() instanceof String) {
            builder.put(key, (String) entry.getValue());
          } else {
            throw new IllegalStateException(
                "Content ID lookups encountered a non-String value; contentId="
                    + contentId
                    + ", key="
                    + entry.getKey()
                    + ", value="
                    + entry.getValue());
          }
        }
      }
    }
    return builder.build();
  }

  @Override
  public void close() throws IOException {
    if (null != bulkLoader) {
      bulkLoader.close();
    }
  }
}
