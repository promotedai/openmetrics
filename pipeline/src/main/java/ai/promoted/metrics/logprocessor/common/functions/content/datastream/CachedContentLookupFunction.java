package ai.promoted.metrics.logprocessor.common.functions.content.datastream;

import ai.promoted.metrics.logprocessor.common.functions.base.SerializableBiConsumer;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableBiFunction;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableConsumer;
import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.Map.Entry;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.util.function.SerializableFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A RichAsyncFunction that caches the Content API lookups for joining in other Content IDs. It's
 * generic so the function can be used for both TinyInsertions and TinyActions.
 *
 * @param <T> the type for the message.
 * @param <TB> T's Builder.
 */
public final class CachedContentLookupFunction<T, TB> extends RichAsyncFunction<T, T> {
  private static final Logger LOGGER = LogManager.getLogger(CachedContentLookupFunction.class);
  private final ContentCacheSupplier cacheSupplier;
  // Use functions so we can support both contentId fields.
  private final SerializableFunction<T, TB> toBuilder;
  private final SerializableFunction<TB, T> build;
  private final SerializableFunction<T, String> getContentId;
  private final SerializableBiFunction<TB, Integer, Boolean> hasOtherContentId;
  private final SerializableBiConsumer<TB, Entry<Integer, String>> putOtherContentIds;
  private final SerializableFunction<T, Boolean> matchesDebugIds;
  @VisibleForTesting transient SerializableConsumer<Boolean> completionNotifier;

  /***
   * @param cacheSupplier Supports sharing caches across other lookup instances.
   *                      Might not happen depending on how the cacheSupplier is serialized.
   * @param hasOtherContentId Allows setting/getting different TinyEvent.*content_id fields.
   * @param putOtherContentIds Allows setting/getting different TinyEvent.*content_id fields.
   * @param matchesDebugIds For extra logging for specific events.
   */
  public CachedContentLookupFunction(
      ContentCacheSupplier cacheSupplier,
      SerializableFunction<T, TB> toBuilder,
      SerializableFunction<TB, T> build,
      SerializableFunction<T, String> getContentId,
      SerializableBiFunction<TB, Integer, Boolean> hasOtherContentId,
      SerializableBiConsumer<TB, Entry<Integer, String>> putOtherContentIds,
      SerializableFunction<T, Boolean> matchesDebugIds) {
    this.cacheSupplier = cacheSupplier;
    this.toBuilder = toBuilder;
    this.build = build;
    this.getContentId = getContentId;
    this.hasOtherContentId = hasOtherContentId;
    this.putOtherContentIds = putOtherContentIds;
    this.matchesDebugIds = matchesDebugIds;
  }

  @Override
  public void asyncInvoke(T event, final ResultFuture<T> resultFuture) {
    boolean debugLogging = matchesDebugIds.apply(event);
    String contentId = getContentId.apply(event);
    if (contentId.isEmpty()) {
      if (debugLogging) {
        LOGGER.info("debug ID async content join no contentId, event={}", event);
      }
      resultFuture.complete(Collections.singleton(event));
    } else {
      if (debugLogging) {
        LOGGER.info("debug ID async content join start get from cache, event={}", event);
      }

      cacheSupplier
          .get()
          .get(contentId)
          .whenComplete(
              (contentFields, throwable) -> {
                if (null != throwable) {
                  resultFuture.completeExceptionally(throwable);
                  LOGGER.warn(
                      "Hit an exception when fetching contents for contentId " + contentId,
                      throwable);
                } else {
                  try {
                    if (debugLogging) {
                      LOGGER.info(
                          "debug ID async content join content, event={}, contentFields={}",
                          event,
                          contentFields);
                    }
                    T result = event;
                    if (!contentFields.isEmpty()) {
                      TB builder = toBuilder.apply(result);
                      // Prefer otherContentIds already set on (logged) TinyEvents over
                      // otherContentIds
                      // from Content Service.
                      for (Entry<Integer, String> entry : contentFields.entrySet()) {
                        if (!hasOtherContentId.apply(builder, entry.getKey())) {
                          putOtherContentIds.accept(builder, entry);
                        }
                      }
                      result = build.apply(builder);
                    }
                    resultFuture.complete(Collections.singleton(result));
                  } catch (Throwable t) {
                    // We're seeing issues with the content-lookup operators not checkpointing.
                    // This logging should be redundant, but we want extra log records to track
                    // down the current issue.
                    LOGGER.warn(
                        () ->
                            String.format(
                                "CachingContentDataStreamLookup failed; contentId=%s", contentId),
                        t);
                    resultFuture.completeExceptionally(t);
                  }
                }
                if (null != completionNotifier) {
                  // This notifier is mainly used for testing.
                  completionNotifier.accept(true);
                }
              });
    }
  }

  @Override
  public void close() throws Exception {
    cacheSupplier.close();
  }
}
