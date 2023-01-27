package ai.promoted.metrics.logprocessor.common.functions.content.datastream;

import ai.promoted.metrics.logprocessor.common.functions.content.common.ContentAPILoader;
import ai.promoted.metrics.logprocessor.common.util.DebugIds;
import ai.promoted.metrics.logprocessor.common.util.StringUtil;
import ai.promoted.proto.event.TinyEvent;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * A function wrapper that caches the Content API lookups for joining in other Content IDs.
 */
public final class CachingContentDataStreamLookup extends RichAsyncFunction<TinyEvent, TinyEvent> {
    private static final Logger LOGGER = LogManager.getLogger(CachingContentDataStreamLookup.class);

    private final int maxContentCount;
    private final Duration expiration;
    private final ContentAPILoader contentAPILoader;
    private final Set<Integer> matchingKeys;
    private final DebugIds debugIds;
    private transient AsyncLoadingCache<String, Map<Integer, String>> cache;

    /**
     * @param maxContentCount maximium number of entries in cache
     * @param expiration expiration time for entries in the cache
     * @param contentIdToFields a function that takes a {@code contentId} and {#code executor} and outputs the content fields
     */
    public CachingContentDataStreamLookup(
            int maxContentCount,
            Duration expiration,
            ContentAPILoader contentAPILoader,
            Collection<String> matchingKeys,
            DebugIds debugIds) {
        this.maxContentCount = maxContentCount;
        this.expiration = expiration;
        this.contentAPILoader = contentAPILoader;
        this.matchingKeys = matchingKeys.stream().map(StringUtil::hash).collect(Collectors.toSet());
        this.debugIds = debugIds;
    }

    private AsyncLoadingCache<String, Map<Integer, String>> getCache() {
        if (cache == null) {
            LOGGER.info("Created Caffeine cache for CachingContentLookup");
            cache = Caffeine.newBuilder()
                    .maximumSize(maxContentCount)
                    .expireAfterAccess(expiration)
                    // Dan: executor is ignored.  Online advice I found says to do blocking calls.
                    // This is confusing and probably wrong.
                    .buildAsync((k, executor) -> toHashedMap(contentAPILoader.apply(ImmutableList.of(k)), k));
        }
        return cache;
    }

    private CompletableFuture<Map<Integer, String>> toHashedMap(Map<String, Map<String, Object>> batchResponse, String contentId) {
        // Filter to a subset of fields.
        Map<String, Object> fields = batchResponse.get(contentId);
        ImmutableMap<Integer, String> result;
        if (fields != null) {
            ImmutableMap.Builder<Integer, String> builder = ImmutableMap.builderWithExpectedSize(fields.size());
            for (Map.Entry<String, Object> entry : fields.entrySet()) {
                Integer key = StringUtil.hash(entry.getKey());
                if (matchingKeys.contains(key)) {
                    builder.put(key, (String) entry.getValue());
                }
            }
            result = builder.build();
        } else {
            result = ImmutableMap.of();
        }
        return CompletableFuture.completedFuture(result);
    }

    @Override
    public void asyncInvoke(TinyEvent event, final ResultFuture<TinyEvent> resultFuture) {
        boolean debugLogging = debugIds.matches(event);
        if (event.getContentId().isEmpty()) {
            if (debugLogging) {
                LOGGER.info("debug ID async content join no contentId, event={}", event);
            }
            resultFuture.complete(Collections.singleton(event));
        } else {
            if (debugLogging) {
                LOGGER.info("debug ID async content join start get from cache, event={}", event);
            }
            getCache().get(event.getContentId())
                    .thenAccept((Map<Integer, String> contentFields) -> {
                        if (debugLogging) {
                            LOGGER.info("debug ID async content join content, event={}, contentFields={}", event, contentFields);
                        }
                        TinyEvent result = event;
                        if (!contentFields.isEmpty()) {
                            TinyEvent.Builder builder = result.toBuilder();
                            // Prefer otherContentIds already set on (logged) TinyEvents over otherContentIds from Content Service.
                            for (Map.Entry<Integer, String> entry : contentFields.entrySet()) {
                                if (!builder.getOtherContentIdsMap().containsKey(entry.getKey())) {
                                    builder.putOtherContentIds(entry.getKey(), entry.getValue());
                                }
                            }
                            result = builder.build();
                        }
                        resultFuture.complete(Collections.singleton(result));
                    });
        }
    }
}
