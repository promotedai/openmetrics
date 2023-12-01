package ai.promoted.metrics.logprocessor.common.functions.content.datastream;

import static com.google.common.truth.Truth.assertThat;

import ai.promoted.metrics.logprocessor.common.functions.content.common.ContentQueryClient;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ContentCacheSupplierTest {
  @Test
  public void bulkLookup() throws ExecutionException, InterruptedException {
    ContentQueryClient client = Mockito.mock(ContentQueryClient.class);
    Mockito.when(client.apply(Mockito.any()))
        .thenReturn(
            ImmutableMap.of(
                "content1",
                ImmutableMap.of(
                    "defaultSource", ImmutableMap.of("unusedProp", 1, "parentId", "2"))));

    ContentCacheSupplier cacheSupplier =
        new ContentCacheSupplier(
            10,
            10,
            Duration.ofSeconds(1),
            Duration.ofSeconds(100),
            client,
            ImmutableSet.of("parentId"));

    Map<String, Map<Integer, String>> contentIdToProps =
        cacheSupplier.asyncBulkLookup(ImmutableList.of("content1", "content2")).get();

    assertThat(contentIdToProps)
        .containsExactly(
            "content1", ImmutableMap.of(1175162725, "2"),
            "content2", ImmutableMap.of());
  }

  @Test
  public void failedToFetchContent() {
    ContentQueryClient client = Mockito.mock(ContentQueryClient.class);
    Mockito.when(client.apply(Mockito.any()))
        .thenThrow(new RuntimeException("Exception when calling Content API"));

    ContentCacheSupplier cacheSupplier =
        new ContentCacheSupplier(
            1,
            10,
            Duration.ofSeconds(1),
            Duration.ofSeconds(100),
            client,
            ImmutableSet.of("parentId"));

    AtomicBoolean cachedException = new AtomicBoolean(false);
    cacheSupplier
        .get()
        .get("content1")
        .whenComplete(
            (integerStringMap, throwable) -> {
              Assertions.assertTrue(throwable instanceof RuntimeException);
              cachedException.set(true);
            })
        .exceptionally(throwable -> null)
        .join();
    Assertions.assertTrue(cachedException.get());
  }

  @Test
  public void contentNotExists() {
    ContentQueryClient client = Mockito.mock(ContentQueryClient.class);
    Mockito.when(client.apply(Mockito.any()))
        .thenReturn(
            ImmutableMap.of(
                "content1",
                ImmutableMap.of(
                    "defaultSource", ImmutableMap.of("unusedProp", 1, "parentId", "2"))));

    ContentCacheSupplier cacheSupplier =
        new ContentCacheSupplier(
            1,
            10,
            Duration.ofSeconds(1),
            Duration.ofSeconds(100),
            client,
            ImmutableSet.of("parentId"));

    AtomicBoolean cachedException = new AtomicBoolean(false);
    cacheSupplier
        .get()
        .get("content")
        .whenComplete(
            (integerStringMap, throwable) -> {
              Assertions.assertNull(throwable);
              Assertions.assertEquals(0, integerStringMap.size());
              cachedException.set(true);
            })
        .exceptionally(throwable -> null)
        .join();
    Assertions.assertTrue(cachedException.get());
  }
}
