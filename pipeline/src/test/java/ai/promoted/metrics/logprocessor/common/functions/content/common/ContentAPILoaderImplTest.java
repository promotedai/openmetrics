package ai.promoted.metrics.logprocessor.common.functions.content.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ContentAPILoaderImplTest {
  private HttpClientInterface httpClientInterface;
  private int failureCount;
  private Function<String, String> contentIdToResponse;

  private Map<String, Map<String, Object>> expectedResponse =
      ImmutableMap.<String, Map<String, Object>>builder()
          .put("content1", ImmutableMap.of("parent1", "value1", "parent2", "value2"))
          .build();

  @BeforeEach
  public void setUp() {
    failureCount = 0;
    contentIdToResponse =
        (contentIds) ->
            String.format(
                "{\"%s\": {\"parent1\": \"value1\", \"parent2\": \"value2\"}}", contentIds);
    httpClientInterface =
        (contentIds) -> {
          if (failureCount > 0) {
            failureCount--;
            HttpResponse response = Mockito.mock(HttpResponse.class);
            Mockito.when(response.statusCode()).thenReturn(500);
            Mockito.when(response.body()).thenReturn("internal server error");
            return response;
          }
          HttpResponse response = Mockito.mock(HttpResponse.class);
          Mockito.when(response.statusCode()).thenReturn(200);
          Mockito.when(response.body())
              .thenReturn(contentIdToResponse.apply(contentIds.iterator().next()));
          return response;
        };
  }

  @Test
  public void apply() throws Exception {
    ContentAPILoaderImpl loader = createLoader(3, 0);
    assertEquals(expectedResponse, loader.apply(ImmutableList.of("content1")));
  }

  @Test
  public void apply_retrySuccess() throws Exception {
    failureCount = 1;
    ContentAPILoaderImpl loader = createLoader(3, 0);
    assertEquals(expectedResponse, loader.apply(ImmutableList.of("content1")));
  }

  @Test
  public void apply_retrySuccess_perStatusCode() throws Exception {
    failureCount = 1;
    // Verifies that the per status code retry works by setting default retries to zero.
    ContentAPILoaderImpl loader =
        new ContentAPILoaderImpl(
            httpClientInterface,
            0,
            ImmutableMap.of(500, 3),
            0,
            ContentAPILoaderImplTest::fakeSleep);
    assertEquals(expectedResponse, loader.apply(ImmutableList.of("content1")));
  }

  @Test
  public void apply_retryFailed_consecutiveFailures() throws Exception {
    failureCount = 4;
    ContentAPILoaderImpl loader = createLoader(3, 0);
    assertThrows(RuntimeException.class, () -> loader.apply(ImmutableList.of("content1")));
  }

  @Test
  public void apply_retrySuccess_goneAway() throws Exception {
    failureCount = 1;
    httpClientInterface =
        (contentIds) -> {
          if (failureCount > 0) {
            failureCount--;
            throw new IOException("/10.12.105.42:57582: GOAWAY received");
          }
          HttpResponse response = Mockito.mock(HttpResponse.class);
          Mockito.when(response.statusCode()).thenReturn(200);
          Mockito.when(response.body())
              .thenReturn(contentIdToResponse.apply(contentIds.iterator().next()));
          return response;
        };

    // Verifies that the per status code retry works by setting default retries to zero.
    ContentAPILoaderImpl loader =
        new ContentAPILoaderImpl(
            httpClientInterface,
            0,
            ImmutableMap.of(410, 3),
            0,
            ContentAPILoaderImplTest::fakeSleep);
    assertEquals(expectedResponse, loader.apply(ImmutableList.of("content1")));
  }

  @Test
  public void apply_retryFailed_increasedMaxConsecutiveFailures() throws Exception {
    failureCount = 4;
    ContentAPILoaderImpl loader = createLoader(3, 1);
    assertEquals(
        ImmutableMap.of(),
        loader.apply(ImmutableList.of("content1")),
        "Too many failures and return ");
  }

  @Test
  public void formatJsonBody() throws Exception {
    assertEquals(
        "{\"content_ids\":[\"abc\"],\"include_deleted\":true}",
        ContentAPILoaderImpl.formatJsonBody(ImmutableList.of("abc")));
    assertEquals(
        "{\"content_ids\":[\"ab\\\"c\"],\"include_deleted\":true}",
        ContentAPILoaderImpl.formatJsonBody(ImmutableList.of("ab\"c")));
    assertEquals(
        "{\"content_ids\":[\"\"],\"include_deleted\":true}",
        ContentAPILoaderImpl.formatJsonBody(ImmutableList.of("")));
    assertEquals(
        "{\"content_ids\":[\"abc\",\"def\",\"ghi\"],\"include_deleted\":true}",
        ContentAPILoaderImpl.formatJsonBody(ImmutableList.of("abc", "def", "ghi")));
  }

  private ContentAPILoaderImpl createLoader(int maxAttempts, int maxAllowedConsecutiveFailures) {
    return new ContentAPILoaderImpl(
        httpClientInterface,
        maxAttempts,
        ImmutableMap.of(),
        maxAllowedConsecutiveFailures,
        ContentAPILoaderImplTest::fakeSleep);
  }

  private static void fakeSleep(long duration) throws InterruptedException {}
}
