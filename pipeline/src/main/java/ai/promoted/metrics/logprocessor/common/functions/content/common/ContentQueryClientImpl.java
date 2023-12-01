package ai.promoted.metrics.logprocessor.common.functions.content.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Loads content details from the Content API. */
public final class ContentQueryClientImpl implements ContentQueryClient {
  private static final Logger LOGGER = LogManager.getLogger(ContentQueryClientImpl.class);

  // So we can mock out the http code for tests.
  private final HttpClientInterface httpClient;
  // If null, uses Thread:sleep.
  private final Sleep sleep;

  @FunctionalInterface
  public interface Sleep extends Serializable {
    void apply(long duration) throws InterruptedException;
  }

  public ContentQueryClientImpl(String rootContentApiUrl, String apiKey, Duration timeout) {
    this(new HttpClientImpl(rootContentApiUrl, apiKey, timeout), null);
  }

  @VisibleForTesting
  ContentQueryClientImpl(HttpClientInterface httpClient, Sleep sleep) {
    this.httpClient = httpClient;
    this.sleep = sleep;
  }

  private Sleep getSleep() {
    if (sleep != null) {
      return sleep;
    }
    return Thread::sleep;
  }

  @Override
  public Map<String, Map<String, Map<String, Object>>> apply(ContentQueryParams params) {
    try {
      String requestBody = formatJsonRequestBody(params);
      HttpResponse response = httpClient.send(requestBody);
      int statusCode = response.statusCode();
      if (statusCode == 200) {
        ObjectMapper mapper = new ObjectMapper();
        // The first layer of the response is a map of contentId to properties.
        try {
          Preconditions.checkNotNull(response);
          var result = mapper.readValue((String) response.body(), Map.class);
          if (result != null) {
            return (Map<String, Map<String, Map<String, Object>>>) result;
          } else {
            return ImmutableMap.of();
          }
        } catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
      }
      String error = (String) response.body();
      throw new ContentApiHttpException(
          "Exception when calling Content API; statusCode=" + statusCode + ", error=" + error);
    } catch (IOException e) {
      LOGGER.warn(
          () -> String.format("IOException when calling Content API; params=%s", params), e);
      throw new ContentApiHttpException("Exception when calling Content API", e);
    } catch (InterruptedException e) {
      LOGGER.warn(
          () -> String.format("InterruptedException when calling Content API; params=%s", params),
          e);
      throw new ContentApiHttpException("Exception when calling Content API", e);
    }
  }

  @VisibleForTesting
  static String formatJsonRequestBody(ContentQueryParams params) throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode node = mapper.createObjectNode();
    params.contentIds().forEach(node.putArray("content_ids")::add);
    if (!params.projections().isEmpty()) {
      params.projections().forEach(node.putArray("projections")::add);
    }
    node.put("include_deleted", true);
    return mapper.writer().writeValueAsString(node);
  }

  public static class ContentApiHttpException extends RuntimeException {
    public ContentApiHttpException(String message) {
      super(message);
    }

    public ContentApiHttpException(String message, Throwable e) {
      super(message, e);
    }
  }

  private static class HttpClientImpl implements HttpClientInterface {
    private final String rootContentApiUrl;
    private final String apiKey;
    private final Duration timeout;
    private transient HttpClient httpClient;

    private HttpClientImpl(String rootContentApiUrl, String apiKey, Duration timeout) {
      this.rootContentApiUrl = rootContentApiUrl;
      this.apiKey = apiKey;
      this.timeout = timeout;
    }

    @Override
    public HttpResponse send(String body) throws IOException, InterruptedException {
      if (httpClient == null) {
        httpClient = HttpClient.newBuilder().connectTimeout(timeout).build();
      }
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(rootContentApiUrl + "v2/content/query"))
              .header("Content-Type", "application/json")
              .header("x-api-key", apiKey)
              .POST(HttpRequest.BodyPublishers.ofString(body))
              .timeout(timeout)
              .build();
      return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }
  }
}
