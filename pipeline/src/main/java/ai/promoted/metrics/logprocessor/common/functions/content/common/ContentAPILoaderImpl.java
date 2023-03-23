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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Loads content details from the Content API. */
public final class ContentAPILoaderImpl implements ContentAPILoader {
  private static final Logger LOGGER = LogManager.getLogger(ContentAPILoaderImpl.class);

  // So we can mock out the http code for tests.
  private final HttpClientInterface httpClient;

  private final int defaultMaxAttempts;
  private final Map<Integer, Integer> maxAttemptsPerStatusCode;
  private final int maxAllowedConsecutiveErrors;
  private int consecutiveErrors;
  // If null, uses Thread:sleep.
  private final Sleep sleep;

  @FunctionalInterface
  public interface Sleep extends Serializable {
    void apply(long duration) throws InterruptedException;
  }

  public ContentAPILoaderImpl(
      String rootContentApiUrl,
      String apiKey,
      int defaultMaxAttempts,
      Map<Integer, Integer> maxAttemptsPerStatusCode,
      int maxAllowedConsecutiveErrors) {
    this(
        new HttpClientImpl(rootContentApiUrl, apiKey),
        defaultMaxAttempts,
        maxAttemptsPerStatusCode,
        maxAllowedConsecutiveErrors,
        null);
  }

  @VisibleForTesting
  ContentAPILoaderImpl(
      HttpClientInterface httpClient,
      int defaultMaxAttempts,
      Map<Integer, Integer> maxAttemptsPerStatusCode,
      int maxAllowedConsecutiveErrors,
      Sleep sleep) {
    this.httpClient = httpClient;
    this.defaultMaxAttempts = defaultMaxAttempts;
    this.maxAttemptsPerStatusCode = maxAttemptsPerStatusCode;
    this.maxAllowedConsecutiveErrors = maxAllowedConsecutiveErrors;
    this.sleep = sleep;
  }

  private Sleep getSleep() {
    if (sleep != null) {
      return sleep;
    }
    return Thread::sleep;
  }

  @Override
  public Map<String, Map<String, Object>> apply(Collection<String> contentIds) {
    // Delay construction to lower costs.
    Map<Integer, Integer> errorsPerStatusCode = null;
    int statusCode;
    String error;

    while (true) {
      HttpResponse response = null;
      try {
        response = httpClient.send(contentIds);
        statusCode = response.statusCode();
        error = (String) response.body();
      } catch (IOException e) {
        // Special case for GOAWAY.
        if (e.getMessage().contains("GOAWAY")) {
          statusCode = 410; // Gone.
          error = e.getMessage();
        } else {
          throw new RuntimeException(e);
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      if (statusCode == 200) {
        // TODO - Implement a custom deserializer so we don't need to add create a map of all
        // properties.
        ObjectMapper mapper = new ObjectMapper();
        // The first layer of the response is a map of contentId to properties.
        try {
          Preconditions.checkNotNull(response);
          var result = mapper.readValue((String) response.body(), Map.class);
          if (result != null) {
            return (Map<String, Map<String, Object>>) result;
          } else {
            return ImmutableMap.of();
          }
        } catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
      }
      if (errorsPerStatusCode == null) {
        errorsPerStatusCode = new HashMap<>();
      }
      int errors = errorsPerStatusCode.getOrDefault(statusCode, 0);
      errors++;
      errorsPerStatusCode.put(statusCode, errors);
      if (errors < maxAttemptsPerStatusCode.getOrDefault(statusCode, defaultMaxAttempts)) {
        // Sleep for a small amount to let issues recover.
        // Do a small backoff on each failure.
        try {
          getSleep().apply(50L * (errors + 1));
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      } else {
        break;
      }
    }

    consecutiveErrors++;
    if (consecutiveErrors > maxAllowedConsecutiveErrors) {
      throw new RuntimeException("Content API error; statusCode=" + statusCode + ", body=" + error);
    } else {
      LOGGER.error(
          "Content API retries failed for contentIds={}.  Has consecutive error limit={}/{}, statusCode={}, error={}",
          contentIds,
          consecutiveErrors,
          maxAllowedConsecutiveErrors,
          statusCode,
          error);
      return ImmutableMap.of();
    }
  }

  @VisibleForTesting
  static String formatJsonBody(Collection<String> contentIds) throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode node = mapper.createObjectNode();
    contentIds.forEach(node.putArray("content_ids")::add);
    node.put("include_deleted", true);
    return mapper.writer().writeValueAsString(node);
  }

  private static class HttpClientImpl implements HttpClientInterface {
    private final String rootContentApiUrl;
    private final String apiKey;
    private transient HttpClient httpClient;

    private HttpClientImpl(String rootContentApiUrl, String apiKey) {
      this.rootContentApiUrl = rootContentApiUrl;
      this.apiKey = apiKey;
    }

    @Override
    public HttpResponse send(Collection<String> contentIds)
        throws IOException, InterruptedException {
      if (httpClient == null) {
        httpClient = HttpClient.newHttpClient();
      }
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(rootContentApiUrl + "v1/content/query"))
              .header("Content-Type", "application/json")
              .header("x-api-key", apiKey)
              .POST(HttpRequest.BodyPublishers.ofString(formatJsonBody(contentIds)))
              .build();
      return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }
  }
}
