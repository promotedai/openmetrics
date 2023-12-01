package ai.promoted.metrics.logprocessor.common.functions.content.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ContentQueryClientImplTest {
  private HttpClientInterface httpClientInterface;
  private Function<String, String> requestBodyToResponse;
  private String expectedRequestBody;
  private final Map<String, Map<String, Map<String, Object>>> expectedResponse =
      ImmutableMap.<String, Map<String, Map<String, Object>>>builder()
          .put(
              "content1",
              ImmutableMap.of(
                  "defaultSource", ImmutableMap.of("parent1", "value1", "parent2", "value2")))
          .build();

  @BeforeEach
  public void setUp() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String responseJson =
        mapper.writeValueAsString(
            ImmutableMap.of(
                "content1",
                ImmutableMap.of(
                    "defaultSource",
                    ImmutableMap.of(
                        "parent1", "value1",
                        "parent2", "value2"))));
    requestBodyToResponse = (requestBody) -> responseJson;

    expectedRequestBody = "{\"content_ids\":[\"content1\"],\"include_deleted\":true}";

    httpClientInterface =
        (requestBody) -> {
          assertEquals(expectedRequestBody, requestBody);
          HttpResponse response = Mockito.mock(HttpResponse.class);
          Mockito.when(response.statusCode()).thenReturn(200);
          Mockito.when(response.body()).thenReturn(requestBodyToResponse.apply(requestBody));
          return response;
        };
  }

  void setUpErrorHttpCase() {
    httpClientInterface =
        (requestBody) -> {
          assertEquals(expectedRequestBody, requestBody);
          HttpResponse response = Mockito.mock(HttpResponse.class);
          Mockito.when(response.statusCode()).thenReturn(500);
          Mockito.when(response.body()).thenReturn("internal server error");
          return response;
        };
  }

  @Test
  public void apply() throws Exception {
    ContentQueryClientImpl loader = createLoader();
    assertEquals(expectedResponse, loader.apply(newParams("content1")));
  }

  @Test
  public void apply_error() throws Exception {
    setUpErrorHttpCase();
    ContentQueryClientImpl loader = createLoader();
    assertThrows(RuntimeException.class, () -> loader.apply(newParams("content1")));
  }

  @Test
  public void formatJsonBody() throws Exception {
    assertEquals(
        "{\"content_ids\":[\"abc\"],\"include_deleted\":true}",
        ContentQueryClientImpl.formatJsonRequestBody(newParams("abc")));
    assertEquals(
        "{\"content_ids\":[\"ab\\\"c\"],\"include_deleted\":true}",
        ContentQueryClientImpl.formatJsonRequestBody(newParams("ab\"c")));
    assertEquals(
        "{\"content_ids\":[\"\"],\"include_deleted\":true}",
        ContentQueryClientImpl.formatJsonRequestBody(newParams("")));
    assertEquals(
        "{\"content_ids\":[\"abc\",\"def\",\"ghi\"],\"include_deleted\":true}",
        ContentQueryClientImpl.formatJsonRequestBody(
            newParams(ImmutableList.of("abc", "def", "ghi"))));

    // Projections
    assertEquals(
        "{\"content_ids\":[\"abc\"],\"projections\":[\"parentId\"],\"include_deleted\":true}",
        ContentQueryClientImpl.formatJsonRequestBody(
            newParams(ImmutableList.of("abc"), ImmutableList.of("parentId"))));
    // Projections
    assertEquals(
        "{\"content_ids\":[\"abc\"],\"projections\":[\"parentId1\",\"parentId2\"],\"include_deleted\":true}",
        ContentQueryClientImpl.formatJsonRequestBody(
            newParams(ImmutableList.of("abc"), ImmutableList.of("parentId1", "parentId2"))));
  }

  private ContentQueryClientImpl createLoader() {
    return new ContentQueryClientImpl(httpClientInterface, ContentQueryClientImplTest::fakeSleep);
  }

  private static void fakeSleep(long duration) throws InterruptedException {}

  private static ContentQueryParams newParams(List<String> contentIds, List<String> projections) {
    return ContentQueryParams.builder()
        .setContentIds(contentIds)
        .setProjections(projections)
        .build();
  }

  private static ContentQueryParams newParams(List<String> contentIds) {
    return newParams(contentIds, ImmutableList.of());
  }

  private static ContentQueryParams newParams(String contentId) {
    return newParams(ImmutableList.of(contentId));
  }
}
