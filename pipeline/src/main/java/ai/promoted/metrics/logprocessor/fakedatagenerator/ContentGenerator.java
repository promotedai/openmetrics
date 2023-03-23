package ai.promoted.metrics.logprocessor.fakedatagenerator;

import ai.promoted.metrics.logprocessor.common.fakedatagenerator.LogRequestIterator;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.LogRequestIteratorOptions;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.Content;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.ContentDB;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.ContentDBFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.DataOutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "fakecontentgenerator",
    mixinStandardHelpOptions = true,
    version = "fakecontentgenerator 1.0.0",
    description = "A generator which pushes fake Content into Content API.")
public class ContentGenerator implements Callable<Integer> {
  private static final Logger LOGGER = LogManager.getLogger(ContentGenerator.class);

  @Option(
      names = {"--contentApiRootUrl"},
      defaultValue = "",
      description =
          "Format matches `http://ee1a-67-168-60-51.ngrok.io/` or `http://localhost:5150/`.")
  private String contentApiRootUrl = "";

  @Option(
      names = {"--contentApiKey"},
      defaultValue = "",
      description = "The API Key for the Content API.")
  private String contentApiKey = "";

  public static void main(String[] args) throws Exception {
    int exitCode = new CommandLine(new ContentGenerator()).execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {
    validateArgs();
    ContentDB contentDb =
        ContentDBFactory.create(
            LogRequestIterator.PLATFORM_ID, LogRequestIteratorOptions.DEFAULT_CONTENT_OPTIONS);
    writeContents(contentDb);
    return 0;
  }

  private void validateArgs() {
    Preconditions.checkArgument(
        !contentApiRootUrl.isEmpty(), "--rootContentApiUrl must be specified");
    Preconditions.checkArgument(
        contentApiRootUrl.endsWith("/"), "--rootContentApiUrl should end with a `/`");
    Preconditions.checkArgument(!contentApiKey.isEmpty(), "--contentApiKey must be specified");
  }

  private void writeContents(ContentDB contentDb) throws Exception {
    LOGGER.info("Write fake contents");
    List<Content> contents = contentDb.listContent();
    List<List<Content>> contentBatches = Lists.partition(contents, 100);

    for (List<Content> contentBatch : contentBatches) {
      Map<String, Map<String, String>> batchMap =
          contentBatch.stream()
              .collect(Collectors.toMap(Content::contentId, Content::requestFields));
      writeContentBatch(batchMap);
    }
    LOGGER.info("Done");
  }

  // TODO - how to write to Content Store API that is running locally?
  private void writeContentBatch(Map<String, Map<String, String>> batch) throws Exception {
    // Since we're using Java8, we cannot use the latest http package.
    HttpURLConnection connection = null;
    try {
      URL url = new URL(contentApiRootUrl + "v1/content/batch");
      connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("PUT");
      connection.setRequestProperty("Content-Type", "application/json");
      connection.setRequestProperty("x-api-key", contentApiKey);
      connection.setDoOutput(true);

      DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
      wr.writeBytes(toJson(batch));
      wr.close();

      LOGGER.info(
          "Wrote content batch responseCode={}, size={}, contentIds={}",
          connection.getResponseCode(),
          batch.size(),
          batch.keySet());
      if (connection.getResponseCode() != 200) {
        throw new IllegalStateException("Hit responseCode=" + connection.getResponseCode());
      }
      // TODO - check the response body;
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  private String toJson(Map<?, ?> obj) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      return objectMapper.writeValueAsString(obj);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
