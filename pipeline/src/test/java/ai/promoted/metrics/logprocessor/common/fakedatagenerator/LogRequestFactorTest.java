package ai.promoted.metrics.logprocessor.common.fakedatagenerator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.ContentDB;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.ContentDBFactory;
import ai.promoted.proto.event.LogRequest;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class LogRequestFactorTest {

  @DisplayName("createItemShoppingCartBuilder() - check for consistency")
  @Test
  void createItemShoppingCartBuilder_checkForConsistency() throws Exception {
    LogRequestIteratorOptions options = LogRequestFactory.createItemShoppingCartBuilder().build();
    ContentDB contentDb =
        ContentDBFactory.create(LogRequestIterator.PLATFORM_ID, options.contentDBFactoryOptions());
    List<LogRequest> logRequests = ImmutableList.copyOf(new LogRequestIterator(options, contentDb));

    assertEquals(37, logRequests.size());
    assertEquals(16, LogRequestFactory.pushDownToImpressions(logRequests).size());
    assertEquals(28, LogRequestFactory.pushDownToActions(logRequests).size());

    try {
      assertCartMatched(logRequests, getGoldenFilePath("createItemShoppingCartBuilder"));
    } catch (Throwable cause) {
      String tmpFile = getTmpFilePath("createItemShoppingCartBuilder");
      writeLogRequestsToTempFile(logRequests, tmpFile);
      throw new UnexpectedLogRequestsChangeException(tmpFile, cause);
    }
  }

  @DisplayName("createStoreInsertionItemShoppingCartBuilder() - check for consistency")
  @Test
  void createStoreInsertionItemShoppingCartBuilder_checkForConsistency() throws Exception {
    LogRequestIteratorOptions options =
        LogRequestFactory.createStoreInsertionItemShoppingCartBuilder().build();
    ContentDB contentDb =
        ContentDBFactory.create(LogRequestIterator.PLATFORM_ID, options.contentDBFactoryOptions());
    List<LogRequest> logRequests = ImmutableList.copyOf(new LogRequestIterator(options, contentDb));

    assertEquals(37, logRequests.size());
    assertEquals(4, LogRequestFactory.pushDownToImpressions(logRequests).size());
    assertEquals(16, LogRequestFactory.pushDownToActions(logRequests).size());

    try {
      assertCartMatched(
          logRequests, getGoldenFilePath("createStoreInsertionItemShoppingCartBuilder"));
    } catch (Throwable cause) {
      String tmpFile = getTmpFilePath("createStoreInsertionItemShoppingCartBuilder");
      writeLogRequestsToTempFile(logRequests, tmpFile);
      throw new UnexpectedLogRequestsChangeException(tmpFile, cause);
    }
  }

  @DisplayName("createStoreToItemBuilder() - check for consistency")
  @Test
  void createStoreToItemBuilder_checkForConsistency() throws Exception {
    LogRequestIteratorOptions options = LogRequestFactory.createStoreToItemBuilder().build();
    ContentDB contentDb =
        ContentDBFactory.create(LogRequestIterator.PLATFORM_ID, options.contentDBFactoryOptions());
    List<LogRequest> logRequests = ImmutableList.copyOf(new LogRequestIterator(options, contentDb));

    assertEquals(16, logRequests.size());
    assertEquals(1, LogRequestFactory.pushDownToImpressions(logRequests).size());
    assertEquals(4, LogRequestFactory.pushDownToActions(logRequests).size());

    try {
      assertCartMatched(logRequests, getGoldenFilePath("createStoreToItemBuilder"));
    } catch (Throwable cause) {
      String tmpFile = getTmpFilePath("createStoreToItemBuilder");
      writeLogRequestsToTempFile(logRequests, tmpFile);
      throw new UnexpectedLogRequestsChangeException(tmpFile, cause);
    }
  }

  private void writeLogRequestsToTempFile(List<LogRequest> logRequests, String tmpFile)
      throws IOException {
    FileWriter myWriter = new FileWriter(tmpFile);
    myWriter.write(Joiner.on("\n").join(logRequests));
    myWriter.close();
  }

  private void assertCartMatched(List<LogRequest> logRequests, String filePath) throws Exception {
    StringBuilder contentBuilder = new StringBuilder();
    try (Stream<String> stream = Files.lines(Paths.get(filePath), StandardCharsets.UTF_8)) {
      // Read the content with Stream
      stream.forEach(s -> contentBuilder.append(s).append("\n"));
    }

    assertEquals(contentBuilder.toString(), Joiner.on("\n").join(logRequests));
  }

  private String getTmpFilePath(String builderName) {
    return "/tmp/" + builderName + ".txt";
  }

  private String getGoldenFilePath(String builderName) {
    return "pipeline/src/test/java/ai/promoted/metrics/logprocessor/common/fakedatagenerator/"
        + builderName
        + ".txt";
  }

  public static final class UnexpectedLogRequestsChangeException extends RuntimeException {
    public UnexpectedLogRequestsChangeException(String tmpFile, Throwable cause) {
      super(
          "Test case LogRequests changed.  New output found in "
              + tmpFile
              + " .  If this is intended, copy the tmp file over to the test directory."
              + "If it was not intended, run through a diff program like tkdiff.",
          cause);
    }
  }
}
