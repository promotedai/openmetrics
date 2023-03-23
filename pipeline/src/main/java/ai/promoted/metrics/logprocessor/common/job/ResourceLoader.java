package ai.promoted.metrics.logprocessor.common.job;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** A utility for static loading static resources. */
public interface ResourceLoader {

  static String getQueryFromResource(Class jobClass, String queryFile) {
    return getTextResource(jobClass, "resources/" + queryFile).collect(Collectors.joining("\n"));
  }

  /** Gets resource from the jar. */
  static Stream<String> getTextResource(Class jobClass, String fileName) {
    InputStream inputStream = jobClass.getResourceAsStream(fileName);
    if (inputStream == null) {
      throw new IllegalArgumentException("Resource filename does not exist, file=" + fileName);
    }
    return new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8)).lines();
  }
}
