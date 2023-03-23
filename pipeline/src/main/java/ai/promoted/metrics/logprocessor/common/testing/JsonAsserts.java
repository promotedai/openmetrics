package ai.promoted.metrics.logprocessor.common.testing;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Set;
import org.json.JSONObject;

public interface JsonAsserts {

  static ImmutableSet<String> getLines(File[] files) throws IOException {
    ImmutableSet.Builder<String> builder = ImmutableSet.builder();
    for (File file : files) {
      builder.addAll(
          Files.readAllLines(file.toPath()).stream()
              .map((json) -> new JSONObject(json).toString())
              .iterator());
    }
    return builder.build();
  }

  static void assertLinesFiles(Set<String> expectedLines, File[] files) throws IOException {
    Set<String> actualLines = getLines(files);
    Sets.SetView<String> expectedDiffs = Sets.difference(expectedLines, actualLines);
    Sets.SetView<String> actualDiffs = Sets.difference(actualLines, expectedLines);

    assertAll(
        "record",
        Streams.concat(
            expectedDiffs.stream()
                .map(
                    (record) ->
                        () -> {
                          fail("Did not find expected record=" + record);
                        }),
            actualDiffs.stream()
                .map(
                    (record) ->
                        () -> {
                          fail("Found unexpected record=" + record);
                        })));
  }
}
