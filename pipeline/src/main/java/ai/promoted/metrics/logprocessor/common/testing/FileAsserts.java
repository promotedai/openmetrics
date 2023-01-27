package ai.promoted.metrics.logprocessor.common.testing;

import com.google.common.collect.ImmutableList;

import java.io.File;
import java.util.Arrays;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static org.junit.jupiter.api.Assertions.assertTrue;

public interface FileAsserts {

  /**
   * Returns the Flink part files in {@code dir}.
   */
  static File[] getPartFiles(File dir) {
    return getFiles(dir, Pattern.compile("part-.*"));
  }

  /**
   * Returns the files in {@code dir} that match {@code expectedName}.
   */
  static File[] getFiles(File dir, Pattern expectedName) {
    return dir.listFiles((tdir, name) -> expectedName.matcher(name).matches());
  }

  /**
   * Returns the matching {@code File[]}
   */
  static File[] assertPartFilesExist(File dir) {
    return assertHasAnyFile(dir, Pattern.compile("part-.*"));
  }

  /**
   * Returns the matching {@code File[]}
   */
  static File[] assertHasAnyFile(File dir, Pattern expectedName) {
    File[] matchingFiles = getFiles(dir, expectedName);
    if (matchingFiles == null) {
      File find = dir;
      while (!find.exists()) find = find.getParentFile();
      checkState(false,
              "Directory not found: %s\nExisting path: %s\nContents:%s",
              dir.getAbsolutePath(), find.getAbsolutePath(), Arrays.toString(find.list()));
    }
    assertTrue(matchingFiles.length > 0,
            () -> "Expected to find at least one file, dir=" + dir.getPath() +
                    ", expectedName=" + expectedName.pattern() +
                    ", other files in directory=" + ImmutableList.copyOf(dir.list()));
    return matchingFiles;
  }
}
