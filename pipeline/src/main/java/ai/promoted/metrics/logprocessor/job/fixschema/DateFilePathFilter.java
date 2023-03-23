package ai.promoted.metrics.logprocessor.job.fixschema;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.core.fs.Path;

public class DateFilePathFilter extends FilePathFilter {
  private static final long serialVersionUID = 1L;

  // Inclusive.
  private final String startDt;
  private final String endDt;
  private transient Pattern pattern;

  DateFilePathFilter(String startDt, String endDt) {
    this.startDt = startDt;
    this.endDt = endDt;
  }

  @Override
  public boolean filterPath(Path filePath) {
    if (FilePathFilter.createDefaultFilter().filterPath(filePath)) {
      return true;
    }
    Matcher m = getDtPattern().matcher(filePath.getPath());
    // If dt is not in the path, do not filter out.  This might be a root path.
    if (!m.find()) {
      return false;
    }
    // Gets the subgroup.  Group 0 gets all matching characters.
    String dt = m.group(1);
    return startDt.compareTo(dt) > 0 || endDt.compareTo(dt) < 0;
  }

  private Pattern getDtPattern() {
    if (pattern == null) {
      String patternString = "/dt=(\\d{4}-\\d{2}-\\d{2})";
      pattern = Pattern.compile(patternString);
    }
    return pattern;
  }
}
