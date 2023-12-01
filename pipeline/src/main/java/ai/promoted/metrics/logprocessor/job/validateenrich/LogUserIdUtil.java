package ai.promoted.metrics.logprocessor.job.validateenrich;

import java.util.Locale;

public interface LogUserIdUtil {
  // A common method so we can track where we convert.
  static String toLogUserId(String anonUserId) {
    return anonUserId.toLowerCase(Locale.ROOT);
  }
}
