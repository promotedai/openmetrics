package ai.promoted.metrics.logprocessor.common.queryablestate;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ClientUtils {

  private static final Logger LOG = LogManager.getLogger(ClientUtils.class);

  public static <T> T doWithRetry(
      Callable<T> callable,
      long waitingInterval,
      int maxRetryTimes,
      Runnable attemptFailedHandler,
      Callable<T> fallback) {

    for (int i = 0; i < maxRetryTimes; ++i) {
      try {
        return callable.call();
      } catch (Exception e) {
        if (i == maxRetryTimes - 1) { // the last retry fails
          LOG.error("Failed to execute after retrying " + maxRetryTimes + " times.", e);
        } else {
          LOG.warn(
              "Failed to execute due to {}. Will retry. {} times left.",
              e.getMessage(),
              maxRetryTimes - i - 1);
        }
        try {
          TimeUnit.MILLISECONDS.sleep(waitingInterval);
        } catch (InterruptedException ex) {
          throw new RuntimeException(ex);
        }
        attemptFailedHandler.run();
      }
    }

    try {
      return fallback.call();
    } catch (Exception e) {
      LOG.error("Failed to execute the fallback logic", e);
      throw new RuntimeException(e);
    }
  }
}
