package ai.promoted.metrics.logprocessor.common.testing;

import com.google.common.base.Joiner;
import com.google.common.collect.Streams;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.util.SerializedThrowable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Utilities for Minicluster. */
public interface MiniclusterUtils {
  // Don't overly check for the job status to reduce load.  3ms was arbitrarily picked to trade off
  // latency and load.
  long POLL_SLEEP_MS = 3;
  Logger LOGGER = LogManager.getLogger(MiniclusterUtils.class);

  static void waitForDone(ClusterClient<?> clusterClient, JobID jobId, Duration timeout)
      throws InterruptedException, ExecutionException {
    Instant start = Instant.now();

    // Accumulate job state for debugging.  Only record when the status changes.  Keep these two
    // lists in sync.
    List<JobStatus> statusHistory = new ArrayList<>();
    List<Long> timeHistory = new ArrayList<>();

    JobStatus jobStatus = null;
    while (jobStatus == null || !jobStatus.isTerminalState()) {
      long startMs = Instant.now().toEpochMilli() - start.toEpochMilli();

      try {
        long timeoutMs = timeout.toMillis() - startMs;
        jobStatus = clusterClient.getJobStatus(jobId).get(timeoutMs, TimeUnit.MILLISECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        throw new RuntimeException(
            "Job timed out, timeToStatus="
                + fromJobStatusHistoryToString(statusHistory, timeHistory),
            e);
      }

      long endMs = Instant.now().toEpochMilli() - start.toEpochMilli();
      if (statusHistory.isEmpty() || jobStatus != statusHistory.get(statusHistory.size() - 1)) {
        timeHistory.add(endMs);
        statusHistory.add(jobStatus);
      }

      if (shouldWait(jobStatus)) {
        long sleepMs = Math.max(0L, POLL_SLEEP_MS - (endMs - startMs));
        if (sleepMs > 0) {
          Thread.sleep(sleepMs);
        }
      }
    }

    if (jobStatus != JobStatus.FINISHED) {
      SerializedThrowable throwable =
          clusterClient.requestJobResult(jobId).get().getSerializedThrowable().get();
      String jobException = getJobException(jobId);
      throw new RuntimeException(
          "expected jobStatus=FINISHED, actual="
              + jobStatus
              + ", jobException="
              + jobException
              + ", timeToStatus="
              + fromJobStatusHistoryToString(statusHistory, timeHistory)
              + ", throwable="
              + toString(throwable));
    }
  }

  static boolean shouldWait(JobStatus jobStatus) {
    // Uncomment to sleep on failure.  Also increase POOL_SLEEP_MS.
    // if (jobStatus == JobStatus.FAILED) {
    //     LOGGER.warn("Encountered JobStatus.FAILED.  Sleeping...");
    //     LOGGER.info("Minicluster Flink REST API=" +
    // MiniClusterExtension.flinkCluster.getClusterClient().getWebInterfaceURL());
    //     return true;
    // }
    return !jobStatus.isTerminalState();
  }

  static String fromJobStatusHistoryToString(List<JobStatus> statuses, List<Long> times) {
    return Streams.zip(statuses.stream(), times.stream(), (status, time) -> status + "@" + time)
        .collect(Collectors.joining(","));
  }

  /** Do toString ourselves to get full stack trace. */
  static String toString(Throwable throwable) {
    return throwable
        + ",\nstackTrace="
        + Joiner.on("\n").join(throwable.getStackTrace())
        + ",\ncause="
        + (throwable.getCause() == null ? null : toString(throwable.getCause()));
  }

  /** Returns the Job's exception using the Flink REST API. */
  static String getJobException(JobID jobId) {
    String baseUrl = MiniClusterExtension.flinkCluster.getClusterClient().getWebInterfaceURL();
    String url = baseUrl + "/jobs/" + jobId.toString() + "/exceptions?maxExceptions=10";
    try {
      return getResponseBody(url);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static String getResponseBody(String urlString) throws IOException {
    StringBuilder result = new StringBuilder();
    URL url = new URL(urlString);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
      for (String line; (line = reader.readLine()) != null; ) {
        result.append(line);
      }
    }
    return result.toString();
  }
}
