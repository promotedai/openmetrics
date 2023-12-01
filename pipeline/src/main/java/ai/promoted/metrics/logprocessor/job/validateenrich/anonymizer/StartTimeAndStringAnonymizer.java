package ai.promoted.metrics.logprocessor.job.validateenrich.anonymizer;

import java.io.Serializable;

public class StartTimeAndStringAnonymizer implements Serializable {
  private final StringAnonymizer anonymizer;
  private final long startTimeMillis;

  public StartTimeAndStringAnonymizer(StringAnonymizer anonymizer, long startTimeMillis) {
    this.anonymizer = anonymizer;
    this.startTimeMillis = startTimeMillis;
  }

  public StringAnonymizer getAnonymizer() {
    return anonymizer;
  }

  public long getStartTimeMillis() {
    return startTimeMillis;
  }
}
