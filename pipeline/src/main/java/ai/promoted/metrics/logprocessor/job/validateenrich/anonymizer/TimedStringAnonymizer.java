package ai.promoted.metrics.logprocessor.job.validateenrich.anonymizer;

import ai.promoted.metrics.logprocessor.common.functions.base.SerializableBiFunction;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Anonymizes a string using time-ranged anonymizers. */
public class TimedStringAnonymizer implements SerializableBiFunction<String, Long, String> {

  private final TreeMap<Long, StringAnonymizer> startTimeToAnonymizer;

  public TimedStringAnonymizer(List<StartTimeAndStringAnonymizer> anonymizers) {
    Preconditions.checkArgument(!anonymizers.isEmpty(), "anonymizers cannot be empty");
    this.startTimeToAnonymizer = new TreeMap<>();
    for (StartTimeAndStringAnonymizer sasa : anonymizers) {
      startTimeToAnonymizer.put(sasa.getStartTimeMillis(), sasa.getAnonymizer());
    }
  }

  @Override
  public String apply(String value, Long timeMillis) {
    Map.Entry<Long, StringAnonymizer> entry = startTimeToAnonymizer.floorEntry(timeMillis);
    if (entry == null) {
      entry = startTimeToAnonymizer.firstEntry();
    }
    return entry.getValue().apply(value);
  }
}
