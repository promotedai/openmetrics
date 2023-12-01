package ai.promoted.metrics.logprocessor.job.validateenrich;

import ai.promoted.metrics.logprocessor.job.validateenrich.anonymizer.TimedStringAnonymizer;
import ai.promoted.proto.common.RetainedUserState;
import java.io.Serializable;

public class RetainedUserIdAnonymizer implements Serializable {
  private final TimedStringAnonymizer valueAnonymizer;

  public RetainedUserIdAnonymizer(TimedStringAnonymizer valueAnonymizer) {
    this.valueAnonymizer = valueAnonymizer;
  }

  public String apply(RetainedUserState state, long eventTime) {
    // Create a string of the parameters and then pass that into our anonymizer.
    String value =
        state.getPlatformId()
            + "\t"
            + state.getUserId()
            + "\t"
            + state.getLastForgottenTimeMillis();
    return valueAnonymizer.apply(value, eventTime);
  }
}
