package ai.promoted.metrics.logprocessor.job.validateenrich.anonymizer;

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import java.util.List;

/** Represents a config for how to anonymize a field. */
@AutoValue
public abstract class AnonymizerConfig {
  public enum Implementation {
    aws,
    constant, // for testing
  }

  public abstract Implementation implementation();

  public abstract int length();

  public abstract List<TimedString> timeValues();

  public static AnonymizerConfig.Builder builder() {
    return new AutoValue_AnonymizerConfig.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setImplementation(Implementation implementation);

    public abstract Builder setLength(int length);

    public abstract Builder setTimeValues(List<TimedString> timeValues);

    abstract AnonymizerConfig autoBuild();

    public final AnonymizerConfig build() {
      AnonymizerConfig config = autoBuild();
      Preconditions.checkState(config.length() > 0, "Needs positive length");
      Preconditions.checkState(config.length() <= 64, "Needs length <= 64");
      return config;
    }
  }
}
