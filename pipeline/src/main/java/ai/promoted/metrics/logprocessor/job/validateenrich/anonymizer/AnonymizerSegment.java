package ai.promoted.metrics.logprocessor.job.validateenrich.anonymizer;

import ai.promoted.metrics.logprocessor.common.aws.AwsSecretsManagerClient;
import ai.promoted.metrics.logprocessor.common.job.FlinkSegment;
import ai.promoted.metrics.logprocessor.common.job.RegionSegment;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.GeneratedMessageV3;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import picocli.CommandLine.Option;

/** A {@code FlinkSegment} that provides an {@UserIdAnonymizer}. */
public class AnonymizerSegment implements FlinkSegment {
  private static final int DEFAULT_LENGTH = 32;
  private final RegionSegment regionSegment;

  @Option(
      names = {"--awsSecretShaAnonymizer"},
      description =
          "A Map of keys to (maps of Secret salt to start times).  See TimedStringMapConverter for value format.")
  public Map<String, String> awsSecretShaAnonymizer = new HashMap<>();

  @Option(
      names = {"--staticKeyAnonymizer"},
      description =
          "A Map of keys to (maps of static keys to start times).  This can be used for tests and local runs. See TimeValueMapConverter.")
  public Map<String, String> staticKeyAnonymizer = new HashMap<>();

  @Option(
      names = {"--anonymizerLength"},
      description = "The anonymizer output length.  Defaults to 32.")
  public Map<String, Integer> anonymizerLength = new HashMap<>();

  /* For mocking. */
  @VisibleForTesting Function<String, String> awsSecretNameToShaSalt;

  // Access through `getNameToAnonymizer()`.
  private Map<String, TimedStringAnonymizer> nameToAnonymizer;

  public AnonymizerSegment(RegionSegment regionSegment) {
    this.regionSegment = regionSegment;
    this.awsSecretNameToShaSalt =
        (secretName) -> AwsSecretsManagerClient.getSecretsValue(regionSegment.region, secretName);
  }

  private Map<String, TimedStringAnonymizer> getNameToAnonymizer() {
    if (nameToAnonymizer == null) {
      nameToAnonymizer = new HashMap<>();
      addAnonymizers(
          awsSecretShaAnonymizer,
          (secretName, length) ->
              new ShaStringAnonymizer(awsSecretNameToShaSalt.apply(secretName), length));
      addAnonymizers(
          staticKeyAnonymizer, (secretName, length) -> new ShaStringAnonymizer(secretName, length));
    }
    return nameToAnonymizer;
  }

  @Override
  public void validateArgs() {
    for (Map.Entry<String, Integer> entry : anonymizerLength.entrySet()) {
      Preconditions.checkArgument(entry.getValue() > 0, "lengths must be positive");
    }
    // Parse configs.
    getNameToAnonymizer();
    if (!awsSecretShaAnonymizer.isEmpty()) {
      Preconditions.checkArgument(!regionSegment.region.isEmpty(), "--region needs to be set");
    }
  }

  @Override
  public Set<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    return ImmutableSet.of();
  }

  public boolean hasAnonymizer(String name) {
    return getNameToAnonymizer().containsKey(name);
  }

  public TimedStringAnonymizer getAnonymizer(String name) {
    TimedStringAnonymizer anonymizer = getNameToAnonymizer().get(name);
    Preconditions.checkNotNull(anonymizer, "Missing anonymizer %s", name);
    return anonymizer;
  }

  private void addAnonymizers(
      Map<String, String> nameToValueFlag,
      BiFunction<String, Integer, StringAnonymizer> valueAndLengthToAnonymizer) {
    for (Map.Entry<String, String> entry : nameToValueFlag.entrySet()) {
      int length = anonymizerLength.getOrDefault(entry.getKey(), DEFAULT_LENGTH);
      nameToAnonymizer.put(
          entry.getKey(),
          toTimedStringAnonymizer(
              length,
              TimedStringMapConverter.convert(entry.getValue()),
              valueAndLengthToAnonymizer));
    }
  }

  private TimedStringAnonymizer toTimedStringAnonymizer(
      int length,
      List<TimedString> timeValues,
      BiFunction<String, Integer, StringAnonymizer> valueAndLengthToAnonymizer) {
    return new TimedStringAnonymizer(
        timeValues.stream()
            .map(
                tv ->
                    new StartTimeAndStringAnonymizer(
                        valueAndLengthToAnonymizer.apply(tv.value(), length), tv.startMillis()))
            .collect(Collectors.toList()));
  }
}
