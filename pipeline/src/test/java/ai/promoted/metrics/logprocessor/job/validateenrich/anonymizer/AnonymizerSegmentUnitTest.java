package ai.promoted.metrics.logprocessor.job.validateenrich.anonymizer;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.promoted.metrics.logprocessor.common.job.RegionSegment;
import java.util.HashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AnonymizerSegmentUnitTest {

  RegionSegment regionSegment;
  AnonymizerSegment segment;

  @BeforeEach
  public void setUp() {
    regionSegment = new RegionSegment();
    segment = new AnonymizerSegment(regionSegment);
    segment.awsSecretNameToShaSalt =
        (name) -> {
          switch (name) {
            case "secret-name":
              return "salt1";
            case "secret-name2":
              return "salt2";
            default:
              throw new UnsupportedOperationException("name=" + name);
          }
        };
  }

  @Test
  public void validateRegion() {
    segment.validateArgs();
    regionSegment.region = "us-east-1";
    segment.validateArgs();
    segment.awsSecretShaAnonymizer = new HashMap<>();
    segment.awsSecretShaAnonymizer.put("key", "value");
    segment.validateArgs();
    regionSegment.region = "";
    assertThrows(IllegalArgumentException.class, () -> segment.validateArgs());
  }

  @Test
  public void validateLength() {
    segment.validateArgs();
    segment.anonymizerLength = new HashMap<>();
    segment.anonymizerLength.put("name", 16);
    segment.validateArgs();
    segment.anonymizerLength.put("name", 0);
    assertThrows(IllegalArgumentException.class, () -> segment.validateArgs());
  }

  @Test
  public void anonymizeSecretSalt() {
    segment.awsSecretShaAnonymizer = new HashMap<>();
    segment.awsSecretShaAnonymizer.put("name", "secret-name=2022-01-01,secret-name2=2023-01-01");
    assertThat(segment.getAnonymizer("name").apply("testUserId", 0L))
        .isEqualTo("86825f914af824a99285342f71466542");
    assertThat(segment.getAnonymizer("name").apply("testUserId", 1666842167000L))
        .isEqualTo("86825f914af824a99285342f71466542");
    assertThat(segment.getAnonymizer("name").apply("testUserId", 1698378167000L))
        .isEqualTo("ea3f2a25ee257e272101027041e2da62");
  }

  // TODO - add static.
}
