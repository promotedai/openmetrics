package ai.promoted.metrics.logprocessor.job.validateenrich.anonymizer;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TimedStringAnonymizerUnitTest {

  private static final String KEY1 = "salt1";
  private static final String KEY2 = "salt2";
  TimedStringAnonymizer anonymizer;

  @BeforeEach
  public void setUp() {
    anonymizer =
        new TimedStringAnonymizer(
            ImmutableList.of(
                // Timestamp in milliseconds: 1640995200000
                // Date and time (GMT): Saturday, January 1, 2022 12:00:00 AM
                new StartTimeAndStringAnonymizer(new ShaStringAnonymizer(KEY1, 32), 1640995200000L),
                // Date and time (GMT): Sunday, January 1, 2023 12:00:00 AM
                new StartTimeAndStringAnonymizer(
                    new ShaStringAnonymizer(KEY2, 32), 1672531200000L)));
  }

  @Test
  public void anonymize() {
    assertThat(anonymizer.apply("testUserId", 0L)).isEqualTo("86825f914af824a99285342f71466542");
    assertThat(anonymizer.apply("testUserId", 1666842167000L))
        .isEqualTo("86825f914af824a99285342f71466542");
    assertThat(anonymizer.apply("testUserId", 1698378167000L))
        .isEqualTo("ea3f2a25ee257e272101027041e2da62");
  }
}
