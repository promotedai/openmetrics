package ai.promoted.metrics.logprocessor.job.validateenrich.anonymizer;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class TimedStringMapConverterUnitTest {

  @Test
  public void invalid() {
    assertThrows(IllegalArgumentException.class, () -> TimedStringMapConverter.convert(""));
    assertThrows(IllegalArgumentException.class, () -> TimedStringMapConverter.convert("=="));
  }

  @Test
  public void validAwsExample1() {
    assertThat(TimedStringMapConverter.convert("prm-dev-2023-10-01"))
        .containsExactly(TimedString.create("prm-dev-2023-10-01", 0L));
  }

  @Test
  public void validAwsLength() {
    assertThat(TimedStringMapConverter.convert("prm-dev-2023-10-01"))
        .containsExactly(TimedString.create("prm-dev-2023-10-01", 0L));
  }

  @Test
  public void validAwsExample2() {
    assertThat(
            TimedStringMapConverter.convert(
                "prm-dev-2023-01-01=2023-01-01,prm-dev-2023-10-01=2023-10-01"))
        .containsExactly(
            TimedString.create("prm-dev-2023-01-01", 1672531200000L),
            TimedString.create("prm-dev-2023-10-01", 1696118400000L));
  }
}
