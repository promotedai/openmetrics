package ai.promoted.metrics.logprocessor.job.join.common;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.junit.jupiter.api.Test;

public class IntervalSqlUtilTest {
  @Test
  public void map() throws Exception {
    assertThat(IntervalSqlUtil.fromDurationToSQLInterval(Duration.ofSeconds(1L)))
        .isEqualTo("(INTERVAL '1' SECOND)");
    assertThat(IntervalSqlUtil.fromDurationToSQLInterval(Duration.ofSeconds(12L)))
        .isEqualTo("(INTERVAL '12' SECOND)");
    assertThat(IntervalSqlUtil.fromDurationToSQLInterval(Duration.ofMinutes(2L)))
        .isEqualTo("(INTERVAL '2' MINUTE)");
    assertThat(IntervalSqlUtil.fromDurationToSQLInterval(Duration.ofHours(3L)))
        .isEqualTo("(INTERVAL '3' HOUR)");
    assertThat(IntervalSqlUtil.fromDurationToSQLInterval(Duration.ofDays(4L)))
        .isEqualTo("(INTERVAL '4' DAY(1))");
    assertThat(IntervalSqlUtil.fromDurationToSQLInterval(Duration.ofDays(13L)))
        .isEqualTo("(INTERVAL '13' DAY(2))");
    assertThat(IntervalSqlUtil.fromDurationToSQLInterval(Duration.ofDays(246L)))
        .isEqualTo("(INTERVAL '246' DAY(3))");
    assertThat(
            IntervalSqlUtil.fromDurationToSQLInterval(
                Duration.ofDays(4L).plus(Duration.ofHours(3L))))
        .isEqualTo("(INTERVAL '4' DAY(1) + INTERVAL '3' HOUR)");
    assertThat(
            IntervalSqlUtil.fromDurationToSQLInterval(
                Duration.ofDays(4L)
                    .plus(
                        Duration.ofHours(3L)
                            .plus(Duration.ofMinutes(2L))
                            .plus(Duration.ofSeconds(1L)))))
        .isEqualTo(
            "(INTERVAL '4' DAY(1) + INTERVAL '3' HOUR + INTERVAL '2' MINUTE + INTERVAL '1' SECOND)");
  }

  @Test
  public void invalid() throws Exception {
    assertThrows(
        IllegalArgumentException.class,
        () -> IntervalSqlUtil.fromDurationToSQLInterval(Duration.ofMillis(1L)));
    assertThrows(
        IllegalArgumentException.class,
        () -> IntervalSqlUtil.fromDurationToSQLInterval(Duration.ofDays(1000L)));
  }
}
