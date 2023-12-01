package ai.promoted.metrics.logprocessor.common.util;

import static com.google.common.truth.Truth.assertThat;

import org.junit.jupiter.api.Test;

public class PagingIdUtilTest {

  @Test
  public void firstNotEmpty() {
    assertThat(PagingIdUtil.isPagingIdEmpty("")).isTrue();
    assertThat(PagingIdUtil.isPagingIdEmpty("_")).isTrue();
    assertThat(PagingIdUtil.isPagingIdEmpty("abc")).isFalse();
  }
}
