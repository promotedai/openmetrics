package ai.promoted.metrics.logprocessor.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class LogUtilTest {

  @ParameterizedTest(name = "LogUtil.truncate({0}) = {1}")
  @CsvSource({
    " apple , apple ",
    "apple broccoli,apple broc (TRUNCATED)",
  })
  public void truncate(String input, String expected) throws Exception {
    // Short maxLength for the test.
    assertEquals(expected, LogUtil.truncate(input, 10));
  }
}
