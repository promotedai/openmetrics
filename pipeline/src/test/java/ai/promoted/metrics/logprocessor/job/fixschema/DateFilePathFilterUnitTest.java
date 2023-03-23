package ai.promoted.metrics.logprocessor.job.fixschema;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.flink.core.fs.Path;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class DateFilePathFilterUnitTest {
  @ParameterizedTest
  @CsvSource({
    "true, 2023-02-01, 2023-02-04, /raw/record/dt=2023-01-30",
    "true, 2023-02-01, 2023-02-04, /raw/record/dt=2023-01-30/hour=04/part",
    "false, 2023-02-01, 2023-02-04, /raw/record/dt=2023-02-01",
    "false, 2023-02-01, 2023-02-04, /raw/record/dt=2023-02-01/hour=04/part",
    "false, 2023-02-01, 2023-02-04, /raw/record/dt=2023-02-04",
    "false, 2023-02-01, 2023-02-04, /raw/record/dt=2023-02-04/hour=04/part",
    "true, 2023-02-01, 2023-02-04, /raw/record/dt=2023-02-05",
    "true, 2023-02-01, 2023-02-04, /raw/record/dt=2023-02-05/hour=04/part",
    "false, 2023-02-01, 2023-02-04, /raw/record/",
    "false, 2023-02-01, 2023-02-04, /raw/record/hour=04/part",
  })
  void itemDeviceCount(boolean filteredOut, String startDt, String endDt, String filePath) {
    assertEquals(
        filteredOut, new DateFilePathFilter(startDt, endDt).filterPath(new Path(filePath)));
  }
}
