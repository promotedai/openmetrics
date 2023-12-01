package ai.promoted.metrics.logprocessor.job.counter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.promoted.metrics.logprocessor.common.counter.Device;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class DeviceTest {
  @ParameterizedTest
  @CsvSource({
    "web, Windows, Chrome",
    "web, Windows, Edge",
    "web, Mac OS X, Chrome",
    "web, Windows, Firefox",
    "mweb, Android, Chrome Mobile",
    "web, Chrome OS, Chrome",
    "mweb, iOS, Mobile Safari",
    "web, Mac OS X, Safari",
    "mweb, iOS, Chrome Mobile iOS",
    "nios, iOS, Google",
    "nios, iOS, Facebook",
    "mweb, iOS, DuckDuckGo Mobile",
    "nandroid, Android, Samsung Internet",
    "other, Mac OS X, Apple Mail",
  })
  void itemDeviceCount(String expected, String os, String userAgent) {
    assertEquals(expected, Device.getDevice(os, userAgent));
  }
}
