package ai.promoted.metrics.logprocessor.common.counter;

import com.google.common.annotations.VisibleForTesting;

/** Functions to help deal with device taxonomy. */
@VisibleForTesting
public interface Device {
  static String getDevice(String os, String userAgent) {
    // The only instances of "Mobile" in the user agent taxonomy are for mobile browsers.
    if (userAgent.contains("Mobile")) {
      return "mweb";
    }
    // Anything on mobile other than mobile web is considered native. There is no mobile "other".
    if (os.equals("iOS")) {
      return "nios";
    }
    if (os.equals("Android")) {
      return "nandroid";
    }
    if (userAgent.equals("Chrome")
        || userAgent.equals("Safari")
        || userAgent.equals("Edge")
        || userAgent.equals("Firefox")
        || userAgent.equals("Opera")) {
      return "web";
    }
    return "other";
  }
}
