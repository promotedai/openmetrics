package ai.promoted.metrics.logprocessor.common.constant;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class ConstantsTest {

  @Test
  public void getS3BucketUri() {
    assertEquals("s3a://mybucket/", Constants.getS3BucketUri("mybucket"));
  }
}
