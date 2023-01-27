package ai.promoted.metrics.logprocessor.common.constant;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConstantsTest {

    @Test
    public void getS3BucketUri() {
        assertEquals("s3a://mybucket/", Constants.getS3BucketUri("mybucket"));
    }
}
