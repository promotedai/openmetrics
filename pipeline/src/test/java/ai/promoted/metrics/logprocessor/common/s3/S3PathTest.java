package ai.promoted.metrics.logprocessor.common.s3;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class S3PathTest {

    @Test
    public void toString_root() {
        assertEquals("s3a://bucket/alpha/beta/",
                S3Path.builder()
                        .setRoot("s3a://bucket")
                        .addSubDirs("alpha", "beta")
                        .build()
                        .toString());
    }

    @Test
    public void toString_addSubDir() {
        assertEquals("s3a://bucket/",
                S3Path.builder()
                        .setRoot("s3a://bucket")
                        .build()
                        .toString());
        assertEquals("s3a://bucket/alpha/",
                S3Path.builder()
                        .setRoot("s3a://bucket")
                        .addSubDir("alpha")
                        .build()
                        .toString());
        assertEquals("s3a://bucket/alpha/beta/",
                S3Path.builder()
                        .setRoot("s3a://bucket")
                        .addSubDir("alpha")
                        .addSubDir("beta")
                        .build()
                        .toString());
    }

    @Test
    public void toString_setLabel_addSubDir() {
        assertEquals("s3a://bucket/alpha/",
                S3Path.builder()
                        .setRoot("s3a://bucket")
                        .setJoinLabel("alpha")
                        .build()
                        .toString());
        assertEquals("s3a://bucket/alpha/beta/",
                S3Path.builder()
                        .setRoot("s3a://bucket")
                        .setJoinLabel("alpha")
                        .addSubDir("beta")
                        .build()
                        .toString());
    }

    @Test
    public void toString_mmultipleCalls() {
        S3Path.Builder builder = S3Path.builder()
                .setRoot("s3a://bucket")
                .addSubDir("alpha")
                .addSubDir("beta");
        assertEquals("s3a://bucket/alpha/beta/", builder.build().toString());
        assertEquals("s3a://bucket/alpha/beta/", builder.build().toString());
    }
}
