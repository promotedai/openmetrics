package ai.promoted.metrics.logprocessor.common.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests (non-minicluster tests). */
public class ContentApiSegmentUnitTest {

  RegionSegment regionSegment;
  ContentApiSegment segment;

  @BeforeEach
  public void setUp() {
    regionSegment = new RegionSegment();
    segment = new ContentApiSegment(null, regionSegment);
  }

  @Test
  public void validate() {
    segment.validateArgs();
  }

  @Test
  public void validate_enableLookup() {
    segment.contentApiRootUrl = "http://localhost:5150";
    assertThrows(IllegalArgumentException.class, () -> segment.validateArgs());
  }

  @Test
  public void validate_needsFieldKeys() {
    segment.contentApiRootUrl = "http://localhost:5150";
    segment.enableActionContentLookup = true;
    assertThrows(IllegalArgumentException.class, () -> segment.validateArgs());
  }

  @Test
  public void validate_needsApiKey() {
    segment.contentApiRootUrl = "http://localhost:5150";
    segment.enableActionContentLookup = true;
    segment.contentIdFieldKeys = ImmutableList.of("storeId");
    assertThrows(IllegalArgumentException.class, () -> segment.validateArgs());
  }

  @Test
  public void validate_allFields_staticApiKey() {
    segment.contentApiRootUrl = "http://localhost:5150";
    segment.enableActionContentLookup = true;
    segment.contentIdFieldKeys = ImmutableList.of("storeId");
    segment.contentApiKey = "abc";
    segment.validateArgs();
  }

  @Test
  public void validate_needsRegion() {
    segment.contentApiRootUrl = "http://localhost:5150";
    segment.enableActionContentLookup = true;
    segment.contentIdFieldKeys = ImmutableList.of("storeId");
    segment.contentApiSecretName = "secret";
    assertThrows(IllegalArgumentException.class, () -> segment.validateArgs());
  }

  // We cannot test validate TODO - we cannot test the AwsSecretsManagerClient from unit tests.
  @Test
  public void getApiKey_awsSecret() {
    segment.contentApiRootUrl = "http://localhost:5150";
    segment.enableActionContentLookup = true;
    segment.contentIdFieldKeys = ImmutableList.of("storeId");
    segment.contentApiSecretName = "secret";
    regionSegment.region = "us-east-1";
    segment.getAwsSecret = () -> "{\"api-key\": \"abc\"}";
    segment.validateArgs();
    assertEquals("abc", segment.getApiKey());
  }
}
