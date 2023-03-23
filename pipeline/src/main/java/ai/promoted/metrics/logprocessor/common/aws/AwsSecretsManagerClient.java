package ai.promoted.metrics.logprocessor.common.aws;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;

/**
 * Client to get secret values from AWS Secrets Manager. No unit tests since this is mostly a
 * connector to AWS and it's pretty simple.
 */
public class AwsSecretsManagerClient {

  /** Returns a String (likely JSON). */
  public static String getSecretsValue(String regionString, String secretName) {
    Region region = Region.of(regionString);
    SecretsManagerClient secretsClient = SecretsManagerClient.builder().region(region).build();
    try {
      GetSecretValueRequest valueRequest =
          GetSecretValueRequest.builder().secretId(secretName).build();
      return secretsClient.getSecretValue(valueRequest).secretString();
    } catch (SecretsManagerException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
