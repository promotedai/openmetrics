package ai.promoted.metrics.logprocessor.job.validateenrich.anonymizer;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Anonymizes a string using a SHA hash with a salt stored in secret store. */
public class ShaStringAnonymizer implements StringAnonymizer {
  // A salt to use when hashing.
  private final String salt;
  private final int outputLength;

  public ShaStringAnonymizer(String salt, int outputLength) {
    this.salt = salt;
    this.outputLength = outputLength;
  }

  @Override
  public String apply(String value) {
    String saltedValue = salt + value;
    String hashedValue = hashSaltedString(saltedValue);
    if (hashedValue.length() > outputLength) {
      hashedValue = hashedValue.substring(0, outputLength);
    }
    return hashedValue;
  }

  private String hashSaltedString(String input) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));

      return bytesToHex(hash);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Could not find SHA-256 algorithm", e);
    }
  }

  private static String bytesToHex(byte[] bytes) {
    StringBuilder hexString = new StringBuilder();
    for (byte b : bytes) {
      String hex = Integer.toHexString(0xff & b);
      if (hex.length() == 1) {
        hexString.append('0');
      }
      hexString.append(hex);
    }
    return hexString.toString();
  }
}
