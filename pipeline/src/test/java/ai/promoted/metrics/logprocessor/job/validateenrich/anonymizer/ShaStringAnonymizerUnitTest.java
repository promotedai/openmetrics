package ai.promoted.metrics.logprocessor.job.validateenrich.anonymizer;

import static com.google.common.truth.Truth.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ShaStringAnonymizerUnitTest {

  StringAnonymizer anonymizer;

  @BeforeEach
  public void setUp() {
    anonymizer = new ShaStringAnonymizer("testsalt", 32);
  }

  @Test
  public void hash() {
    assertThat(anonymizer.apply("")).isEqualTo("4edf07edc95b2fdcbcaf2378fd12d8ac");
    assertThat(anonymizer.apply("userId1")).isEqualTo("b19624efb80dcd37f76d245b186b565c");
  }

  @Test
  public void hashLimitLength() {
    anonymizer = new ShaStringAnonymizer("testsalt", 12);
    assertThat(anonymizer.apply("")).isEqualTo("4edf07edc95b");
    assertThat(anonymizer.apply("userId1")).isEqualTo("b19624efb80d");
    anonymizer = new ShaStringAnonymizer("testsalt", 0);
    assertThat(anonymizer.apply("")).isEqualTo("");
    anonymizer = new ShaStringAnonymizer("testsalt", 100);
    assertThat(anonymizer.apply(""))
        .isEqualTo("4edf07edc95b2fdcbcaf2378fd12d8ac212c2aa6e326c59c3e629be3039d6432");
  }
}
