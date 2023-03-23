package ai.promoted.metrics.logprocessor.common.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

public class IterablesTest {

  @Test
  public void exactlyOne() {
    assertEquals(1, Iterables.exactlyOne(ImmutableList.of(1)));
  }

  @Test
  public void exactlyOne_throwZero() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          Iterables.exactlyOne(ImmutableList.of());
        });
  }

  @Test
  public void exactlyOne_throwTooMany() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          Iterables.exactlyOne(ImmutableList.of(1, 2));
        });
  }
}
