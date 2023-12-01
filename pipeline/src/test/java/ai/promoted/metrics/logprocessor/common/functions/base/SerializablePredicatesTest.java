package ai.promoted.metrics.logprocessor.common.functions.base;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class SerializablePredicatesTest {

  @Test
  public void not() throws Exception {
    assertTrue(SerializablePredicates.not(returnFalse).test("notUsed"));
    assertFalse(SerializablePredicates.not(returnTrue).test("notUsed"));
  }

  private static final SerializablePredicate<String> returnTrue = (s) -> true;
  private static final SerializablePredicate<String> returnFalse = (s) -> false;
}
