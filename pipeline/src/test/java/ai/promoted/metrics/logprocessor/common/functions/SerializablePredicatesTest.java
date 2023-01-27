package ai.promoted.metrics.logprocessor.common.functions;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class SerializablePredicatesTest {

    @Test
    public void not() throws Exception {
        assertTrue(SerializablePredicates.not(returnFalse).test("notUsed"));
        assertFalse(SerializablePredicates.not(returnTrue).test("notUsed"));
    }

    private static SerializablePredicate<String> returnTrue = (s) -> true;
    private static SerializablePredicate<String> returnFalse = (s) -> false;
}
